"""
Render Stage.
Handles final HTML report rendering.
"""

import logging
import os
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, List, TYPE_CHECKING

from render import render_report, df_to_html_table, render_capital_overall_table
from bbm.matrix_builder import is_bbm_row_active, RENAME_MAP

if TYPE_CHECKING:
    from pipeline.context import PipelineContext


def _render_dti_table(bbm_data: Dict[str, Any]) -> str:
    """Render DTI table: prefer expert table (Excel schema, English), else pipeline DTI/LTI."""
    expert_df = bbm_data.get("dti_expert_table")
    if expert_df is not None and not expert_df.empty:
        try:
            from bbm.dti_expert_renderer import render_dti_expert_table_html
            return render_dti_expert_table_html(expert_df)
        except ImportError:
            pass
    return df_to_html_table(bbm_data.get("dti_lti_compare"), table_type="dti_lti")
from reciprocation import render_reciprocation_table1, render_reciprocation_table2
from news import fetch_news, build_news_feed_html
from osii import prepare_osii_by_country, build_osii_table_html, get_osii_countries, build_all_sii_institutions_table_html

logger = logging.getLogger(__name__)


class RenderStage:
    """Processes final report rendering."""
    
    def __init__(
        self, 
        base_dir: Path, 
        reports_dir: Path, 
        news_config: Dict[str, Any],
        use_supabase: bool = False,
        supabase_config: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize render stage.
        
        Args:
            base_dir: Base directory
            reports_dir: Reports directory
            news_config: News configuration
            use_supabase: Whether to use Supabase for data fetching
            supabase_config: Supabase configuration dict (url, anon_key)
        """
        self.base_dir = base_dir
        self.reports_dir = reports_dir
        self.news_config = news_config
        self.use_supabase = use_supabase
        self.supabase_config = supabase_config or {}
        self.supabase_client = None
        
        if self.use_supabase and self.supabase_config.get("url") and self.supabase_config.get("anon_key"):
            try:
                from supabase import create_client
                self.supabase_client = create_client(
                    self.supabase_config["url"],
                    self.supabase_config["anon_key"]
                )
                logger.info("Supabase client initialized for render stage")
            except Exception as exc:
                logger.warning(f"Failed to initialize Supabase client: {exc}")
                self.use_supabase = False
    
    def _fetch_countries_data_from_supabase(self) -> Dict[str, Any]:
        """
        Fetch countries data from Supabase and transform to countries_data format.
        
        Returns:
            Dictionary mapping country names to profile data
        """
        if not self.supabase_client:
            return {}
        
        try:
            logger.info("Fetching countries data from Supabase...")
            
            # Fetch all countries
            countries_resp = self.supabase_client.table("countries").select("*").execute()
            countries = {row["iso2"]: row for row in countries_resp.data}
            
            # Fetch latest snapshots (using Materialized Views)
            ccyb_snapshots_resp = self.supabase_client.table("mv_latest_ccyb_snapshot").select("*").execute()
            syrb_snapshots_resp = self.supabase_client.table("mv_latest_syrb_snapshot").select("*").execute()
            osii_snapshots_resp = self.supabase_client.table("mv_latest_osii_snapshot").select("*").execute()
            
            # Fetch actual O-SII banks to calculate min/max rates
            osii_banks_resp = self.supabase_client.table("osii_banks").select("*").filter("status", "eq", "Active").execute()
            
            ccyb_snapshots = {row["country_iso2"]: row for row in ccyb_snapshots_resp.data}
            syrb_snapshots = {row["country_iso2"]: row for row in syrb_snapshots_resp.data}
            osii_snapshots = {row["country_iso2"]: row for row in osii_snapshots_resp.data}
            
            # Group O-SII banks by country to calculate min/max
            osii_by_country_iso2 = {}
            for bank in osii_banks_resp.data:
                iso2 = bank.get("country_iso2")
                if not iso2:
                    continue
                rate = float(bank.get("rate", 0)) if bank.get("rate") else 0.0
                if rate > 0:  # Only count banks with positive rates
                    if iso2 not in osii_by_country_iso2:
                        osii_by_country_iso2[iso2] = []
                    osii_by_country_iso2[iso2].append(rate)
            
            # Fetch historical data
            ccyb_decisions_resp = self.supabase_client.table("ccyb_decisions").select("*").order("effective_date").execute()
            syrb_measures_resp = self.supabase_client.table("syrb_measures").select("*").order("effective_date").execute()
            bbm_measures_resp = self.supabase_client.table("bbm_measures").select("*").order("effective_date").execute()

            # Fetch institutional setup (optional; table may not exist before migration)
            inst_by_iso2: Dict[str, Dict[str, Any]] = {}
            try:
                inst_resp = self.supabase_client.table("institutional_setup").select("*").execute()
                for row in inst_resp.data or []:
                    iso = row.get("country_iso2")
                    if iso:
                        r = dict(row)
                        for k in ("key_regulations", "ai_sources_cited"):
                            if k in r and r[k] is None:
                                r[k] = []
                        inst_by_iso2[iso] = r
            except Exception as inst_exc:
                logger.debug("institutional_setup table not available: %s", inst_exc)
            
            # Group by country
            ccyb_by_country = {}
            for row in ccyb_decisions_resp.data:
                iso2 = row["country_iso2"]
                if iso2 not in ccyb_by_country:
                    ccyb_by_country[iso2] = []
                ccyb_by_country[iso2].append(row)
            
            syrb_by_country = {}
            for row in syrb_measures_resp.data:
                iso2 = row["country_iso2"]
                if iso2 not in syrb_by_country:
                    syrb_by_country[iso2] = []
                syrb_by_country[iso2].append(row)
            
            bbm_by_country = {}
            for row in bbm_measures_resp.data:
                iso2 = row["country_iso2"]
                if iso2 not in bbm_by_country:
                    bbm_by_country[iso2] = []
                bbm_by_country[iso2].append(row)
            
            # Transform to countries_data format
            countries_data = {}
            for iso2, country_info in countries.items():
                country_name = country_info.get("country_name", country_info.get("name", ""))
                if not country_name:
                    continue
                
                # Current status
                ccyb_snap = ccyb_snapshots.get(iso2, {})
                syrb_snap = syrb_snapshots.get(iso2, {})
                osii_snap = osii_snapshots.get(iso2, {})
                
                # BBM - same logic as BBM overview (is_bbm_row_active)
                raw_active = [m for m in bbm_by_country.get(iso2, []) if is_bbm_row_active(m)]
                bbm_types = []
                active_bbm_list = []
                for m in raw_active:
                    measure_type = m.get("measure_type", "")
                    measure_short = RENAME_MAP.get(measure_type, measure_type) if measure_type else ""
                    if measure_short and measure_short not in bbm_types:
                        bbm_types.append(measure_short)
                    active_bbm_list.append({
                        "type": measure_short or measure_type,
                        "status": "Active",
                        "date": m.get("effective_date") or m.get("date"),
                        "description": m.get("description", ""),
                    })
                
                current_status = {
                    "ccyb": {
                        "rate": float(ccyb_snap.get("rate", 0)) if ccyb_snap.get("rate") else 0.0,
                        "date": ccyb_snap.get("effective_date", ""),  # Materialized View uses effective_date
                        "status": "Active" if ccyb_snap.get("rate", 0) > 0 else "Inactive",
                    } if ccyb_snap else None,
                    "syrb": {
                        "rate": float(syrb_snap.get("total_rate", 0)) if syrb_snap.get("total_rate") else 0.0,  # Materialized View uses total_rate
                        "date": "",  # Materialized View doesn't have date, use empty or fetch from measures
                        "type": "General" if syrb_snap.get("general_rate", 0) > 0 else ("Sectoral" if syrb_snap.get("sectoral_rate", 0) > 0 else "General"),
                        "status": "Active" if syrb_snap.get("total_rate", 0) > 0 else "Inactive",
                    } if syrb_snap else None,
                    "osii": self._build_osii_status(osii_snap, osii_by_country_iso2.get(iso2, []), iso2=iso2),
                    "bbm": bbm_types,
                    "total_capital": None,  # Would need capital_overall calculation
                }
                
                # Historical evolution
                ccyb_history = []
                for decision in ccyb_by_country.get(iso2, []):
                    ccyb_history.append({
                        "date": decision.get("effective_date", ""),
                        "rate": float(decision.get("rate", 0)) if decision.get("rate") else 0.0,
                        "credit_gap": float(decision.get("credit_gap", 0)) if decision.get("credit_gap") else None,
                    })
                
                syrb_history = []
                for measure in syrb_by_country.get(iso2, []):
                    syrb_history.append({
                        "date": measure.get("effective_date", ""),
                        "rate_numeric": float(measure.get("rate", 0)) if measure.get("rate") else 0.0,
                    })
                
                historical_evolution = {
                    "ccyb": ccyb_history,
                    "syrb": syrb_history,
                }
                
                # Recent changes (last 12 months)
                recent_changes = []
                # TODO: Implement recent changes logic
                
                # Active measures
                active_measures = {
                    "ccyb": current_status.get("ccyb"),
                    "syrb": [m for m in syrb_by_country.get(iso2, []) if m.get("active_status") == "Active" or m.get("status") == "Active"],
                    "bbm": active_bbm_list,
                    "osii": current_status.get("osii"),
                }
                
                inst = inst_by_iso2.get(iso2)
                countries_data[country_name] = {
                    "country": country_name,
                    "iso2": iso2,
                    "current_status": current_status,
                    "institutional_setup": inst,
                    "historical_evolution": historical_evolution,
                    "recent_changes": recent_changes,
                    "active_measures": active_measures,
                    "comparison": {
                        "regional_average": None,
                        "similar_countries": [],
                    },
                    "ai_analysis": "",  # Would need to fetch from analyses
                }
            
            logger.info(f"Fetched {len(countries_data)} countries from Supabase")
            return countries_data
            
        except Exception as exc:
            logger.error(f"Error fetching countries data from Supabase: {exc}", exc_info=True)
            return {}
    
    def _build_osii_status(self, osii_snap: Dict[str, Any], osii_rates: List[float], iso2: str = None) -> Dict[str, Any]:
        """
        Build O-SII status with min/max rates for proper display.
        Rates are normalized to percentage scale (e.g. 1.5 for 1.5%) for display.
        """
        if not osii_snap:
            return None
        
        total_rate = float(osii_snap.get("total_rate", 0)) if osii_snap.get("total_rate") else 0.0
        # Normalize total_rate to percentage scale if stored as decimal
        if 0 < total_rate < 1:
            total_rate = total_rate * 100

        if osii_rates and len(osii_rates) > 0:
            min_rate = min(osii_rates)
            max_rate = max(osii_rates)
            # Normalize to percentage scale (e.g. 0.01 -> 1, 0.02 -> 2)
            if max_rate > 0 and max_rate < 1:
                min_rate = min_rate * 100
                max_rate = max_rate * 100

            if max_rate < 0.01:
                rate_display = "0%"
            elif abs(max_rate - min_rate) < 0.01:
                rate_display = f"{int(max_rate)}%" if max_rate == int(max_rate) else f"{max_rate:.2f}%"
            elif min_rate < 0.01:
                rate_display = f"0-{int(round(max_rate))}%" if max_rate == int(max_rate) else f"0-{max_rate:.1f}%"
            else:
                min_str = f"{min_rate:.1f}" if min_rate != int(min_rate) else str(int(min_rate))
                max_str = f"{max_rate:.1f}" if max_rate != int(max_rate) else str(int(max_rate))
                rate_display = f"{min_str}-{max_str}%"

            return {
                "rate": max_rate,
                "rate_min": min_rate,
                "rate_max": max_rate,
                "rate_display": rate_display,
                "status": "Active" if max_rate > 0 else "Inactive",
            }
        else:
            rate_display = f"{int(total_rate)}%" if total_rate == int(total_rate) else f"{total_rate:.2f}%" if total_rate > 0 else "0%"
            return {
                "rate": total_rate,
                "rate_min": total_rate,
                "rate_max": total_rate,
                "rate_display": rate_display,
                "status": "Active" if total_rate > 0 else "Inactive",
            }
    
    def process(self, ctx: "PipelineContext") -> str:
        """
        Render final HTML report.

        Args:
            ctx: Pipeline context with data, analyses, plots, and all stage outputs.

        Returns:
            Rendered HTML string
        """
        logger.info("4. Riport...")

        data = ctx.data
        analyses = ctx.analyses
        plots_inline = ctx.plots_inline
        plot_figs = ctx.plot_figs
        download_data = ctx.download_data
        ccyb_decisions = ctx.ccyb_decisions
        active_syrb = ctx.active_syrb
        syrb_decisions = ctx.syrb_decisions
        bbm_data = ctx.bbm_data
        capital_overall_df = data.get("capital_overall_df")
        countries_data = ctx.countries_data
        knowledge_graph_json = ctx.knowledge_graph_json

        # News Processing
        api_key = os.getenv(self.news_config.get("api_key_env", "CUSTOM_SEARCH_API_KEY"), "")
        cse_id = os.getenv(self.news_config.get("cse_id_env", "GOOGLE_CSE_ID"), "")
        query = self.news_config.get("query", "")
        months_back = int(self.news_config.get("months_back", 12))
        max_results = int(self.news_config.get("max_results", 10))
        
        try:
            news_df = fetch_news(
                api_key=api_key,
                cse_id=cse_id,
                query=query,
                months_back=months_back,
                max_results=max_results
            )
        except Exception as exc:
            logger.warning(f"News search failed: {exc}")
            news_df = None
        
        if news_df is not None and not news_df.empty:
            try:
                from llm_analysis import LLMAnalyzer
                from config import LLM_CONFIG
                analyzer = LLMAnalyzer(LLM_CONFIG)
                news_texts = (news_df['TITLE'].fillna('') + " - " + news_df['SUMMARY'].fillna('')).tolist()
                news_tags = analyzer.classify_news_tags(news_texts)
                news_df['TAGS'] = news_tags
            except Exception as exc:
                logger.warning(f"News tag classification failed: {exc}")
            try:
                summaries = analyzer.summarize_news_items(
                    (news_df['TITLE'].fillna('') + ". " + news_df['SUMMARY'].fillna('')).tolist()
                )
                news_df['SUMMARY_SHORT'] = summaries
            except Exception as exc:
                logger.warning(f"News summarization failed: {exc}")
                news_df['SUMMARY_SHORT'] = news_df['SUMMARY'].fillna('').astype(str).apply(
                    lambda x: (x[:220] + '...') if len(x) > 220 else x
                )
            try:
                news_df['DATE_PARSED'] = pd.to_datetime(news_df['DATE'], errors='coerce')
                news_df = news_df.sort_values('DATE_PARSED', ascending=False, na_position='last')
            except Exception:
                pass
        else:
            news_df = pd.DataFrame()
        
        today_str = datetime.now().strftime("%Y-%m-%d")
        
        # Reciprocation tables (from ETL data)
        reciprocation_data = data.get("reciprocation_data") or {}
        recip_measures = reciprocation_data.get("measures_df")
        recip_matrix = reciprocation_data.get("matrix_df")
        recip_countries = reciprocation_data.get("country_columns") or []
        reciprocation_table1_html = render_reciprocation_table1(recip_measures) if recip_measures is not None else "<p class='no-data'>No data.</p>"
        reciprocation_table2_html = render_reciprocation_table2(recip_measures, recip_matrix, recip_countries) if (recip_matrix is not None and not recip_matrix.empty) else "<p class='no-data'>No data.</p>"

        tables_html = {
            "ccyb_decisions": df_to_html_table(ccyb_decisions),
            "syrb_active": df_to_html_table(active_syrb),
            "syrb_decisions": df_to_html_table(syrb_decisions),
            "bbm_pivot": bbm_data.get('bbm_pivot_html', "") or "<p class='no-data'>No Data</p>",
            "bbm_decisions": df_to_html_table(bbm_data.get('bbm_decisions')),
            "ltv_table": df_to_html_table(bbm_data.get('ltv_table'), table_type="ltv"),
            "dti_lti_compare": _render_dti_table(bbm_data),
            "capital_overall": render_capital_overall_table(capital_overall_df) if capital_overall_df is not None and not capital_overall_df.empty else "<p class='no-data'>No Data</p>",
            "reciprocation_table1": reciprocation_table1_html,
            "reciprocation_table2": reciprocation_table2_html,
        }
        
        plot_files_input = {
            "ccyb_diffusion": plot_figs.get("ccyb_diffusion"),
            "cross_section_map": plot_figs.get("cross_section_map"),
            "cross_section_bar": plot_figs.get("cross_section_bar"),
            "risk_plot": plot_figs.get("risk_plot"),
            "syrb_counts_trend": plot_figs.get("syrb_counts_trend"),
            "syrb_sector": plot_figs.get("syrb_sector"),
            "bbm_diffusion": plot_figs.get("bbm_diffusion"),
            "capital_overall_buffers": plot_figs.get("capital_overall_buffers"),
        }
        
        download_data_input = {
            "ccyb_diffusion": download_data.get("ccyb_diffusion"),
            "bbm_diffusion": download_data.get("bbm_diffusion"),
            "capital_overall_buffers": download_data.get("capital_overall_buffers"),
        }
        
        news_feed_html = build_news_feed_html(news_df, today_str=today_str)
        
        # OSII/GSII data processing
        osii_by_country = prepare_osii_by_country(data.get('osii_df'))
        osii_countries = get_osii_countries(data.get('osii_df'))
        osii_table_html = build_osii_table_html(osii_by_country, selected_country="Austria")
        all_sii_table_html = build_all_sii_institutions_table_html(data.get('osii_df'))
        
        # Fetch countries_data from Supabase if enabled, otherwise use pipeline data
        if self.use_supabase:
            supabase_countries_data = self._fetch_countries_data_from_supabase()
            if supabase_countries_data:
                countries_data = supabase_countries_data
                logger.info("Using countries data from Supabase")
            else:
                logger.warning("Supabase fetch returned empty data, falling back to pipeline data")
        
        # Supabase credentials for frontend
        supabase_url = self.supabase_config.get("url", "") if self.use_supabase else ""
        supabase_key = self.supabase_config.get("anon_key", "") if self.use_supabase else ""
        
        rendered_html = render_report(
            base_dir=self.base_dir,
            reports_dir=self.reports_dir,
            template_dir=self.base_dir,
            template_name="report_template.html",
            generation_date=today_str,
            analyses=analyses,
            plots_inline=plots_inline,
            plot_figs=plot_files_input,
            download_data=download_data_input,
            tables_html=tables_html,
            news_feed_html=news_feed_html,
            bbm_ref_date=bbm_data.get('bbm_ref_date', ''),
            ltv_ref_date=bbm_data.get('ltv_ref_date', ''),
            countries_data=countries_data,
            knowledge_graph_json=knowledge_graph_json,
            osii_countries=osii_countries,
            osii_table_html=osii_table_html,
            all_sii_table_html=all_sii_table_html,
            osii_by_country=osii_by_country,
            supabase_url=supabase_url,
            supabase_key=supabase_key,
        )
        
        return rendered_html
