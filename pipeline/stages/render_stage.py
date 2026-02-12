"""
Render Stage.
Handles final HTML report rendering.
"""

import logging
import os
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional

from render import render_report, df_to_html_table
from news import fetch_news, build_news_feed_html
from osii import prepare_osii_by_country, build_osii_table_html, get_osii_countries

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
            
            ccyb_snapshots = {row["country_iso2"]: row for row in ccyb_snapshots_resp.data}
            syrb_snapshots = {row["country_iso2"]: row for row in syrb_snapshots_resp.data}
            osii_snapshots = {row["country_iso2"]: row for row in osii_snapshots_resp.data}
            
            # Fetch historical data
            ccyb_decisions_resp = self.supabase_client.table("ccyb_decisions").select("*").order("effective_date").execute()
            syrb_measures_resp = self.supabase_client.table("syrb_measures").select("*").order("effective_date").execute()
            bbm_measures_resp = self.supabase_client.table("bbm_measures").select("*").order("effective_date").execute()
            
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
                
                # BBM types
                bbm_types = []
                for bbm in bbm_by_country.get(iso2, []):
                    if bbm.get("active_status") == "Active" or bbm.get("status") == "Active":
                        measure_type = bbm.get("measure_type", "")
                        if measure_type and measure_type not in bbm_types:
                            bbm_types.append(measure_type)
                
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
                    "osii": {
                        "rate": float(osii_snap.get("total_rate", 0)) if osii_snap.get("total_rate") else 0.0,  # Materialized View uses total_rate
                        "status": "Active" if osii_snap.get("total_rate", 0) > 0 else "Inactive",
                    } if osii_snap else None,
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
                    "bbm": [m for m in bbm_by_country.get(iso2, []) if m.get("active_status") == "Active" or m.get("status") == "Active"],
                    "osii": current_status.get("osii"),
                }
                
                countries_data[country_name] = {
                    "country": country_name,
                    "iso2": iso2,
                    "current_status": current_status,
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
    
    def process(
        self,
        data: Dict[str, Any],
        analyses: Dict[str, str],
        plots_inline: Dict[str, str],
        plot_figs: Dict[str, Any],
        download_data: Dict[str, Any],
        ccyb_decisions,
        active_syrb,
        syrb_decisions,
        bbm_data: Dict[str, Any],
        capital_overall_df,
        countries_data: Dict[str, Any],
        knowledge_graph_json: str,
    ) -> str:
        """
        Render final HTML report.
        
        Args:
            data: Processed data dictionary
            analyses: AI analyses
            plots_inline: Inline plot HTML
            plot_figs: Plot figure files
            download_data: Download data
            ccyb_decisions: CCyB decisions dataframe
            active_syrb: Active SyRB dataframe
            syrb_decisions: SyRB decisions dataframe
            bbm_data: BBM processing results
            capital_overall_df: Capital overall dataframe
            countries_data: Country profiles data
            knowledge_graph_json: Knowledge graph JSON string
            
        Returns:
            Rendered HTML string
        """
        logger.info("4. Riport...")
        
        # News Processing
        import os
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
        
        tables_html = {
            "ccyb_decisions": df_to_html_table(ccyb_decisions),
            "syrb_active": df_to_html_table(active_syrb),
            "syrb_decisions": df_to_html_table(syrb_decisions),
            "bbm_pivot": bbm_data.get('bbm_pivot_html', "") or "<p class='no-data'>No Data</p>",
            "bbm_decisions": df_to_html_table(bbm_data.get('bbm_decisions')),
            "ltv_table": df_to_html_table(bbm_data.get('ltv_table'), table_type="ltv"),
            "dti_lti_compare": df_to_html_table(bbm_data.get('dti_lti_compare'), table_type="dti_lti"),
            "capital_overall": df_to_html_table(capital_overall_df),
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
            osii_by_country=osii_by_country,
            supabase_url=supabase_url,
            supabase_key=supabase_key,
        )
        
        return rendered_html
