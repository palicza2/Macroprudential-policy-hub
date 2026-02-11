"""
Render Stage.
Handles final HTML report rendering.
"""

import logging
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Dict, Any

from render import render_report, df_to_html_table
from news import fetch_news, build_news_feed_html
from osii import prepare_osii_by_country, build_osii_table_html, get_osii_countries

logger = logging.getLogger(__name__)


class RenderStage:
    """Processes final report rendering."""
    
    def __init__(self, base_dir: Path, reports_dir: Path, news_config: Dict[str, Any]):
        """
        Initialize render stage.
        
        Args:
            base_dir: Base directory
            reports_dir: Reports directory
            news_config: News configuration
        """
        self.base_dir = base_dir
        self.reports_dir = reports_dir
        self.news_config = news_config
    
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
            "ltv_table": df_to_html_table(bbm_data.get('ltv_table')),
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
        )
        
        return rendered_html
