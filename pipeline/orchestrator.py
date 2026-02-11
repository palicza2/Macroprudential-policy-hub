"""
Pipeline Orchestrator.
Coordinates all pipeline stages in a stage-based architecture.
"""

import logging
import os
import json
from pathlib import Path
from typing import Dict, Any

from config import (
    BASE_DIR, DATA_DIR, URLS, FIGURES_DIR, REPORTS_DIR,
    LLM_CONFIG, SEARCH_CONFIG, NEWS_CONFIG, SUPABASE_RENDER_CONFIG
)
from utils import ensure_dirs

from pipeline.stages import (
    DataStage,
    VisualizationStage,
    AIStage,
    BBMStage,
    ProfileStage,
    RenderStage,
)

logger = logging.getLogger(__name__)


class PipelineOrchestrator:
    """Orchestrates the entire pipeline using stage-based architecture."""
    
    def __init__(self):
        """Initialize orchestrator with all stages."""
        # Initialize Supabase writer (shared across stages)
        from pipeline.writers.supabase_writer import SupabaseWriter
        self.supabase_writer = SupabaseWriter()
        
        self.data_stage = DataStage(
            DATA_DIR, 
            URLS["ccyb"], 
            URLS["syrb"], 
            URLS.get("capital_measures"),
            supabase_writer=self.supabase_writer
        )
        self.visualization_stage = VisualizationStage(FIGURES_DIR)
        self.render_stage = RenderStage(
            BASE_DIR, 
            REPORTS_DIR, 
            NEWS_CONFIG,
            use_supabase=SUPABASE_RENDER_CONFIG["enabled"],
            supabase_config=SUPABASE_RENDER_CONFIG
        )
    
    def run(self, run_grounding: bool = False) -> None:
        """
        Run the complete pipeline.
        
        Args:
            run_grounding: Whether to run grounded validation
        """
        logger.info("STARTING...")
        ensure_dirs(DATA_DIR, FIGURES_DIR, REPORTS_DIR)
        
        # Stage 1: Data ETL
        data = self.data_stage.process()
        
        ccyb_full = data.get("ccyb_df")
        syrb_full = data.get("syrb_df")
        
        # Stage 2: Visualization
        plots_inline, plot_figs, download_data, paths = self.visualization_stage.process(data)
        
        # Stage 3: AI Analysis (initialize analyzer)
        ai_stage = AIStage(LLM_CONFIG, SEARCH_CONFIG, run_grounding)
        
        # Stage 3a: BBM Processing
        bbm_stage = BBMStage(
            ai_stage.analyzer, 
            search_config=SEARCH_CONFIG,
            supabase_writer=self.supabase_writer
        )
        bbm_full = data.get('bbm_df')
        bbm_data = bbm_stage.process(bbm_full)
        
        # Prepare analysis inputs
        analysis_inputs = {
            'latest_ccyb_df': data.get('latest_ccyb_df'),
            'ccyb_decisions_df': None,  # Will be set after AI stage
            'active_syrb_df': None,  # Will be set after AI stage
            'syrb_decisions_df': None,  # Will be set after AI stage
            'active_bbm_df': bbm_data.get('active_bbm'),
            'bbm_decisions_df': bbm_data.get('bbm_decisions'),
            'latest_syrb_df': data.get('latest_syrb_df'),
            'latest_bbm_df': data.get('latest_bbm_df'),
            'ltv_table_df': bbm_data.get('ltv_table'),
            'news_df': None,  # Will be set in render stage
            'capital_overall_df': data.get('capital_overall_df'),
            'latest_osii_df': data.get('latest_osii_df'),
        }
        
        # Knowledge graph data (disabled)
        knowledge_graph_json = '{"nodes": [], "edges": []}'
        graph_data = {"nodes": [], "edges": []}
        logger.info("3d. Knowledge Graph... (DISABLED - skipped for performance)")
        
        if graph_data:
            analysis_inputs['knowledge_graph_data'] = graph_data
        
        # Stage 3b: AI Analysis (includes CCyB and SyRB decisions)
        ai_results = ai_stage.process(ccyb_full, syrb_full, analysis_inputs, paths, data)
        analyses = ai_results['analyses']
        ccyb_decisions = ai_results['ccyb_decisions']
        active_syrb = ai_results['active_syrb']
        syrb_decisions = ai_results['syrb_decisions']
        
        # Attach BBM DTI/LTI AI-verified EU list
        if bbm_data.get('dti_lti_eu_list_html'):
            analyses["bbm_dti_lti_eu_list"] = bbm_data.get('dti_lti_eu_list_html')
        
        # Stage 3c: Country Profiles
        profile_stage = ProfileStage(ai_stage.analyzer)
        profile_results = profile_stage.process(data, analyses)
        countries_data = profile_results['countries_data']
        analyses = profile_results['analyses']
        
        rendered_html = self.render_stage.process(
            data=data,
            analyses=analyses,
            plots_inline=plots_inline,
            plot_figs=plot_figs,
            download_data=download_data,
            ccyb_decisions=ccyb_decisions,
            active_syrb=active_syrb,
            syrb_decisions=syrb_decisions,
            bbm_data=bbm_data,
            capital_overall_df=data.get('capital_overall_df'),
            countries_data=countries_data,
            knowledge_graph_json=knowledge_graph_json,
        )
        
        # Write output
        with open("index.html", "w", encoding="utf-8") as f:
            f.write(rendered_html)
        logger.info("DONE: index.html")


def main():
    """Main entry point."""
    # Check if grounding should run
    run_grounding = False
    if os.getenv("RUN_GROUNDING", "").lower() in ("1", "true", "yes", "y"):
        run_grounding = True
    else:
        try:
            answer = input("Run grounded validation? (y/N): ").strip().lower()
            run_grounding = answer in ("y", "yes")
        except Exception:
            run_grounding = False
    
    orchestrator = PipelineOrchestrator()
    orchestrator.run(run_grounding=run_grounding)


if __name__ == "__main__":
    main()
