"""
Pipeline Orchestrator.
Coordinates all pipeline stages in a stage-based architecture.
Uses PipelineContext to simplify data flow between stages.
"""

import logging
import os
from pathlib import Path
from typing import Dict, Any

from config import (
    BASE_DIR, DATA_DIR, URLS, FIGURES_DIR, REPORTS_DIR,
    LLM_CONFIG, SEARCH_CONFIG, NEWS_CONFIG, SUPABASE_RENDER_CONFIG
)
from utils import ensure_dirs

from pipeline.context import PipelineContext
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

        ctx = PipelineContext()

        # Stage 1: Data ETL
        ctx.data = self.data_stage.process()

        # Stage 2: Visualization
        ctx.plots_inline, ctx.plot_figs, ctx.download_data, ctx.paths = (
            self.visualization_stage.process(ctx.data)
        )

        # Stage 3: AI (initialize)
        ai_stage = AIStage(LLM_CONFIG, SEARCH_CONFIG, run_grounding)
        bbm_stage = BBMStage(
            ai_stage.analyzer,
            search_config=SEARCH_CONFIG,
            supabase_writer=self.supabase_writer
        )

        # Stage 3a: BBM Processing
        ctx.bbm_data = bbm_stage.process(ctx.data.get("bbm_df"))

        # Prepare analysis inputs from context
        analysis_inputs = {
            "latest_ccyb_df": ctx.data.get("latest_ccyb_df"),
            "ccyb_decisions_df": None,
            "active_syrb_df": None,
            "syrb_decisions_df": None,
            "active_bbm_df": ctx.bbm_data.get("active_bbm"),
            "bbm_decisions_df": ctx.bbm_data.get("bbm_decisions"),
            "latest_syrb_df": ctx.data.get("latest_syrb_df"),
            "latest_bbm_df": ctx.data.get("latest_bbm_df"),
            "ltv_table_df": ctx.bbm_data.get("ltv_table"),
            "news_df": None,
            "capital_overall_df": ctx.data.get("capital_overall_df"),
            "latest_osii_df": ctx.data.get("latest_osii_df"),
            "knowledge_graph_data": {"nodes": [], "edges": []},
        }

        logger.info("3d. Knowledge Graph... (DISABLED - skipped for performance)")

        # Stage 3b: AI Analysis
        ai_results = ai_stage.process(
            ctx.data.get("ccyb_df"),
            ctx.data.get("syrb_df"),
            analysis_inputs,
            ctx.paths,
            ctx.data,
        )
        ctx.analyses = ai_results["analyses"]
        ctx.ccyb_decisions = ai_results["ccyb_decisions"]
        ctx.active_syrb = ai_results["active_syrb"]
        ctx.syrb_decisions = ai_results["syrb_decisions"]

        if ctx.bbm_data.get("dti_lti_eu_list_html"):
            ctx.analyses["bbm_dti_lti_eu_list"] = ctx.bbm_data["dti_lti_eu_list_html"]

        # Stage 3c: Country Profiles
        profile_stage = ProfileStage(ai_stage.analyzer)
        profile_results = profile_stage.process(ctx.data, ctx.analyses)
        ctx.countries_data = profile_results["countries_data"]
        ctx.analyses = profile_results["analyses"]

        # Stage 4: Render
        rendered_html = self.render_stage.process(ctx)

        with open("index.html", "w", encoding="utf-8") as f:
            f.write(rendered_html)
        logger.info("DONE: index.html")


def main():
    """Main entry point."""
    env_val = os.getenv("RUN_GROUNDING", "").strip().lower()
    run_grounding = env_val not in ("0", "false", "no", "n")

    orchestrator = PipelineOrchestrator()
    orchestrator.run(run_grounding=run_grounding)


if __name__ == "__main__":
    main()
