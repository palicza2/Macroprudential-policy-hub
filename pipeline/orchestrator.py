"""
Pipeline Orchestrator.
Coordinates all pipeline stages in a stage-based architecture.
Uses PipelineContext to simplify data flow between stages.
"""

import logging
import os

from config import (
    BASE_DIR, DATA_DIR, URLS, FIGURES_DIR, REPORTS_DIR,
    LLM_CONFIG, SEARCH_CONFIG, NEWS_CONFIG, SUPABASE_RENDER_CONFIG,
    FORCE_REBUILD, NEWS_TTL_DAYS,
)
from utils import ensure_dirs

from pipeline.context import PipelineContext
from pipeline.manifest import PipelineManifest
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
        from pipeline.writers.supabase_writer import SupabaseWriter
        self.supabase_writer = SupabaseWriter()
        self.manifest = PipelineManifest(DATA_DIR, BASE_DIR, REPORTS_DIR, FIGURES_DIR)

        self.data_stage = DataStage(
            DATA_DIR,
            URLS["ccyb"],
            URLS["syrb"],
            URLS.get("capital_measures"),
            supabase_writer=self.supabase_writer,
        )
        self.visualization_stage = VisualizationStage(
            FIGURES_DIR,
            cache_path=self.manifest.plots_inline_path,
        )
        self.render_stage = RenderStage(
            BASE_DIR,
            REPORTS_DIR,
            NEWS_CONFIG,
            use_supabase=SUPABASE_RENDER_CONFIG["enabled"],
            supabase_config=SUPABASE_RENDER_CONFIG,
        )

    def run(self, run_grounding: bool = False, force: bool = None) -> None:
        logger.info("STARTING...")
        ensure_dirs(DATA_DIR, FIGURES_DIR, REPORTS_DIR)
        force = FORCE_REBUILD if force is None else force
        if force:
            logger.info("FORCE_REBUILD: all manifest skips disabled")

        ctx = PipelineContext()

        etl = self.data_stage.download_bronze()
        plan = self.manifest.plan(force=force, news_ttl_days=NEWS_TTL_DAYS)

        ctx.data = self.data_stage.process(skip=plan, etl=etl)
        plan = self.manifest.refine_after_silver(plan, force=force, news_ttl_days=NEWS_TTL_DAYS)
        plan.log()
        ctx.skip_plan = plan

        supabase_wrote = False
        if self.supabase_writer.is_enabled() and not plan.supabase:
            logger.info("Writing ETL data to Supabase...")
            results = self.supabase_writer.write_etl_data(
                ccyb_df=ctx.data.get("ccyb_df"),
                syrb_df=ctx.data.get("syrb_df"),
                bbm_df=ctx.data.get("bbm_df"),
                osii_df=ctx.data.get("osii_df"),
                latest_ccyb_df=ctx.data.get("latest_ccyb_df"),
                latest_syrb_df=ctx.data.get("latest_syrb_df"),
                latest_osii_df=ctx.data.get("latest_osii_df"),
                ccyb_trend_df=ctx.data.get("agg_trend_df"),
                syrb_trend_df=ctx.data.get("syrb_trend_df"),
                bbm_trend_df=ctx.data.get("bbm_trend_df"),
            )
            if results:
                logger.info("Supabase write results: %s", results)
            supabase_wrote = True
        elif self.supabase_writer.is_enabled() and plan.supabase:
            logger.info("Skipping Supabase ETL upsert (silver unchanged)")

        ctx.plots_inline, ctx.plot_figs, ctx.download_data, ctx.paths = (
            self.visualization_stage.process(ctx.data, skip=plan.viz)
        )

        ai_stage = AIStage(LLM_CONFIG, SEARCH_CONFIG, run_grounding)
        bbm_stage = BBMStage(
            ai_stage.analyzer,
            search_config=SEARCH_CONFIG,
            supabase_writer=self.supabase_writer,
        )
        ctx.bbm_data = bbm_stage.process(
            ctx.data.get("bbm_df"),
            skip_llm=plan.llm,
            skip_supabase=plan.supabase,
        )

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

        cached = self.manifest.load_llm_cache() if plan.llm else None
        ai_results = ai_stage.process(
            ctx.data.get("ccyb_df"),
            ctx.data.get("syrb_df"),
            analysis_inputs,
            ctx.paths,
            ctx.data,
            skip=plan.llm,
            cached=cached,
        )
        ctx.analyses = ai_results["analyses"]
        ctx.ccyb_decisions = ai_results["ccyb_decisions"]
        ctx.active_syrb = ai_results["active_syrb"]
        ctx.syrb_decisions = ai_results["syrb_decisions"]

        if ctx.bbm_data.get("dti_lti_eu_list_html"):
            ctx.analyses["bbm_dti_lti_eu_list"] = ctx.bbm_data["dti_lti_eu_list_html"]

        if plan.llm and cached and cached.get("countries_data"):
            logger.info("3c. Country Profiles... (skipped; reusing cache)")
            ctx.countries_data = cached["countries_data"]
        else:
            profile_stage = ProfileStage(ai_stage.analyzer)
            profile_results = profile_stage.process(ctx.data, ctx.analyses, skip_ai=plan.llm)
            ctx.countries_data = profile_results["countries_data"]
            ctx.analyses = profile_results["analyses"]

        if self.supabase_writer.is_enabled() and ctx.countries_data and not plan.llm:
            self.supabase_writer.write_institutional_setup(ctx.countries_data)

        if not plan.llm:
            self.manifest.save_llm_cache(
                ctx.analyses,
                ctx.countries_data,
                ctx.ccyb_decisions,
                ctx.active_syrb,
                ctx.syrb_decisions,
            )

        rendered_html = self.render_stage.process(ctx)

        with open("index.html", "w", encoding="utf-8") as f:
            f.write(rendered_html)

        self.manifest.write(
            plan=plan,
            news_fetched=ctx.news_fetched,
            supabase_wrote=supabase_wrote,
        )
        logger.info("DONE: index.html")


def main():
    """Main entry point."""
    env_val = os.getenv("RUN_GROUNDING", "").strip().lower()
    run_grounding = env_val in ("1", "true", "yes", "on")

    orchestrator = PipelineOrchestrator()
    orchestrator.run(run_grounding=run_grounding)


if __name__ == "__main__":
    main()
