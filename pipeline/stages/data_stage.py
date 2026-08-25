"""
Data ETL Stage.
Handles data extraction, transformation, and loading.
"""

import logging
from pathlib import Path
from typing import Any, Dict, Optional

from etl import ETLPipeline
from capital_overall import build_capital_overall_df
from pipeline.writers.supabase_writer import SupabaseWriter
from pipeline.manifest import SkipPlan

logger = logging.getLogger(__name__)


class DataStage:
    """Processes data extraction and transformation."""

    def __init__(
        self,
        data_dir: Path,
        ccyb_url: str,
        syrb_url: str,
        capital_measures_url: str = None,
        supabase_writer: SupabaseWriter = None,
    ):
        self.data_dir = data_dir
        self.ccyb_url = ccyb_url
        self.syrb_url = syrb_url
        self.capital_measures_url = capital_measures_url
        self.supabase_writer = supabase_writer or SupabaseWriter()

    def download_bronze(self) -> ETLPipeline:
        etl = ETLPipeline(self.data_dir, self.ccyb_url, self.syrb_url, self.capital_measures_url)
        etl.download_bronze()
        return etl

    def process(self, skip: Optional[SkipPlan] = None, etl: ETLPipeline = None) -> Dict[str, Any]:
        """Parse bronze or load silver parquet according to skip flags."""
        logger.info("1. Adatfeldolgozás...")
        etl = etl or ETLPipeline(self.data_dir, self.ccyb_url, self.syrb_url, self.capital_measures_url)
        skip = skip or SkipPlan()

        if skip.etl:
            data = etl.load_silver()
        else:
            data = etl.run_pipeline(
                skip_ccyb=skip.ccyb,
                skip_measures=skip.measures,
                skip_capital=skip.capital,
            )

        data["capital_overall_df"] = build_capital_overall_df(
            ccyb_df=data.get("ccyb_df"),
            syrb_df=data.get("syrb_df"),
            osii_df=data.get("osii_df"),
            ccob_rate=2.5,
        )
        return data
