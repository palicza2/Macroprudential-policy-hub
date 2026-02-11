"""
Data ETL Stage.
Handles data extraction, transformation, and loading.
"""

import logging
from pathlib import Path
from typing import Dict, Any

from etl import ETLPipeline
from capital_overall import build_capital_overall_df
from pipeline.writers.supabase_writer import SupabaseWriter

logger = logging.getLogger(__name__)


class DataStage:
    """Processes data extraction and transformation."""
    
    def __init__(self, data_dir: Path, ccyb_url: str, syrb_url: str, capital_measures_url: str = None, supabase_writer: SupabaseWriter = None):
        """
        Initialize data stage.
        
        Args:
            data_dir: Directory for data files
            ccyb_url: URL for CCyB data
            syrb_url: URL for SyRB data
            capital_measures_url: Optional URL for capital measures data
            supabase_writer: Optional Supabase writer for data persistence
        """
        self.data_dir = data_dir
        self.ccyb_url = ccyb_url
        self.syrb_url = syrb_url
        self.capital_measures_url = capital_measures_url
        self.supabase_writer = supabase_writer or SupabaseWriter()
    
    def process(self) -> Dict[str, Any]:
        """
        Run ETL pipeline and prepare capital overall data.
        
        Returns:
            Dictionary with processed data including capital_overall_df
        """
        logger.info("1. Adatfeldolgozás...")
        etl = ETLPipeline(self.data_dir, self.ccyb_url, self.syrb_url, self.capital_measures_url)
        data = etl.run_pipeline()
        
        # Capital overall dataframe (needs to be available for plotting)
        capital_overall_df = build_capital_overall_df(
            ccyb_df=data.get("ccyb_df"),
            syrb_df=data.get("syrb_df"),
            osii_df=data.get("osii_df"),
            ccob_rate=2.5,
        )
        data["capital_overall_df"] = capital_overall_df
        
        # Write to Supabase if enabled
        if self.supabase_writer.is_enabled():
            logger.info("Writing ETL data to Supabase...")
            results = self.supabase_writer.write_etl_data(
                ccyb_df=data.get("ccyb_df"),
                syrb_df=data.get("syrb_df"),
                bbm_df=data.get("bbm_df"),
                osii_df=data.get("osii_df"),
                latest_ccyb_df=data.get("latest_ccyb_df"),
                latest_syrb_df=data.get("latest_syrb_df"),
                latest_osii_df=data.get("latest_osii_df"),
                ccyb_trend_df=data.get("agg_trend_ccyb"),
                syrb_trend_df=data.get("syrb_trend"),
                bbm_trend_df=data.get("bbm_trend"),
            )
            if results:
                logger.info(f"Supabase write results: {results}")
        
        return data
