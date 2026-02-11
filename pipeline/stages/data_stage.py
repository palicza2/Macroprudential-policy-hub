"""
Data ETL Stage.
Handles data extraction, transformation, and loading.
"""

import logging
from pathlib import Path
from typing import Dict, Any

from etl import ETLPipeline
from capital_overall import build_capital_overall_df

logger = logging.getLogger(__name__)


class DataStage:
    """Processes data extraction and transformation."""
    
    def __init__(self, data_dir: Path, ccyb_url: str, syrb_url: str, capital_measures_url: str = None):
        """
        Initialize data stage.
        
        Args:
            data_dir: Directory for data files
            ccyb_url: URL for CCyB data
            syrb_url: URL for SyRB data
            capital_measures_url: Optional URL for capital measures data
        """
        self.data_dir = data_dir
        self.ccyb_url = ccyb_url
        self.syrb_url = syrb_url
        self.capital_measures_url = capital_measures_url
    
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
        
        return data
