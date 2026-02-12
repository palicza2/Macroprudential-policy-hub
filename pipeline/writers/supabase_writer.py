"""
Supabase Writer Module.
Handles writing data to Supabase during pipeline execution.
"""

import logging
import os
import pandas as pd
from typing import Dict, Any, Optional, List
from pathlib import Path

from supabase import create_client, Client
# Import from archived supabase_migration (still in use)
# TODO: Refactor to remove dependency on archived module
import sys
from pathlib import Path
_base_dir = Path(__file__).parent.parent.parent
_archive_dir = _base_dir / "archive"
if _archive_dir.exists():
    sys.path.insert(0, str(_archive_dir))
try:
    from supabase_migration.config import SupabaseConfig
    from supabase_migration.transformers import (
        transform_ccyb_data,
        transform_syrb_data,
        transform_bbm_data,
        transform_osii_data,
        transform_dti_lti_data,
        transform_ltv_data,
        transform_countries,
        transform_snapshots,
        transform_trends,
    )
except ImportError:
    logger.warning("supabase_migration module not found in archive. Some features may not work.")
    SupabaseConfig = None
    transform_ccyb_data = None
    transform_syrb_data = None
    transform_bbm_data = None
    transform_osii_data = None
    transform_dti_lti_data = None
    transform_ltv_data = None
    transform_countries = None
    transform_snapshots = None
    transform_trends = None

logger = logging.getLogger(__name__)


class SupabaseWriter:
    """Writes pipeline data to Supabase."""
    
    def __init__(self, enabled: bool = None):
        """
        Initialize Supabase writer.
        
        Args:
            enabled: Whether Supabase writing is enabled. If None, checks ENABLE_SUPABASE env var.
        """
        if enabled is None:
            enabled = os.getenv("ENABLE_SUPABASE", "false").lower() in ("true", "1", "yes")
        
        self.enabled = enabled
        
        if self.enabled:
            try:
                config = SupabaseConfig()
                # Use service key for writes
                if config.service_key:
                    self.client = create_client(config.url, config.service_key)
                    logger.info("Supabase writer initialized with service role key")
                else:
                    logger.warning("ENABLE_SUPABASE is True but SUPABASE_SERVICE_KEY not found. Using anon key (may fail with RLS).")
                    self.client = create_client(config.url, config.anon_key)
                self.config = config
            except Exception as e:
                logger.error(f"Failed to initialize Supabase writer: {e}")
                logger.warning("Disabling Supabase writer. Pipeline will continue without Supabase writes.")
                self.enabled = False
                self.client = None
        else:
            self.client = None
            self.config = None
            logger.info("Supabase writer disabled (ENABLE_SUPABASE=false or not set)")
    
    def is_enabled(self) -> bool:
        """Check if Supabase writing is enabled."""
        return self.enabled and self.client is not None
    
    def write_etl_data(
        self,
        ccyb_df: Optional[pd.DataFrame] = None,
        syrb_df: Optional[pd.DataFrame] = None,
        bbm_df: Optional[pd.DataFrame] = None,
        osii_df: Optional[pd.DataFrame] = None,
        latest_ccyb_df: Optional[pd.DataFrame] = None,
        latest_syrb_df: Optional[pd.DataFrame] = None,
        latest_osii_df: Optional[pd.DataFrame] = None,
        ccyb_trend_df: Optional[pd.DataFrame] = None,
        syrb_trend_df: Optional[pd.DataFrame] = None,
        bbm_trend_df: Optional[pd.DataFrame] = None,
    ) -> Dict[str, int]:
        """
        Write ETL data to Supabase.
        
        Returns:
            Dictionary with counts of written records per table.
        """
        if not self.is_enabled():
            return {}
        
        results = {}
        
        try:
            # 1. Countries (if not already in DB, or update if needed)
            logger.info("Writing countries to Supabase...")
            # transform_countries needs all dataframes to extract unique countries
            countries_records = transform_countries(
                ccyb_df if ccyb_df is not None and not ccyb_df.empty else pd.DataFrame(),
                syrb_df if syrb_df is not None and not syrb_df.empty else pd.DataFrame(),
                bbm_df if bbm_df is not None and not bbm_df.empty else pd.DataFrame(),
                osii_df if osii_df is not None and not osii_df.empty else pd.DataFrame()
            )
            if countries_records:
                # Remove duplicates
                countries_df = pd.DataFrame(countries_records)
                countries_df = countries_df.drop_duplicates(subset=['iso2'])
                records = countries_df.to_dict('records')
                response = self.client.table("countries").upsert(records).execute()
                results['countries'] = len(response.data) if response.data else len(records)
                logger.info(f"  -> {results['countries']} countries written")
            
            # 2. CCyB Decisions
            if ccyb_df is not None and not ccyb_df.empty:
                logger.info("Writing CCyB decisions to Supabase...")
                ccyb_records = transform_ccyb_data(ccyb_df)
                if ccyb_records:
                    # Remove duplicates
                    ccyb_df_clean = pd.DataFrame(ccyb_records)
                    ccyb_df_clean = ccyb_df_clean.drop_duplicates(subset=['country_iso2', 'effective_date'])
                    records = ccyb_df_clean.to_dict('records')
                    response = self.client.table("ccyb_decisions").upsert(
                        records,
                        on_conflict="country_iso2,effective_date"
                    ).execute()
                    results['ccyb'] = len(response.data) if response.data else len(records)
                    logger.info(f"  -> {results['ccyb']} CCyB decisions written")
            
            # 3. SyRB Measures
            if syrb_df is not None and not syrb_df.empty:
                logger.info("Writing SyRB measures to Supabase...")
                syrb_records = transform_syrb_data(syrb_df)
                if syrb_records:
                    records = pd.DataFrame(syrb_records).to_dict('records')
                    response = self.client.table("syrb_measures").upsert(records).execute()
                    results['syrb'] = len(response.data) if response.data else len(records)
                    logger.info(f"  -> {results['syrb']} SyRB measures written")
            
            # 4. BBM Measures
            if bbm_df is not None and not bbm_df.empty:
                logger.info("Writing BBM measures to Supabase...")
                bbm_records = transform_bbm_data(bbm_df)
                if bbm_records:
                    records = pd.DataFrame(bbm_records).to_dict('records')
                    response = self.client.table("bbm_measures").upsert(records).execute()
                    results['bbm'] = len(response.data) if response.data else len(records)
                    logger.info(f"  -> {results['bbm']} BBM measures written")
            
            # 5. OSII Banks
            if osii_df is not None and not osii_df.empty:
                logger.info("Writing OSII banks to Supabase...")
                osii_records = transform_osii_data(osii_df)
                if osii_records:
                    records = pd.DataFrame(osii_records).to_dict('records')
                    response = self.client.table("osii_banks").upsert(records).execute()
                    results['osii'] = len(response.data) if response.data else len(records)
                    logger.info(f"  -> {results['osii']} OSII banks written")
            
            # 6. Snapshots
            snapshots = {}
            if latest_ccyb_df is not None and not latest_ccyb_df.empty:
                snapshots['ccyb'] = latest_ccyb_df
            if latest_syrb_df is not None and not latest_syrb_df.empty:
                snapshots['syrb'] = latest_syrb_df
            if latest_osii_df is not None and not latest_osii_df.empty:
                snapshots['osii'] = latest_osii_df
            
            # Note: Snapshot tables are now Materialized Views (mv_latest_*_snapshot)
            # They are automatically refreshed when source data (ccyb_decisions, syrb_measures, osii_banks) changes.
            # We no longer write directly to snapshot tables - they are computed views.
            # Keeping this code for backward compatibility, but it will fail if old tables don't exist.
            # TODO: Remove snapshot writing after migration to Materialized Views is complete.
            if snapshots:
                logger.info("Writing snapshots to Supabase...")
                logger.warning("Note: Snapshot tables are now Materialized Views. This write may fail if old tables are dropped.")
                snapshot_records = transform_snapshots(snapshots)
                for table_name, records in snapshot_records.items():
                    if records:
                        # Remove duplicates
                        df = pd.DataFrame(records)
                        if table_name == 'ccyb':
                            df = df.drop_duplicates(subset=['country_iso2'])
                        elif table_name == 'syrb':
                            df = df.drop_duplicates(subset=['country_iso2'])
                        elif table_name == 'osii':
                            df = df.drop_duplicates(subset=['country_iso2'])
                        records_clean = df.to_dict('records')
                        
                        # Old table names (will be removed after migration)
                        table_map = {
                            'ccyb': 'latest_ccyb_snapshot',
                            'syrb': 'latest_syrb_snapshot',
                            'osii': 'latest_osii_snapshot'
                        }
                        try:
                            response = self.client.table(table_map[table_name]).upsert(
                                records_clean,
                                on_conflict="country_iso2"
                            ).execute()
                            results[f'{table_name}_snapshot'] = len(response.data) if response.data else len(records_clean)
                            logger.info(f"  -> {results[f'{table_name}_snapshot']} {table_name} snapshots written")
                        except Exception as e:
                            logger.warning(f"  -> Failed to write {table_name} snapshots (table may be dropped): {e}")
                            # Don't fail - Materialized Views will be refreshed automatically
            
            # 7. Trends
            trends = {}
            if ccyb_trend_df is not None and not ccyb_trend_df.empty:
                trends['ccyb'] = ccyb_trend_df
            if syrb_trend_df is not None and not syrb_trend_df.empty:
                trends['syrb'] = syrb_trend_df
            if bbm_trend_df is not None and not bbm_trend_df.empty:
                trends['bbm'] = bbm_trend_df
            
            # Note: Trend tables are now Materialized Views (mv_*_trend)
            # They are automatically refreshed when source data changes.
            # We no longer write directly to trend tables - they are computed views.
            # TODO: Remove trend writing after migration to Materialized Views is complete.
            if trends:
                logger.info("Writing trends to Supabase...")
                logger.warning("Note: Trend tables are now Materialized Views. This write may fail if old tables are dropped.")
                trend_records = transform_trends(trends)
                for table_name, records in trend_records.items():
                    if records:
                        # Remove duplicates
                        df = pd.DataFrame(records)
                        df = df.drop_duplicates(subset=['date'])
                        records_clean = df.to_dict('records')
                        
                        # Old table names (will be removed after migration)
                        table_map = {
                            'ccyb': 'ccyb_diffusion_trend',
                            'syrb': 'syrb_trend',
                            'bbm': 'bbm_diffusion_trend'
                        }
                        try:
                            response = self.client.table(table_map[table_name]).upsert(
                                records_clean,
                                on_conflict="date"
                            ).execute()
                            results[f'{table_name}_trend'] = len(response.data) if response.data else len(records_clean)
                            logger.info(f"  -> {results[f'{table_name}_trend']} {table_name} trend records written")
                        except Exception as e:
                            logger.warning(f"  -> Failed to write {table_name} trends (table may be dropped): {e}")
                            # Don't fail - Materialized Views will be refreshed automatically
            
        except Exception as e:
            logger.error(f"Error writing ETL data to Supabase: {e}", exc_info=True)
            # Don't raise - allow pipeline to continue
        
        return results
    
    def write_bbm_structured_data(
        self,
        dti_lti_df: Optional[pd.DataFrame] = None,
        ltv_df: Optional[pd.DataFrame] = None,
    ) -> Dict[str, int]:
        """
        Write structured BBM data (DTI/LTI and LTV) to Supabase.
        
        Returns:
            Dictionary with counts of written records per table.
        """
        if not self.is_enabled():
            return {}
        
        results = {}
        
        try:
            # 1. DTI/LTI Rules
            if dti_lti_df is not None and not dti_lti_df.empty:
                logger.info("Writing DTI/LTI rules to Supabase...")
                dti_lti_records = transform_dti_lti_data(dti_lti_df)
                if dti_lti_records:
                    # Remove duplicates
                    df = pd.DataFrame(dti_lti_records)
                    df = df.drop_duplicates(subset=['country_iso2', 'measure_code'])
                    records = df.to_dict('records')
                    response = self.client.table("dti_lti_rules").upsert(
                        records,
                        on_conflict="country_iso2,measure_code"
                    ).execute()
                    results['dti_lti'] = len(response.data) if response.data else len(records)
                    logger.info(f"  -> {results['dti_lti']} DTI/LTI rules written")
            
            # 2. LTV Rules
            if ltv_df is not None and not ltv_df.empty:
                logger.info("Writing LTV rules to Supabase...")
                ltv_records = transform_ltv_data(ltv_df)
                if ltv_records:
                    # Remove duplicates
                    df = pd.DataFrame(ltv_records)
                    df = df.drop_duplicates(subset=['country_iso2'])
                    records = df.to_dict('records')
                    response = self.client.table("ltv_rules").upsert(
                        records,
                        on_conflict="country_iso2"
                    ).execute()
                    results['ltv'] = len(response.data) if response.data else len(records)
                    logger.info(f"  -> {results['ltv']} LTV rules written")
            
        except Exception as e:
            logger.error(f"Error writing BBM structured data to Supabase: {e}", exc_info=True)
            # Don't raise - allow pipeline to continue
        
        return results
