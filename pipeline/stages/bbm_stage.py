"""
BBM (Borrower-Based Measures) Processing Stage.
Handles BBM data processing, LTV extraction, and DTI/LTI verification.
"""

import logging
import pandas as pd
from typing import Dict, Tuple, Any
from pipeline.writers.supabase_writer import SupabaseWriter

# Import from bbm package
from bbm import (
    build_bbm_matrix_html,
    extract_ltv_details_regex,
    build_dti_lti_eu_list_html,
)
from bbm.dti_lti_builder import build_dti_lti_comparison_df_structured
from bbm.dti_excel_loader import load_dti_expert_table

# Wrapper for backward compatibility
def build_dti_lti_comparison_df(bbm_full: pd.DataFrame, analyzer, search_config=None) -> pd.DataFrame:
    """Wrapper for build_dti_lti_comparison_df_structured (backward compatibility)."""
    from config import SEARCH_CONFIG
    return build_dti_lti_comparison_df_structured(
        bbm_full,
        analyzer,
        validate_with_ai=True,
        final_validation_with_search=False,
        search_config=search_config or SEARCH_CONFIG
    )

logger = logging.getLogger(__name__)


class BBMStage:
    """Processes BBM data including LTV tables and DTI/LTI verification."""
    
    def __init__(self, analyzer, search_config=None, supabase_writer: SupabaseWriter = None):
        """
        Initialize BBM stage.
        
        Args:
            analyzer: LLMAnalyzer instance for AI processing
            search_config: Optional search configuration for external validation
            supabase_writer: Optional Supabase writer for structured BBM data persistence
        """
        self.analyzer = analyzer
        self.search_config = search_config
        self.supabase_writer = supabase_writer or SupabaseWriter()
    
    def process(self, bbm_full: pd.DataFrame) -> Dict[str, Any]:
        """
        Process BBM data.
        
        Args:
            bbm_full: Full BBM dataframe
            
        Returns:
            Dictionary with processed BBM data:
            - active_bbm: Active BBM measures
            - bbm_decisions: Recent BBM decisions
            - bbm_pivot_html: HTML for BBM matrix
            - bbm_ref_date: Reference date for BBM data
            - ltv_table: LTV table dataframe
            - ltv_ref_date: Reference date for LTV data
            - dti_lti_compare: DTI/LTI comparison dataframe
            - dti_lti_eu_list_html: HTML for DTI/LTI EU list
        """
        active_bbm = pd.DataFrame()
        bbm_decisions = pd.DataFrame()
        bbm_pivot_html = ""
        bbm_ref_date = ""
        ltv_table = pd.DataFrame()
        ltv_ref_date = ""
        dti_lti_compare = pd.DataFrame()
        dti_lti_eu_list_html = ""
        
        if bbm_full is None or bbm_full.empty:
            dti_expert = load_dti_expert_table()
            return {
                'active_bbm': active_bbm,
                'bbm_decisions': bbm_decisions,
                'bbm_pivot_html': bbm_pivot_html,
                'bbm_ref_date': bbm_ref_date,
                'ltv_table': ltv_table,
                'ltv_ref_date': ltv_ref_date,
                'dti_lti_compare': dti_lti_compare,
                'dti_expert_table': dti_expert,
                'dti_lti_eu_list_html': dti_lti_eu_list_html,
            }
        
        logger.info("   -> BBM processing...")
        active_bbm = bbm_full[bbm_full['active_status'] == 'Active'].copy()
        bbm_pivot_html, bbm_ref_date = build_bbm_matrix_html(bbm_full)
        
        # A1) LTV Subsection Table (using new structured builder)
        try:
            logger.info("   -> LTV verification (ESRB + AI)...")
            from bbm.ltv_builder import build_ltv_comparison_df_structured
            # Get search config for external validation if not provided
            if self.search_config is None:
                from config import SEARCH_CONFIG
                self.search_config = SEARCH_CONFIG
            ltv_table = build_ltv_comparison_df_structured(
                bbm_full,
                self.analyzer,
                validate_with_ai=True,
                final_validation_with_search=True,
                search_config=self.search_config
            )
            
            # Get reference date
            ltv_active = bbm_full[
                (bbm_full['active_status'] == 'Active') &
                (bbm_full['measure_type'].astype(str).str.contains('LTV', case=False, na=False))
            ].copy()
            if not ltv_active.empty:
                max_date = ltv_active['date'].max()
                if pd.notna(max_date):
                    ltv_ref_date = max_date.strftime('%Y-%m-%d')
            
            logger.info(f"   -> LTV DataFrame shape: {ltv_table.shape}")
            logger.info(f"   -> LTV Countries: {sorted(ltv_table['Country'].unique().tolist()) if not ltv_table.empty and 'Country' in ltv_table.columns else []}")
        except Exception as exc:
            logger.warning(f"LTV comparison build failed: {exc}")
            import traceback
            logger.debug(traceback.format_exc())
            ltv_table = pd.DataFrame()
        
        # B) Legutóbbi 10 BBM döntés
        bbm_decisions = bbm_full.sort_values('date', ascending=False).head(10).copy()
        cols_bbm_dec = ['date', 'iso2', 'measure_type', 'status', 'description']
        bbm_decisions = bbm_decisions[[c for c in cols_bbm_dec if c in bbm_decisions.columns]]
        
        if not bbm_decisions.empty:
            logger.info("   -> BBM AI cleaning (Decisions)...")
            if 'date' in bbm_decisions.columns:
                bbm_decisions['date'] = pd.to_datetime(bbm_decisions['date']).dt.strftime('%Y-%m-%d')
            
            # AI Tisztítás a leírásra
            details = self.analyzer.extract_keywords(
                bbm_decisions['description'].astype(str).tolist(),
                "targeted risk or background"
            )
            bbm_decisions['description'] = details
            
            bbm_decisions.columns = [c.upper() for c in bbm_decisions.columns]
            bbm_decisions = bbm_decisions.rename(columns={
                'DATE': 'DATE',
                'ISO2': 'COUNTRY',
                'MEASURE_TYPE': 'TYPE',
                'STATUS': 'STATUS',
                'DESCRIPTION': 'DETAILS'
            })
        
        # C) DTI/LTI comparative table (EU only; ESRB + AI verified)
        try:
            logger.info("   -> DTI/LTI verification (ESRB + AI)...")
            # Get search config for external validation if not provided
            if self.search_config is None:
                from config import SEARCH_CONFIG
                self.search_config = SEARCH_CONFIG
            # Temporarily disable AI validation to see all extracted rules
            dti_lti_compare = build_dti_lti_comparison_df(bbm_full, self.analyzer, search_config=self.search_config)
            logger.info(f"   -> DTI/LTI DataFrame shape: {dti_lti_compare.shape}")
            logger.info(f"   -> DTI/LTI Countries: {sorted(dti_lti_compare['Country'].unique().tolist()) if not dti_lti_compare.empty and 'Country' in dti_lti_compare.columns else []}")
            dti_lti_eu_list_html = build_dti_lti_eu_list_html(bbm_full, self.analyzer)
            
            # Save to CSV if we have data
            if not dti_lti_compare.empty:
                logger.info(f"   -> DTI/LTI DataFrame shape: {dti_lti_compare.shape}")
                logger.info(f"   -> DTI/LTI Countries: {sorted(dti_lti_compare['Country'].unique().tolist()) if 'Country' in dti_lti_compare.columns else []}")
                try:
                    from pathlib import Path
                    from bbm.dti_lti_renderer import save_dti_lti_csv
                    csv_path = Path("data/dti_lti_rules.csv")
                    save_dti_lti_csv(dti_lti_compare, csv_path)
                    logger.info(f"   -> Saved DTI/LTI rules to {csv_path} ({len(dti_lti_compare)} rows)")
                except Exception as exc:
                    logger.warning(f"Failed to save DTI/LTI CSV: {exc}")
                    import traceback
                    logger.debug(traceback.format_exc())
            
            # Write structured BBM data to Supabase if enabled
            if self.supabase_writer.is_enabled():
                logger.info("Writing structured BBM data to Supabase...")
                results = self.supabase_writer.write_bbm_structured_data(
                    dti_lti_df=dti_lti_compare,
                    ltv_df=ltv_table,
                )
                if results:
                    logger.info(f"Supabase BBM write results: {results}")
        except Exception as exc:
            logger.warning(f"DTI/LTI comparison build failed: {exc}")
            dti_lti_compare = pd.DataFrame()
            dti_lti_eu_list_html = ""

        # DTI expert table (Excel schema, English) for BBM page display
        from config import BBM_EXCEL_PATH
        dti_expert_table = load_dti_expert_table(excel_path=BBM_EXCEL_PATH)

        return {
            'active_bbm': active_bbm,
            'bbm_decisions': bbm_decisions,
            'bbm_pivot_html': bbm_pivot_html,
            'bbm_ref_date': bbm_ref_date,
            'ltv_table': ltv_table,
            'ltv_ref_date': ltv_ref_date,
            'dti_lti_compare': dti_lti_compare,
            'dti_expert_table': dti_expert_table,
            'dti_lti_eu_list_html': dti_lti_eu_list_html,
        }
