"""
BBM (Borrower-Based Measures) Processing Stage.
Handles BBM data processing, LTV extraction, and DTI/LTI verification.
"""

import logging
import pandas as pd
from typing import Dict, Tuple, Any

# Import from bbm.py (not from bbm package to avoid circular import)
import sys
from pathlib import Path
_bbm_py = Path(__file__).parent.parent.parent / "bbm.py"
if _bbm_py.exists():
    import importlib.util
    _spec = importlib.util.spec_from_file_location("_bbm_py_module", _bbm_py)
    _bbm_py_module = importlib.util.module_from_spec(_spec)
    _spec.loader.exec_module(_bbm_py_module)
    build_bbm_matrix_html = _bbm_py_module.build_bbm_matrix_html
    extract_ltv_details_regex = _bbm_py_module.extract_ltv_details_regex
    build_dti_lti_comparison_df = _bbm_py_module.build_dti_lti_comparison_df
    build_dti_lti_eu_list_html = _bbm_py_module.build_dti_lti_eu_list_html
else:
    raise ImportError("bbm.py not found")

logger = logging.getLogger(__name__)


class BBMStage:
    """Processes BBM data including LTV tables and DTI/LTI verification."""
    
    def __init__(self, analyzer, search_config=None):
        """
        Initialize BBM stage.
        
        Args:
            analyzer: LLMAnalyzer instance for AI processing
            search_config: Optional search configuration for external validation
        """
        self.analyzer = analyzer
        self.search_config = search_config
    
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
            return {
                'active_bbm': active_bbm,
                'bbm_decisions': bbm_decisions,
                'bbm_pivot_html': bbm_pivot_html,
                'bbm_ref_date': bbm_ref_date,
                'ltv_table': ltv_table,
                'ltv_ref_date': ltv_ref_date,
                'dti_lti_compare': dti_lti_compare,
                'dti_lti_eu_list_html': dti_lti_eu_list_html,
            }
        
        logger.info("   -> BBM processing...")
        active_bbm = bbm_full[bbm_full['active_status'] == 'Active'].copy()
        bbm_pivot_html, bbm_ref_date = build_bbm_matrix_html(bbm_full)
        
        # A1) LTV Subsection Table
        ltv_active = bbm_full[
            (bbm_full['active_status'] == 'Active') &
            (bbm_full['measure_type'].astype(str).str.contains('LTV', case=False, na=False))
        ].copy()
        
        if not ltv_active.empty:
            max_date = ltv_active['date'].max()
            if pd.notna(max_date):
                ltv_ref_date = max_date.strftime('%Y-%m-%d')
            
            descriptions = ltv_active['description'].fillna('').astype(str).tolist()
            ltv_llm = self.analyzer.extract_ltv_fields(descriptions)
            ltv_llm = ltv_llm if ltv_llm else [{} for _ in descriptions]
            llm_df = pd.DataFrame(ltv_llm)
            llm_df = llm_df.reindex(range(len(ltv_active))).fillna("")
            
            def normalize_limits(val):
                if isinstance(val, list):
                    cleaned = [str(v).strip() for v in val if str(v).strip()]
                    return ", ".join(sorted(
                        set(cleaned),
                        key=lambda x: float(x.strip('%')) if x.strip('%').replace('.', '').isdigit() else x
                    ))
                if isinstance(val, str) and val.strip():
                    return val.strip()
                return ""
            
            limits_series = llm_df['limits'] if 'limits' in llm_df.columns else pd.Series([""] * len(ltv_active))
            ftb_flag_series = llm_df['ftb_flag'] if 'ftb_flag' in llm_df.columns else pd.Series([""] * len(ltv_active))
            ftb_details_series = llm_df['ftb_details'] if 'ftb_details' in llm_df.columns else pd.Series([""] * len(ltv_active))
            other_series = llm_df['other_exceptions'] if 'other_exceptions' in llm_df.columns else pd.Series([""] * len(ltv_active))
            
            ltv_active['limits'] = limits_series.apply(normalize_limits)
            ltv_active['ftb_flag'] = ftb_flag_series.replace("", "No")
            ltv_active['ftb_details'] = ftb_details_series
            ltv_active['other_details'] = other_series
            
            # Fallback to regex if LLM output is missing
            for idx, row in ltv_active.iterrows():
                if not row.get('limits'):
                    limits_str, ftb_flag, ftb_details, other_details = extract_ltv_details_regex(
                        row.get('description', '')
                    )
                    ltv_active.at[idx, 'limits'] = limits_str
                    if row.get('ftb_flag') in ("", None):
                        ltv_active.at[idx, 'ftb_flag'] = ftb_flag
                    if not row.get('ftb_details'):
                        ltv_active.at[idx, 'ftb_details'] = ftb_details
                    if not row.get('other_details'):
                        ltv_active.at[idx, 'other_details'] = other_details
            
            ltv_table = (
                ltv_active.groupby('country', as_index=False)
                .agg({
                    'limits': lambda x: ", ".join(sorted(set(", ".join(x.fillna("").astype(str)).split(", ")))) if x.notna().any() else "N/A",
                    'ftb_flag': lambda x: "Yes" if (x == "Yes").any() else "No",
                    'ftb_details': lambda x: " ".join([v for v in x.fillna("").astype(str) if v]).strip(),
                    'other_details': lambda x: " ".join([v for v in x.fillna("").astype(str) if v]).strip(),
                })
            )
            ltv_table = ltv_table.rename(columns={
                'country': 'COUNTRY',
                'limits': 'LTV LIMITS',
                'ftb_flag': 'FTB DISCOUNT',
                'ftb_details': 'FTB DETAILS',
                'other_details': 'OTHER EXCEPTIONS'
            })
        
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
        except Exception as exc:
            logger.warning(f"DTI/LTI comparison build failed: {exc}")
            dti_lti_compare = pd.DataFrame()
            dti_lti_eu_list_html = ""
        
        return {
            'active_bbm': active_bbm,
            'bbm_decisions': bbm_decisions,
            'bbm_pivot_html': bbm_pivot_html,
            'bbm_ref_date': bbm_ref_date,
            'ltv_table': ltv_table,
            'ltv_ref_date': ltv_ref_date,
            'dti_lti_compare': dti_lti_compare,
            'dti_lti_eu_list_html': dti_lti_eu_list_html,
        }
