"""
BBM (Borrower-Based Measures) - Legacy compatibility module.

⚠️ DEPRECATED: This module is kept for backward compatibility only.
All functions have been moved to the bbm/ package:
- build_bbm_matrix_html -> bbm.matrix_builder
- build_dti_lti_items -> bbm.dti_lti.items_builder
- build_dti_lti_comparison_df -> bbm.dti_lti_builder (wrapper)
- build_dti_lti_eu_list_html -> bbm.dti_lti.list_builder
- extract_ltv_details_regex -> bbm.ltv_legacy

Please update your imports to use the bbm package directly.
"""

import warnings

# Import from bbm package
from bbm import (
    build_bbm_matrix_html,
    build_dti_lti_items,
    build_dti_lti_eu_list_html,
    extract_ltv_details_regex,
    RENAME_MAP,
    EU_ISO2,
)
from bbm.dti_lti_builder import build_dti_lti_comparison_df_structured

# Wrapper for backward compatibility
def build_dti_lti_comparison_df(bbm_full, analyzer, search_config=None):
    """
    Build DTI/LTI comparison DataFrame (legacy wrapper).
    
    ⚠️ DEPRECATED: Use bbm.dti_lti_builder.build_dti_lti_comparison_df_structured instead.
    """
    warnings.warn(
        "bbm.build_dti_lti_comparison_df is deprecated. "
        "Use bbm.dti_lti_builder.build_dti_lti_comparison_df_structured instead.",
        DeprecationWarning,
        stacklevel=2
    )
    from config import SEARCH_CONFIG
    return build_dti_lti_comparison_df_structured(
        bbm_full,
        analyzer,
        validate_with_ai=True,
        final_validation_with_search=False,
        search_config=search_config or SEARCH_CONFIG
    )

# Export all for backward compatibility
__all__ = [
    'build_bbm_matrix_html',
    'build_dti_lti_items',
    'build_dti_lti_comparison_df',
    'build_dti_lti_eu_list_html',
    'extract_ltv_details_regex',
    'RENAME_MAP',
    'EU_ISO2',
]
