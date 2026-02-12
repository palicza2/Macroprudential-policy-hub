"""
BBM (Borrower-Based Measures) package.
Contains structured DTI/LTI and LTV data models and extraction logic.
"""

# Import new structured DTI/LTI model
from .dti_lti_model import (
    DTILTIRule,
    MeasureCode,
    ImplementationStatus,
    LegalForm,
    IncomeBasis,
    create_dti_lti_schema,
    rules_to_dataframe,
    dataframe_to_rules,
)

# Import new structured LTV model
from .ltv_model import (
    LTVRule,
    create_ltv_schema,
    rules_to_dataframe as ltv_rules_to_dataframe,
    dataframe_to_rules as ltv_dataframe_to_rules,
)

# Import BBM matrix builder
from .matrix_builder import build_bbm_matrix_html, RENAME_MAP

# Import DTI/LTI builders
from .dti_lti import build_dti_lti_items, build_dti_lti_eu_list_html, EU_ISO2

# Import legacy LTV extractor (for backward compatibility)
from .ltv_legacy import extract_ltv_details_regex

__all__ = [
    'DTILTIRule',
    'MeasureCode',
    'ImplementationStatus',
    'LegalForm',
    'IncomeBasis',
    'create_dti_lti_schema',
    'rules_to_dataframe',
    'dataframe_to_rules',
    'LTVRule',
    'create_ltv_schema',
    'ltv_rules_to_dataframe',
    'ltv_dataframe_to_rules',
    'build_bbm_matrix_html',
    'RENAME_MAP',
    'build_dti_lti_items',
    'build_dti_lti_eu_list_html',
    'EU_ISO2',
    'extract_ltv_details_regex',
]
