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
]
