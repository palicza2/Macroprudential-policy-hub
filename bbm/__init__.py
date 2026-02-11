"""
BBM (Borrower-Based Measures) package.
Contains structured DTI/LTI data model and extraction logic.
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

__all__ = [
    'DTILTIRule',
    'MeasureCode',
    'ImplementationStatus',
    'LegalForm',
    'IncomeBasis',
    'create_dti_lti_schema',
    'rules_to_dataframe',
    'dataframe_to_rules',
]
