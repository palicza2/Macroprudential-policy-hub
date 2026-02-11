"""
DTI/LTI Data Model.
Structured data model for Debt-to-Income and Loan-to-Income measures.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional
import pandas as pd


class MeasureCode(str, Enum):
    """Type of measure: LTI (mortgage only) or DTI (total debt)."""
    LTI = "LTI"
    DTI = "DTI"


class ImplementationStatus(str, Enum):
    """Implementation status: separates timing from legality."""
    ACTIVE = "Active"
    WITHDRAWN = "Withdrawn"
    ANNOUNCED = "Announced"


class LegalForm(str, Enum):
    """Legal form: Binding (hard law) or Recommendation (soft law)."""
    BINDING = "Binding"
    RECOMMENDATION = "Recommendation"


class IncomeBasis(str, Enum):
    """Income basis: Gross (pre-tax) or Net (post-tax)."""
    GROSS = "Gross"
    NET = "Net"
    UNKNOWN = "Unknown"


@dataclass
class DTILTIRule:
    """
    Structured data model for DTI/LTI rules.
    
    Attributes:
        country: ISO2 country code (e.g., "DK", "IE")
        measure_code: LTI (mortgage only) or DTI (total debt)
        implementation_status: Active, Withdrawn, or Announced
        legal_form: Binding (hard law) or Recommendation (soft law)
        limit_standard: Standard multiplier (e.g., 4.5)
        limit_ftb: Preferential multiplier for First-Time Buyers (nullable)
        limit_btl: Stricter multiplier for Buy-to-Let/Investors (nullable)
        income_basis: Gross (pre-tax) or Net (post-tax) income
        allowance_share: Percentage of volume allowed to exceed limit (e.g., "15%")
    """
    country: str
    measure_code: MeasureCode
    implementation_status: ImplementationStatus
    legal_form: LegalForm
    limit_standard: Optional[float] = None  # Can be None if limit not found
    income_basis: IncomeBasis = IncomeBasis.UNKNOWN
    allowance_share: Optional[str] = None
    limit_ftb: Optional[float] = None
    limit_btl: Optional[float] = None
    limit_green: Optional[float] = None  # Green/sustainable mortgage limit (e.g., for LV)
    regulation_url: Optional[str] = None  # URL to authority's dedicated page for the measure
    notes: Optional[str] = None  # Additional notes/clarifications (e.g., "Amortization requirement trigger, not a hard cap")
    
    def to_dict(self) -> dict:
        """Convert to dictionary for DataFrame."""
        return {
            "Country": self.country,
            "Measure_Code": self.measure_code.value,
            "Implementation_Status": self.implementation_status.value,
            "Legal_Form": self.legal_form.value,
            "Limit_Standard": self.limit_standard,
            "Limit_FTB": self.limit_ftb,
            "Limit_BTL": self.limit_btl,
            "Limit_Green": self.limit_green,
            "Income_Basis": self.income_basis.value,
            "Allowance_Share": self.allowance_share,
            "Regulation_URL": self.regulation_url,
            "Notes": self.notes,
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> "DTILTIRule":
        """Create from dictionary."""
        return cls(
            country=str(data.get("Country", "")).strip(),
            measure_code=MeasureCode(data.get("Measure_Code", "DTI")),
            implementation_status=ImplementationStatus(data.get("Implementation_Status", "Active")),
            legal_form=LegalForm(data.get("Legal_Form", "Binding")),
            limit_standard=float(data["Limit_Standard"]) if pd.notna(data.get("Limit_Standard")) and data.get("Limit_Standard") is not None else None,
            limit_ftb=float(data["Limit_FTB"]) if pd.notna(data.get("Limit_FTB")) and data.get("Limit_FTB") else None,
            limit_btl=float(data["Limit_BTL"]) if pd.notna(data.get("Limit_BTL")) and data.get("Limit_BTL") else None,
            limit_green=float(data["Limit_Green"]) if pd.notna(data.get("Limit_Green")) and data.get("Limit_Green") else None,
            income_basis=IncomeBasis(data.get("Income_Basis", "Gross")),
            allowance_share=str(data.get("Allowance_Share", "")).strip(),
            regulation_url=str(data.get("Regulation_URL", "")).strip() if data.get("Regulation_URL") else None,
            notes=str(data.get("Notes", "")).strip() if data.get("Notes") else None,
        )


def create_dti_lti_schema() -> pd.DataFrame:
    """
    Create empty DataFrame with proper schema for DTI/LTI rules.
    
    Returns:
        Empty DataFrame with correct column types and names.
    """
    return pd.DataFrame({
        "Country": pd.Series(dtype="string"),
        "Measure_Code": pd.Series(dtype="string"),  # "LTI" or "DTI"
        "Implementation_Status": pd.Series(dtype="string"),  # "Active", "Withdrawn", "Announced"
        "Legal_Form": pd.Series(dtype="string"),  # "Binding" or "Recommendation"
        "Limit_Standard": pd.Series(dtype="float64"),
        "Limit_FTB": pd.Series(dtype="float64"),  # Nullable
        "Limit_BTL": pd.Series(dtype="float64"),  # Nullable
        "Limit_Green": pd.Series(dtype="float64"),  # Nullable - Green/sustainable mortgage limit
        "Income_Basis": pd.Series(dtype="string"),  # "Gross", "Net", or "Unknown"
        "Allowance_Share": pd.Series(dtype="string"),  # e.g., "15%"
        "Regulation_URL": pd.Series(dtype="string"),  # URL to authority's dedicated page
        "Notes": pd.Series(dtype="string"),  # Additional notes/clarifications
    })


def rules_to_dataframe(rules: list[DTILTIRule]) -> pd.DataFrame:
    """
    Convert list of DTILTIRule objects to DataFrame.
    
    Args:
        rules: List of DTILTIRule objects
        
    Returns:
        DataFrame with DTI/LTI rules
    """
    if not rules:
        return create_dti_lti_schema()
    
    data = [rule.to_dict() for rule in rules]
    df = pd.DataFrame(data)
    
    # Ensure proper types
    df["Limit_Standard"] = pd.to_numeric(df["Limit_Standard"], errors="coerce")
    df["Limit_FTB"] = pd.to_numeric(df["Limit_FTB"], errors="coerce")
    df["Limit_BTL"] = pd.to_numeric(df["Limit_BTL"], errors="coerce")
    df["Limit_Green"] = pd.to_numeric(df["Limit_Green"], errors="coerce")
    
    return df


def dataframe_to_rules(df: pd.DataFrame) -> list[DTILTIRule]:
    """
    Convert DataFrame to list of DTILTIRule objects.
    
    Args:
        df: DataFrame with DTI/LTI rules
        
    Returns:
        List of DTILTIRule objects
    """
    rules = []
    for _, row in df.iterrows():
        try:
            rule = DTILTIRule.from_dict(row.to_dict())
            rules.append(rule)
        except Exception as e:
            # Skip invalid rows
            continue
    return rules
