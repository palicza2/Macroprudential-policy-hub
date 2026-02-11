"""
LTV Data Model.
Structured data model for Loan-to-Value measures.
"""

from dataclasses import dataclass
from enum import Enum
from typing import Optional, Union, List
import pandas as pd


class ImplementationStatus(str, Enum):
    """Implementation status: separates timing from legality."""
    ACTIVE = "Active"
    INACTIVE = "Inactive"
    ANNOUNCED = "Announced"


class LegalForm(str, Enum):
    """Legal form: Binding (hard law) or Recommendation (soft law)."""
    BINDING = "Binding"
    RECOMMENDATION = "Recommendation"


@dataclass
class LTVRule:
    """
    Structured data model for LTV rules.
    
    Attributes:
        country_iso2: ISO2 country code (e.g., "HU", "IE")
        implementation_status: Active, Inactive, or Announced
        legal_form: Binding (hard law) or Recommendation (soft law)
        limit_standard: Standard LTV limit (0-100, e.g., 80.0) or list of limits (e.g., [80.0, 90.0])
        limit_ftb: Preferential limit for First-Time Buyers (nullable, 0-100)
        limit_btl: Stricter limit for Buy-to-Let/Investors (nullable, 0-100)
        exception_quota: Speed limit - percentage of volume allowed to exceed (e.g., "15% of volume")
        notes: Specific conditions (e.g., "Limit applies to secondary homes"). If limit_standard is a list, notes should explain what each value means.
    """
    country_iso2: str
    implementation_status: ImplementationStatus
    legal_form: LegalForm
    limit_standard: Optional[Union[float, List[float]]] = None  # 0-100, or list of limits
    limit_ftb: Optional[float] = None  # 0-100, nullable
    limit_btl: Optional[float] = None  # 0-100, nullable
    exception_quota: Optional[str] = None  # e.g., "15% of volume"
    notes: Optional[str] = None  # Additional notes/clarifications (should explain list meanings if limit_standard is a list)
    
    def to_dict(self) -> dict:
        """Convert to dictionary for DataFrame."""
        # Convert list to string representation for DataFrame storage
        limit_standard_val = self.limit_standard
        if isinstance(limit_standard_val, list):
            limit_standard_val = ", ".join([f"{x:.1f}%" for x in limit_standard_val])
        
        return {
            "Country": self.country_iso2,
            "Implementation_Status": self.implementation_status.value,
            "Legal_Form": self.legal_form.value,
            "Limit_Standard": limit_standard_val,
            "Limit_FTB": self.limit_ftb,
            "Limit_BTL": self.limit_btl,
            "Exception_Quota": self.exception_quota,
            "Notes": self.notes,
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> "LTVRule":
        """Create from dictionary."""
        # Handle limit_standard: can be float, list, or string representation of list
        limit_standard_val = data.get("Limit_Standard")
        if pd.notna(limit_standard_val) and limit_standard_val is not None:
            if isinstance(limit_standard_val, list):
                limit_standard = limit_standard_val
            elif isinstance(limit_standard_val, str) and "," in limit_standard_val:
                # Parse string like "80.0%, 90.0%" to list
                try:
                    limit_standard = [float(x.strip().replace("%", "")) for x in limit_standard_val.split(",")]
                except:
                    limit_standard = None
            else:
                try:
                    limit_standard = float(limit_standard_val)
                except:
                    limit_standard = None
        else:
            limit_standard = None
        
        return cls(
            country_iso2=str(data.get("Country", "")).strip(),
            implementation_status=ImplementationStatus(data.get("Implementation_Status", "Active")),
            legal_form=LegalForm(data.get("Legal_Form", "Binding")),
            limit_standard=limit_standard,
            limit_ftb=float(data["Limit_FTB"]) if pd.notna(data.get("Limit_FTB")) and data.get("Limit_FTB") is not None else None,
            limit_btl=float(data["Limit_BTL"]) if pd.notna(data.get("Limit_BTL")) and data.get("Limit_BTL") is not None else None,
            exception_quota=str(data.get("Exception_Quota", "")).strip() if data.get("Exception_Quota") else None,
            notes=str(data.get("Notes", "")).strip() if data.get("Notes") else None,
        )


def create_ltv_schema() -> pd.DataFrame:
    """
    Create empty DataFrame with proper schema for LTV rules.
    
    Returns:
        Empty DataFrame with correct column types and names.
    """
    return pd.DataFrame({
        "Country": pd.Series(dtype="string"),
        "Implementation_Status": pd.Series(dtype="string"),  # "Active", "Inactive", "Announced"
        "Legal_Form": pd.Series(dtype="string"),  # "Binding" or "Recommendation"
        "Limit_Standard": pd.Series(dtype="object"),  # Can be float, list, or string representation
        "Limit_FTB": pd.Series(dtype="float64"),  # Nullable, 0-100
        "Limit_BTL": pd.Series(dtype="float64"),  # Nullable, 0-100
        "Exception_Quota": pd.Series(dtype="string"),  # e.g., "15% of volume"
        "Notes": pd.Series(dtype="string"),  # Additional notes/clarifications
    })


def rules_to_dataframe(rules: list[LTVRule]) -> pd.DataFrame:
    """
    Convert list of LTVRule objects to DataFrame.
    
    Args:
        rules: List of LTVRule objects
        
    Returns:
        DataFrame with LTV rules
    """
    if not rules:
        return create_ltv_schema()
    
    data = [rule.to_dict() for rule in rules]
    df = pd.DataFrame(data)
    
    # Ensure proper types (Limit_Standard can be string if it's a list representation)
    # Don't convert Limit_Standard to numeric if it's a string representation of a list
    if "Limit_Standard" in df.columns:
        # Keep as object type to allow strings (list representations)
        pass
    df["Limit_FTB"] = pd.to_numeric(df["Limit_FTB"], errors="coerce")
    df["Limit_BTL"] = pd.to_numeric(df["Limit_BTL"], errors="coerce")
    
    return df


def dataframe_to_rules(df: pd.DataFrame) -> list[LTVRule]:
    """
    Convert DataFrame to list of LTVRule objects.
    
    Args:
        df: DataFrame with LTV rules
        
    Returns:
        List of LTVRule objects
    """
    rules = []
    for _, row in df.iterrows():
        try:
            rule = LTVRule.from_dict(row.to_dict())
            rules.append(rule)
        except Exception as e:
            # Skip invalid rows
            continue
    return rules
