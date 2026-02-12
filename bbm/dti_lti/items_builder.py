"""
DTI/LTI Items Builder.
Builds DTI/LTI candidate items from ESRB BBM data.
"""

import re
from typing import Dict
import pandas as pd

from ..matrix_builder import RENAME_MAP

# EU + EEA (NO, IS, LI) + UK
EU_ISO2 = {
    "AT", "BE", "BG", "HR", "CY", "CZ", "DK", "EE", "FI", "FR", "DE", "GR", "HU", "IE", "IT", 
    "LV", "LT", "LU", "MT", "NL", "PL", "PT", "RO", "SK", "SI", "ES", "SE",
    "NO", "IS", "LI", "GB"  # EEA countries + UK
}


def build_dti_lti_items(bbm_full: pd.DataFrame) -> list[dict]:
    """
    Build DTI/LTI candidate items from ESRB BBM data (EU + EEA + UK, Active only).
    
    Note: ESRB sometimes marks LTI measures as "DTI" in measure_type.
    We detect LTI from description if measure_type is "DTI" but description mentions LTI.
    """
    if bbm_full is None or bbm_full.empty:
        return []

    df = bbm_full.copy()
    if "active_status" in df.columns:
        df = df[df["active_status"].astype(str) == "Active"].copy()

    df["measure_short"] = df["measure_type"].astype(str).map(lambda x: RENAME_MAP.get(x, x))
    df["iso2"] = df.get("iso2").astype(str)
    df = df[df["iso2"].isin(EU_ISO2)].copy()
    
    # First, get explicit DTI/LTI from measure_short
    df_explicit = df[df["measure_short"].isin(["DTI", "LTI"])].copy()
    
    # Also check for DTI/LTI mentions in description, even if measure_type is not DTI/LTI
    # This handles cases where ESRB marks LTI as "DTI" in measure_type
    # Look for patterns like "8-times income", "DTI ratio", "loan-to-income", etc.
    df_desc = df[~df["measure_short"].isin(["DTI", "LTI"])].copy()
    if not df_desc.empty and "description" in df_desc.columns:
        # Look for DTI/LTI mentions in description (broader pattern)
        # Includes: "DTI", "LTI", "debt-to-income", "loan-to-income", "X-times income", "X times", etc.
        def has_dti_lti_mention(desc: str) -> bool:
            """Check if description mentions DTI/LTI (but NOT DSTI)."""
            if pd.isna(desc) or not desc:
                return False
            desc_str = str(desc).lower()
            
            # EXCLUDE DSTI (Debt-Service-to-Income) - this is different from DTI
            if "dsti" in desc_str or "debt-service-to-income" in desc_str or "debt service to income" in desc_str:
                return False
            
            # Explicit mentions (DTI or LTI, but not DSTI)
            if any(term in desc_str for term in ["dti", "lti", "debt-to-income", "loan-to-income", 
                                                  "debt to income", "loan to income"]):
                # Double-check it's not DSTI
                if "dsti" not in desc_str and "debt-service" not in desc_str:
                    return True
            
            # Pattern: "X times income" or "X-times income" (e.g., "8-times income", "6 times")
            # But exclude if it's about "debt service" or "instalment"
            if re.search(r"\d+(?:\.\d+)?[- ]times?\s+(?:yearly|annual|net|gross|disposable)?\s*income", desc_str, re.I):
                # Exclude if it's about debt service/instalment
                if "debt service" not in desc_str and "instalment" not in desc_str and "payment" not in desc_str:
                    return True
            
            # Pattern: "income" + "times" or "multiple" (e.g., "income multiple", "times income")
            # But exclude DSTI context
            if (re.search(r"income.*(?:times|multiple)", desc_str, re.I) or \
               re.search(r"(?:times|multiple).*income", desc_str, re.I)) and \
               "debt service" not in desc_str:
                return True
            
            # Pattern: "DTI ratio" or "LTI ratio" (explicit)
            if re.search(r"\b(?:dti|lti)\s+ratio\b", desc_str, re.I):
                return True
            
            # Pattern: "debt/loan" + "income" + "ratio" (but not "debt service")
            if re.search(r"(?:debt|loan).*income.*ratio", desc_str, re.I) and \
               "debt service" not in desc_str and "dsti" not in desc_str:
                return True
            
            return False
        
        df_desc["has_dti_lti"] = df_desc["description"].apply(has_dti_lti_mention)
        df_desc = df_desc[df_desc["has_dti_lti"]].copy()
        
        # Try to determine if it's DTI or LTI from description
        def detect_dti_lti_from_desc(desc: str, measure_short: str) -> str:
            desc_lower = str(desc).lower()
            # LTI indicators (loan-to-income, mortgage only, loan amount)
            lti_indicators = [
                "loan-to-income", "loan to income", "lti",
                "mortgage loan", "housing loan", "residential loan",
                "loan amount", "loan size", "loan.*income",
                "mortgage.*income"
            ]
            # DTI indicators (debt-to-income, total debt, all debt)
            dti_indicators = [
                "debt-to-income", "debt to income", "dti",
                "total debt", "all debt", "total indebtedness",
                "debt obligations", "total borrower", "debt.*income",
                "indebtedness.*income", "borrower.*indebtedness"
            ]
            
            # Check for explicit mentions
            has_lti_explicit = any(re.search(ind, desc_lower, re.I) for ind in lti_indicators)
            has_dti_explicit = any(re.search(ind, desc_lower, re.I) for ind in dti_indicators)
            
            # Also check for context clues
            has_mortgage_context = any(word in desc_lower for word in ["mortgage", "housing", "residential", "property"])
            has_total_debt_context = any(phrase in desc_lower for phrase in [
                "total debt", "all debt", "total indebtedness", 
                "including both new and existing loans",
                "debt obligations"
            ])
            
            if has_lti_explicit and not has_dti_explicit:
                return "LTI"
            elif has_dti_explicit and not has_lti_explicit:
                return "DTI"
            elif has_lti_explicit and has_dti_explicit:
                # If both, prefer based on context
                if has_mortgage_context and not has_total_debt_context:
                    return "LTI"
                elif has_total_debt_context and not has_mortgage_context:
                    return "DTI"
                else:
                    # If unclear, prefer DTI (more common)
                    return "DTI"
            elif has_mortgage_context and "income" in desc_lower:
                # Mortgage + income mention → likely LTI
                return "LTI"
            elif has_total_debt_context and "income" in desc_lower:
                # Total debt + income mention → likely DTI
                return "DTI"
            else:
                # Fallback: if measure_short is "DTI", use that; otherwise default to DTI
                if measure_short == "DTI":
                    return "DTI"
                # Default to DTI if unclear
                return "DTI"
        
        df_desc["measure_short"] = df_desc.apply(
            lambda row: detect_dti_lti_from_desc(
                row.get("description", ""),
                row.get("measure_short", "")
            ),
            axis=1
        )
        df_desc = df_desc.drop(columns=["has_dti_lti"], errors="ignore")
    
    # Combine explicit and description-detected
    df_combined = pd.concat([df_explicit, df_desc], ignore_index=True) if not df_desc.empty else df_explicit
    
    if df_combined.empty:
        return []

    items: list[dict] = []
    for _, row in df_combined.iterrows():
        items.append(
            {
                "iso2": str(row.get("iso2", "")).strip(),
                "country": str(row.get("country", "")).strip(),
                "measure_short": str(row.get("measure_short", "")).strip(),
                "description": str(row.get("description", "")).strip(),
                "status": str(row.get("status", "")).strip() if "status" in row else "",
                "related_links": str(row.get("Related links", "")).strip() if "Related links" in df_combined.columns else "",
            }
        )
    return items
