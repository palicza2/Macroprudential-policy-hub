"""
DTI/LTI Rule Validator.
Validates extracted rules using AI and optional external search.
"""

import logging
from typing import List, Dict, Optional, Any
import pandas as pd

from .dti_lti_model import DTILTIRule, rules_to_dataframe

logger = logging.getLogger(__name__)


def _google_search_for_validation(query: str, search_config: Dict[str, Any]) -> List[Dict[str, str]]:
    """
    Perform Google Search for validation (reuses grounding_validator logic).
    
    Args:
        query: Search query
        search_config: Search configuration
        
    Returns:
        List of search results
    """
    try:
        from grounding_validator import _google_search
        return _google_search(query, search_config)
    except ImportError:
        logger.warning("Could not import Google Search from grounding_validator")
        return []


def validate_rules_with_ai(
    rules: List[DTILTIRule],
    analyzer: Any,
    descriptions: Dict[str, str],
    use_external_search: bool = False,
    search_config: Optional[Dict[str, Any]] = None
) -> List[DTILTIRule]:
    """
    Validate extracted DTI/LTI rules using AI.
    
    Args:
        rules: List of extracted DTILTIRule objects
        analyzer: LLMAnalyzer instance
        descriptions: Dictionary mapping country+measure to ESRB description
        use_external_search: Whether to use external search for validation
        
    Returns:
        List of validated rules (only high-confidence ones)
    """
    if not rules:
        return []
    
    # Convert rules to validation input format
    validation_inputs = []
    for rule in rules:
        key = f"{rule.country}_{rule.measure_code.value}"
        desc = descriptions.get(key, "")
        
        validation_inputs.append({
            "country": rule.country,
            "measure_code": rule.measure_code.value,
            "implementation_status": rule.implementation_status.value,
            "legal_form": rule.legal_form.value,
            "limit_standard": rule.limit_standard,
            "limit_ftb": rule.limit_ftb,
            "limit_btl": rule.limit_btl,
            "income_basis": rule.income_basis.value,
            "allowance_share": rule.allowance_share,
            "description": desc,
        })
    
    # AI validation
    validated_rules = analyzer.validate_dti_lti_rules(
        validation_inputs,
        use_external_search=use_external_search,
        search_config=search_config
    )
    
    # Update rules with validated/corrected values (don't filter out, just improve data)
    # This allows all rules to appear, but with improved/filled data
    from .dti_lti_model import IncomeBasis, LegalForm
    
    updated_rules = []
    for rule, validation in zip(rules, validated_rules):
        if not isinstance(validation, dict):
            # Keep rule even if validation failed
            updated_rules.append(rule)
            continue
        
        confidence = str(validation.get("confidence", "")).strip().lower()
        if confidence not in {"high", "medium"}:
            logger.debug(f"Low confidence for {rule.country} {rule.measure_code.value}: {confidence}, but keeping rule")
        
        # Update rule with validated/corrected values if provided
        # Fill missing limit_standard if AI found it
        if "limit_standard" in validation:
            limit_val = validation["limit_standard"]
            # Handle None, empty string, "None", "null", etc.
            if limit_val is not None and limit_val != "" and str(limit_val).strip().lower() not in ["none", "null", "nan", ""]:
                try:
                    rule.limit_standard = float(limit_val)
                    logger.info(f"   -> Filled limit_standard for {rule.country} {rule.measure_code.value}: {rule.limit_standard}")
                except (ValueError, TypeError) as e:
                    logger.debug(f"Could not convert limit_standard for {rule.country}: {limit_val} ({e})")
            elif rule.limit_standard is None:
                logger.debug(f"   -> limit_standard still missing for {rule.country} {rule.measure_code.value} (AI did not find it)")
        
        if "income_basis" in validation and validation["income_basis"]:
            try:
                rule.income_basis = IncomeBasis(validation["income_basis"])
            except (ValueError, KeyError):
                pass
        
        if "legal_form" in validation and validation["legal_form"]:
            try:
                rule.legal_form = LegalForm(validation["legal_form"])
            except (ValueError, KeyError):
                pass
        
        if "allowance_share" in validation and validation["allowance_share"] is not None:
            rule.allowance_share = str(validation["allowance_share"])
        
        if "regulation_url" in validation and validation["regulation_url"] is not None:
            rule.regulation_url = str(validation["regulation_url"])
        
        # Also try to fill limit_ftb and limit_btl if provided
        if "limit_ftb" in validation and validation["limit_ftb"] is not None:
            try:
                rule.limit_ftb = float(validation["limit_ftb"])
            except (ValueError, TypeError):
                pass
        
        if "limit_btl" in validation and validation["limit_btl"] is not None:
            try:
                rule.limit_btl = float(validation["limit_btl"])
            except (ValueError, TypeError):
                pass
        
        updated_rules.append(rule)
    
    logger.info(f"   -> Updated {len(updated_rules)} rules with AI validation (kept all rules, filled missing data)")
    return updated_rules


def validate_complete_table_with_ai(
    df: pd.DataFrame,
    analyzer: Any,
    use_external_search: bool = True,
    search_config: Optional[Dict[str, Any]] = None
) -> pd.DataFrame:
    """
    Final validation pass: validate complete DTI/LTI table with AI and optional external search.
    
    Args:
        df: Complete DataFrame with DTI/LTI rules
        analyzer: LLMAnalyzer instance
        use_external_search: Whether to use external search for validation
        
    Returns:
        Validated DataFrame (only high-confidence rows)
    """
    if df.empty:
        return df
    
    # Convert DataFrame to validation format
    validation_inputs = df.to_dict('records')
    
    # AI validation with external search
    validated_results = analyzer.validate_dti_lti_table(
        validation_inputs,
        use_external_search=use_external_search,
        search_config=search_config
    )
    
    # Filter to high-confidence rows
    validated_rows = []
    for row, validation in zip(validation_inputs, validated_results):
        if not isinstance(validation, dict):
            continue
        
        confidence = str(validation.get("confidence", "")).strip().lower()
        if confidence != "high":
            continue
        
        # Keep original row (or update with corrections if provided)
        validated_rows.append(row)
    
    if not validated_rows:
        return pd.DataFrame(columns=df.columns)
    
    validated_df = pd.DataFrame(validated_rows)
    return validated_df
