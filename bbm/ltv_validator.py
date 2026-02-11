"""
LTV Validation Logic.
AI-based validation for extracted LTV rules.
"""

import logging
from typing import Dict, List, Optional, Any

from .ltv_model import LTVRule

logger = logging.getLogger(__name__)


def validate_rules_with_ai(
    rules: List[LTVRule],
    items: List[Dict[str, Any]],
    analyzer: Any,
    use_external_search: bool = False,
    search_config: Optional[Dict[str, Any]] = None
) -> List[LTVRule]:
    """
    Validate extracted LTV rules with AI, filling missing data.
    
    Args:
        rules: List of extracted LTVRule objects
        items: Original ESRB items (for context)
        analyzer: LLMAnalyzer instance
        use_external_search: Whether to use external search
        search_config: Optional search configuration
        
    Returns:
        List of validated LTVRule objects (all rules kept, missing data filled)
    """
    if not rules or not items:
        return rules
    
    # Convert rules to validation format
    rules_dict = []
    for rule in rules:
        rules_dict.append({
            "country": rule.country_iso2,
            "limit_standard": rule.limit_standard,
            "limit_ftb": rule.limit_ftb,
            "limit_btl": rule.limit_btl,
            "exception_quota": rule.exception_quota,
            "legal_form": rule.legal_form.value,
            "implementation_status": rule.implementation_status.value,
            "notes": rule.notes,
        })
    
    # Get descriptions for context
    descriptions = [str(item.get("description", "")).strip() for item in items[:len(rules)]]
    
    # Validate with AI
    try:
        validated = analyzer.validate_ltv_rules(rules_dict, descriptions, use_external_search, search_config)
        
        # Update rules with validated data
        for i, rule in enumerate(rules):
            if i < len(validated) and validated[i]:
                val = validated[i]
                
                # Update fields if AI provides them
                if val.get("limit_standard") is not None:
                    rule.limit_standard = val.get("limit_standard")
                if val.get("limit_ftb") is not None:
                    rule.limit_ftb = val.get("limit_ftb")
                if val.get("limit_btl") is not None:
                    rule.limit_btl = val.get("limit_btl")
                if val.get("exception_quota"):
                    rule.exception_quota = val.get("exception_quota")
                if val.get("legal_form"):
                    from .ltv_model import LegalForm
                    try:
                        rule.legal_form = LegalForm(val.get("legal_form"))
                    except:
                        pass
                if val.get("notes"):
                    rule.notes = val.get("notes")
                
                confidence = val.get("confidence", "medium")
                if confidence == "low":
                    logger.warning(f"   -> Low confidence for {rule.country_iso2} LTV rule")
    except Exception as e:
        logger.error(f"Error in validate_rules_with_ai: {e}")
    
    logger.info(f"   -> Updated {len(rules)} LTV rules with AI validation (kept all rules, filled missing data)")
    return rules


def validate_complete_table_with_ai(
    df: Any,  # pd.DataFrame
    analyzer: Any,
    use_external_search: bool = False,
    search_config: Optional[Dict[str, Any]] = None
) -> Any:  # pd.DataFrame
    """
    Final validation pass: validate complete LTV table with AI and optional external search.
    
    Args:
        df: DataFrame with LTV rules
        analyzer: LLMAnalyzer instance
        use_external_search: Whether to use external search
        search_config: Optional search configuration
        
    Returns:
        Validated DataFrame (all rows kept, potentially updated)
    """
    if df is None or df.empty:
        return df
    
    # Convert DataFrame to validation format
    table_rows = df.to_dict("records")
    
    try:
        validated = analyzer.validate_ltv_table(table_rows, use_external_search, search_config)
        
        # Update DataFrame with validated data
        for i, row in enumerate(table_rows):
            if i < len(validated) and validated[i]:
                val = validated[i]
                
                # Update fields if AI provides them
                for key, value in val.items():
                    if key in df.columns and value is not None:
                        df.at[i, key] = value
    except Exception as e:
        logger.error(f"Error in validate_complete_table_with_ai: {e}")
    
    return df
