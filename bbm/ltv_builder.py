"""
LTV Comparison Table Builder.
Builds structured comparison table using new data model.
"""

import logging
from pathlib import Path
from typing import List, Optional, Dict, Any

import pandas as pd

from .ltv_model import LTVRule, rules_to_dataframe, create_ltv_schema
from .ltv_extractor import extract_ltv_rule_from_item
from .ltv_validator import validate_rules_with_ai, validate_complete_table_with_ai

logger = logging.getLogger(__name__)


def build_ltv_items(bbm_full: pd.DataFrame) -> list[dict]:
    """
    Build LTV candidate items from ESRB BBM data (EU + EEA + UK, Active only).
    
    Args:
        bbm_full: Full BBM dataframe
        
    Returns:
        List of candidate items (dictionaries)
    """
    if bbm_full is None or bbm_full.empty:
        return []
    
    df = bbm_full.copy()
    if "active_status" in df.columns:
        df = df[df["active_status"].astype(str) == "Active"].copy()
    
    # Filter for LTV measures
    df = df[df["measure_type"].astype(str).str.contains("LTV", case=False, na=False)].copy()
    
    if df.empty:
        return []
    
    # Convert to list of dictionaries
    items = []
    for _, row in df.iterrows():
        item = {
            "iso2": str(row.get("iso2", "")).strip().upper(),
            "country": str(row.get("country", "")).strip(),
            "description": str(row.get("description", "")).strip(),
            "active_status": str(row.get("active_status", "Active")).strip(),
            "status": str(row.get("status", "")).strip(),
            "date": row.get("date"),
        }
        if item["iso2"] and item["description"]:
            items.append(item)
    
    return items


def build_ltv_comparison_df_structured(
    bbm_full: pd.DataFrame,
    analyzer,
    validate_with_ai: bool = True,
    final_validation_with_search: bool = True,
    search_config: Optional[Dict[str, Any]] = None
) -> pd.DataFrame:
    """
    Build structured LTV comparison DataFrame using new data model.
    
    Args:
        bbm_full: Full BBM dataframe
        analyzer: LLMAnalyzer instance
        validate_with_ai: Whether to validate extracted rules with AI
        final_validation_with_search: Whether to perform final validation with external search
        search_config: Optional search configuration
        
    Returns:
        DataFrame with structured LTV rules
    """
    logger.info("   -> Building LTV comparison table (structured)...")
    
    # Get LTV candidate items
    items = build_ltv_items(bbm_full)
    
    if not items:
        logger.info("   -> No LTV items found")
        return create_ltv_schema()
    
    logger.info(f"   -> Found {len(items)} LTV candidate items")
    
    # Extract rules
    rules: List[LTVRule] = []
    for item in items:
        rule = extract_ltv_rule_from_item(item, analyzer)
        if rule:
            rules.append(rule)
    
    logger.info(f"   -> Extracted {len(rules)} LTV rules before validation")
    
    if not rules:
        return create_ltv_schema()
    
    # Validate with AI (fill missing data)
    if validate_with_ai and analyzer:
        rules = validate_rules_with_ai(rules, items, analyzer, False, search_config)
    
    # Convert to DataFrame
    df = rules_to_dataframe(rules)
    
    if df.empty:
        return create_ltv_schema()
    
    # Deduplicate by country (keep first occurrence)
    if not df.empty and 'Country' in df.columns:
        initial_len = len(df)
        df = df.drop_duplicates(subset=['Country'], keep='first').copy()
        removed = initial_len - len(df)
        if removed > 0:
            logger.info(f"   -> After deduplication: {len(df)} rows (removed {removed} duplicates)")
    
    # Final validation with external search (optional)
    if final_validation_with_search and analyzer and search_config:
        logger.info("   -> Final validation with external search...")
        df = validate_complete_table_with_ai(df, analyzer, True, search_config)
    
    logger.info(f"   -> LTV DataFrame shape: {df.shape}")
    logger.info(f"   -> LTV Countries: {sorted(df['Country'].unique().tolist()) if not df.empty and 'Country' in df.columns else []}")
    
    return df
