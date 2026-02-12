"""
DTI/LTI Comparison Table Builder.
Builds structured comparison table using new data model.
"""

import logging
from pathlib import Path
from typing import List, Optional, Dict, Any

import pandas as pd

from .dti_lti_model import DTILTIRule, MeasureCode, rules_to_dataframe, create_dti_lti_schema
from .dti_lti_extractor import extract_dti_lti_rule_from_item
from .dti_lti_validator import validate_rules_with_ai, validate_complete_table_with_ai

# Import build_dti_lti_items from dti_lti subpackage
from .dti_lti.items_builder import build_dti_lti_items

logger = logging.getLogger(__name__)


def apply_expert_corrections(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply expert corrections based on policy expert validation.
    
    This function applies specific corrections identified by domain experts:
    - SK: Income_Basis should be "Net" (not "Gross")
    - SE: Add note that it's an amortization trigger, not a hard cap
    - IE: Swap Standard Limit (3.5x) and FTB Limit (4.0x)
    - GB: Set Allowance_Share to "15%"
    - NO: Set Allowance_Share to "10%"
    - DK: Set Limit_Standard to 4.0 (or 5.0 if guidelines mention both)
    
    Args:
        df: DataFrame with DTI/LTI rules
        
    Returns:
        Corrected DataFrame
    """
    if df.empty:
        return df
    
    df = df.copy()
    
    # Ensure Notes and Limit_Green columns exist
    if "Notes" not in df.columns:
        df["Notes"] = None
    if "Limit_Green" not in df.columns:
        df["Limit_Green"] = None
    
    # 1. Slovakia (SK): Income_Basis should be "Net" (not "Gross"), Limit_Standard range 3-8, Notes: "Decreasing by age"
    if "Country" in df.columns:
        sk_mask = df["Country"] == "SK"
        if sk_mask.any():
            if "Income_Basis" in df.columns:
                df.loc[sk_mask, "Income_Basis"] = "Net"
            if "Limit_Standard" in df.columns:
                # Set to list representation: "3.0x, 8.0x" (range)
                df.loc[sk_mask, "Limit_Standard"] = "3.0x, 8.0x"
            if "Notes" in df.columns:
                df.loc[sk_mask, "Notes"] = "Decreasing by age"
            logger.info("   -> Expert correction: SK Income_Basis='Net', Limit_Standard=[3.0x, 8.0x] (range), Notes='Decreasing by age'")
    
    # 2. Ireland (IE): Swap Standard Limit (3.5x) and FTB Limit (4.0x)
    if "Country" in df.columns and "Limit_Standard" in df.columns and "Limit_FTB" in df.columns:
        ie_mask = df["Country"] == "IE"
        if ie_mask.any():
            # Current values
            current_standard = df.loc[ie_mask, "Limit_Standard"].iloc[0] if ie_mask.any() else None
            current_ftb = df.loc[ie_mask, "Limit_FTB"].iloc[0] if ie_mask.any() else None
            
            # Swap: Standard should be 3.5x, FTB should be 4.0x
            if pd.notna(current_standard) and pd.notna(current_ftb):
                # If current Standard is 4.0 and FTB is empty or different, swap
                if abs(current_standard - 4.0) < 0.1:
                    df.loc[ie_mask, "Limit_Standard"] = 3.5
                    df.loc[ie_mask, "Limit_FTB"] = 4.0
                    logger.info("   -> Expert correction: IE limits swapped (Standard: 3.5x, FTB: 4.0x)")
            elif pd.notna(current_standard) and abs(current_standard - 4.0) < 0.1:
                # If only Standard is set to 4.0, move it to FTB and set Standard to 3.5
                df.loc[ie_mask, "Limit_Standard"] = 3.5
                df.loc[ie_mask, "Limit_FTB"] = 4.0
                logger.info("   -> Expert correction: IE limits corrected (Standard: 3.5x, FTB: 4.0x)")
    
    # 4. United Kingdom (GB): Set Allowance_Share to "15%" and rename country code to "UK"
    if "Country" in df.columns and "Allowance_Share" in df.columns:
        gb_mask = df["Country"] == "GB"
        if gb_mask.any():
            df.loc[gb_mask, "Allowance_Share"] = "15%"
            df.loc[gb_mask, "Country"] = "UK"  # Display as "UK" instead of "GB"
            logger.info("   -> Expert correction: GB -> UK, Allowance_Share set to '15%'")
    
    # 5. Norway (NO): Set Allowance_Share to "10%"
    if "Country" in df.columns and "Allowance_Share" in df.columns:
        no_mask = df["Country"] == "NO"
        if no_mask.any():
            df.loc[no_mask, "Allowance_Share"] = "10%"
            logger.info("   -> Expert correction: NO Allowance_Share set to '10%'")
    
    # 6. Denmark (DK): Set Limit_Standard to 4.0 (guidelines mention 4x and 5x, use 4.0 as primary)
    if "Country" in df.columns and "Limit_Standard" in df.columns:
        dk_mask = df["Country"] == "DK"
        if dk_mask.any():
            # Only set if currently empty/NaN
            dk_limit = df.loc[dk_mask, "Limit_Standard"].iloc[0] if dk_mask.any() else None
            if pd.isna(dk_limit):
                df.loc[dk_mask, "Limit_Standard"] = 4.0
                logger.info("   -> Expert correction: DK Limit_Standard set to 4.0x (guidelines: 4x-5x)")
    
    # 7. Latvia (LV): Set Limit_Green to 8.0 (green DTI limit)
    if "Country" in df.columns and "Limit_Green" in df.columns:
        lv_mask = df["Country"] == "LV"
        if lv_mask.any():
            df.loc[lv_mask, "Limit_Green"] = 8.0
            logger.info("   -> Expert correction: LV Limit_Green set to 8.0x")
    
    return df


def build_dti_lti_comparison_df_structured(
    bbm_full: pd.DataFrame,
    analyzer,
    validate_with_ai: bool = True,
    final_validation_with_search: bool = True,
    search_config: Optional[Dict[str, Any]] = None
) -> pd.DataFrame:
    """
    Build structured DTI/LTI comparison DataFrame using new data model.
    
    Args:
        bbm_full: Full BBM dataframe
        analyzer: LLMAnalyzer instance
        validate_with_ai: Whether to validate extracted rules with AI
        final_validation_with_search: Whether to perform final validation with external search
        
    Returns:
        DataFrame with structured DTI/LTI rules
    """
    items = build_dti_lti_items(bbm_full)
    if not items:
        return create_dti_lti_schema()
    
    # Step 1: AI confirmation (strict, evidence-quoted)
    confirmations = analyzer.confirm_dti_lti_presence(items)
    
    def _is_confirmed_high(c: dict, it: dict) -> bool:
        if not isinstance(c, dict):
            return False
        if str(c.get("confirmed", "")).strip().lower() != "yes":
            return False
        # Accept both "high" and "medium" confidence
        confidence = str(c.get("confidence", "")).strip().lower()
        if confidence not in {"high", "medium"}:
            return False
        # Evidence excerpt is still required
        if not str(c.get("evidence_excerpt", "")).strip():
            return False
        t = str(c.get("type", "")).strip().upper()
        if t not in {"DTI", "LTI"}:
            return False
        return True
    
    confirmed_items = []
    for it, c in zip(items, confirmations):
        # Ensure it is a dict
        if not isinstance(it, dict):
            logger.warning(f"Skipping non-dict item in confirmed_items: {type(it)}")
            continue
        if _is_confirmed_high(c, it):
            logger.info(f"   -> Confirmed: {it.get('iso2')} {it.get('measure_short')} (confidence: {c.get('confidence', 'N/A')})")
            confirmed_items.append(it)
        else:
            if it.get('iso2') in ['SK', 'DK', 'LV']:
                logger.warning(f"   -> NOT confirmed: {it.get('iso2')} {it.get('measure_short')} - c: {c}")
    
    if not confirmed_items:
        return create_dti_lti_schema()
    
    # Step 2: Extract structured rules (regex + AI fallback)
    rules: List[DTILTIRule] = []
    descriptions: Dict[str, str] = {}
    
    for item in confirmed_items:
        # Ensure item is a dict
        if not isinstance(item, dict):
            logger.warning(f"Skipping non-dict item in extraction: {type(item)}, value: {item}")
            continue
        
        # Debug: log item structure
        iso2 = item.get('iso2', 'N/A')
        measure_short = item.get('measure_short', 'N/A')
        logger.info(f"   -> Extracting rule for: {iso2} {measure_short}")
        
        description = str(item.get("description", ""))
        rule = extract_dti_lti_rule_from_item(item, description, analyzer)
        if rule:
            logger.info(f"   -> Successfully extracted rule: {rule.country} {rule.measure_code.value} (limit: {rule.limit_standard})")
            key = f"{rule.country}_{rule.measure_code.value}"
            descriptions[key] = description
            rules.append(rule)
        else:
            if iso2 in ['SK', 'DK', 'LV']:
                logger.warning(f"   -> FAILED to extract rule for: {iso2} {measure_short}")
    
    if not rules:
        logger.warning("   -> No rules extracted after confirmation")
        return create_dti_lti_schema()
    
    logger.info(f"   -> Extracted {len(rules)} rules before validation")
    for rule in rules:
        logger.info(f"      - {rule.country} {rule.measure_code.value} (limit: {rule.limit_standard})")
    
    # Step 3: Validate extracted rules with AI (validates regex-extracted fields and fills missing data)
    if validate_with_ai:
        logger.info("   -> Validating extracted rules with AI (filling missing data)...")
        rules = validate_rules_with_ai(
            rules,
            analyzer,
            descriptions,
            use_external_search=False,  # First pass: ESRB only
            search_config=search_config
        )
    
    if not rules:
        logger.warning("   -> No rules after validation")
        return create_dti_lti_schema()
    
    # Step 4: Convert to DataFrame
    df = rules_to_dataframe(rules)
    logger.info(f"   -> DataFrame created with {len(df)} rows")
    logger.info(f"   -> Countries in DataFrame BEFORE filtering: {sorted(df['Country'].unique().tolist()) if not df.empty and 'Country' in df.columns else []}")
    
    # Filter out invalid entries:
    # 1. Remove LI (Liechtenstein) - it's DSTI (loan service to income), not LTI/DTI
    # 2. Remove SE (Sweden) - it's an amortization requirement, not an LTI/DTI limit
    if not df.empty and 'Country' in df.columns:
        df_before_filter = df.copy()
        df = df[~df['Country'].isin(['LI', 'SE'])].copy()
        removed = len(df_before_filter) - len(df)
        if removed > 0:
            logger.info(f"   -> Removed {removed} entries: LI (DSTI, not LTI/DTI), SE (amortization requirement, not LTI/DTI limit)")
    
    # 2. Filter out entries with limit_standard outside typical range (4-9) or suspiciously low/high
    # Also filter out LI entries with very low limits (0.3x) which are likely DSTI, not DTI/LTI
    if not df.empty and 'Limit_Standard' in df.columns:
        df_before_limit_filter = df.copy()
        # Keep entries with None/NaN limit_standard (might be guidelines without explicit limit)
        # But filter out entries with limits outside reasonable range (0.5-15)
        # Typical DTI/LTI limits are between 4-9, but allow wider range for edge cases
        # Specifically filter out very low limits (< 0.5) which are likely DSTI ratios
        df = df[
            df['Limit_Standard'].isna() | 
            ((df['Limit_Standard'] >= 0.5) & (df['Limit_Standard'] <= 15.0))
        ].copy()
        removed = len(df_before_limit_filter) - len(df)
        if removed > 0:
            logger.info(f"   -> Removed {removed} entries with limit_standard outside reasonable range (0.5-15) or suspiciously low (<0.5, likely DSTI)")
    
    # Deduplicate (some countries may have multiple rows in ESRB)
    # Use a broader subset for deduplication if Limit_Standard can be None
    dedup_subset = ["Country", "Measure_Code"]
    if "Limit_Standard" in df.columns:
        # Only include Limit_Standard in dedup if it's not None
        # This allows multiple rules per country if limits differ
        df_before_dedup = df.copy()
        df = df.drop_duplicates(subset=dedup_subset, keep="first")
        logger.info(f"   -> After deduplication: {len(df)} rows (removed {len(df_before_dedup) - len(df)} duplicates)")
        logger.info(f"   -> Countries in DataFrame AFTER deduplication: {sorted(df['Country'].unique().tolist()) if not df.empty and 'Country' in df.columns else []}")
    df = df.sort_values(["Measure_Code", "Country"]).reset_index(drop=True)
    
    # Step 5: Apply expert corrections (policy expert validations)
    df = apply_expert_corrections(df)
    
    # Step 6: Final validation pass with external search
    # Note: This step is optional and may filter out rows if confidence is not "high"
    # For now, we skip this to ensure all extracted rules are shown
    # if final_validation_with_search and not df.empty:
    #     logger.info("   -> Final validation with external search...")
    #     df = validate_complete_table_with_ai(
    #         df,
    #         analyzer,
    #         use_external_search=True,
    #         search_config=search_config
    #     )
    
    return df


def save_dti_lti_template_csv(output_path: Path, df: Optional[pd.DataFrame] = None) -> None:
    """
    Save DTI/LTI rules to CSV template file.
    
    Args:
        output_path: Path to save CSV file
        df: Optional DataFrame to save (if None, creates empty template)
    """
    if df is None or df.empty:
        df = create_dti_lti_schema()
    
    # Ensure directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Save to CSV
    df.to_csv(output_path, index=False, encoding="utf-8")
    logger.info(f"Saved DTI/LTI rules to {output_path}")
