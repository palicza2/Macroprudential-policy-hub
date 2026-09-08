"""
LTV validation: fill missing FTB/OOO, SSB/BTL, and other-limit fields.
"""

import logging
from typing import Any, Dict, List, Optional

from .ltv_extractor import parse_ai_limit_field
from .ltv_model import LABEL_BTL, LABEL_OOO, LTVRule

logger = logging.getLogger(__name__)


def validate_rules_with_ai(
    rules: List[LTVRule],
    items: List[Dict[str, Any]],
    analyzer: Any,
    use_external_search: bool = False,
    search_config: Optional[Dict[str, Any]] = None,
) -> List[LTVRule]:
    if not rules or not items:
        return rules

    rules_dict = []
    for rule in rules:
        rules_dict.append({
            "country": rule.country_iso2,
            "limit_ftb_ooo": rule.to_dict()["Limit_FTB_OOO"],
            "limit_ssb_btl": rule.to_dict()["Limit_SSB_BTL"],
            "other_limits": rule.other_limits,
            "exception_quota": rule.exception_quota,
            "legal_form": rule.legal_form.value,
            "implementation_status": rule.implementation_status.value,
            "notes": rule.notes,
        })

    descriptions = [str(item.get("description", "")).strip() for item in items[:len(rules)]]

    try:
        validated = analyzer.validate_ltv_rules(rules_dict, descriptions, use_external_search, search_config)
        for i, rule in enumerate(rules):
            if i >= len(validated) or not validated[i]:
                continue
            val = validated[i]
            if val.get("limit_ftb_ooo"):
                filled = parse_ai_limit_field(val.get("limit_ftb_ooo"), LABEL_OOO)
                if filled:
                    rule.limits_ftb_ooo = filled
            if val.get("limit_ssb_btl"):
                filled = parse_ai_limit_field(val.get("limit_ssb_btl"), LABEL_BTL)
                if filled:
                    rule.limits_ssb_btl = filled
            if val.get("other_limits"):
                other = val.get("other_limits")
                rule.other_limits = other if isinstance(other, str) else str(other)
            if val.get("exception_quota"):
                rule.exception_quota = val.get("exception_quota")
            if val.get("legal_form"):
                from .ltv_model import LegalForm
                try:
                    rule.legal_form = LegalForm(val.get("legal_form"))
                except ValueError:
                    pass
            if val.get("notes"):
                rule.notes = val.get("notes")
            if val.get("confidence") == "low":
                logger.warning("   -> Low confidence for %s LTV rule", rule.country_iso2)
    except Exception as exc:
        logger.error("Error in validate_rules_with_ai: %s", exc)

    logger.info("   -> Updated %s LTV rules with AI validation", len(rules))
    return rules


def validate_complete_table_with_ai(
    df: Any,
    analyzer: Any,
    use_external_search: bool = False,
    search_config: Optional[Dict[str, Any]] = None,
) -> Any:
    if df is None or df.empty:
        return df

    table_rows = df.to_dict("records")
    try:
        validated = analyzer.validate_ltv_table(table_rows, use_external_search, search_config)
        for i, row in enumerate(table_rows):
            if i >= len(validated) or not validated[i]:
                continue
            val = validated[i]
            for key, value in val.items():
                if key in df.columns and value is not None:
                    df.at[i, key] = value
    except Exception as exc:
        logger.error("Error in validate_complete_table_with_ai: %s", exc)

    return df
