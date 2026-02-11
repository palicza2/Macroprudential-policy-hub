"""
LTV Extraction Logic.
Extracts structured LTV rules from ESRB descriptions using regex and AI.
"""

import logging
import re
from typing import Dict, List, Optional, Any, Union

import pandas as pd

from .ltv_model import (
    LTVRule,
    ImplementationStatus,
    LegalForm,
)

logger = logging.getLogger(__name__)


def extract_implementation_status(description: str, status: str) -> ImplementationStatus:
    """
    Extract implementation status from description and ESRB status.
    
    Args:
        description: Measure description
        status: ESRB status field
        
    Returns:
        ImplementationStatus enum
    """
    status_lower = str(status or "").lower()
    desc_lower = str(description or "").lower()
    
    if "withdrawn" in status_lower or "revoked" in status_lower or "deactivated" in status_lower or "inactive" in status_lower:
        return ImplementationStatus.INACTIVE
    
    if "announced" in desc_lower or "will be" in desc_lower or "planned" in desc_lower:
        return ImplementationStatus.ANNOUNCED
    
    return ImplementationStatus.ACTIVE


def extract_legal_form(description: str) -> LegalForm:
    """
    Extract legal form from description.
    
    Args:
        description: Measure description
        
    Returns:
        LegalForm enum
    """
    d = str(description or "").lower()
    
    # Soft law indicators
    if any(k in d for k in ["guideline", "best practice", "guidelines", "recommendation", "recommended"]):
        return LegalForm.RECOMMENDATION
    
    # Hard law indicators
    if any(k in d for k in ["shall", "must", "cannot exceed", "shall not", "loan shall not be issued", "shall be assessed"]):
        return LegalForm.BINDING
    
    # Heuristic: explicit "limits" with allowance shares usually indicates binding
    if "limit" in d and any(k in d for k in ["lending can take place above", "above the limits", "share of new loans", "can be granted", "cannot exceed"]):
        return LegalForm.BINDING
    
    # Default to Binding if unclear (conservative)
    return LegalForm.BINDING


def extract_ltv_limits_regex(description: str) -> tuple:
    """
    Extract LTV limits using regex patterns.
    
    Args:
        description: Measure description
        
    Returns:
        Tuple of (limit_standard, limit_ftb, limit_btl) 
        - limit_standard can be float, list of floats, or None
        - limit_ftb and limit_btl can be float or None
    """
    text = str(description or "").lower()
    
    # Find all percentage values
    percentages = re.findall(r"(\d+(?:\.\d+)?)\s*%", text)
    percentages = [float(p) for p in percentages if 0 <= float(p) <= 100]
    
    if not percentages:
        return None, None, None
    
    # Try to identify standard, FTB, and BTL limits
    limit_standard = None
    limit_ftb = None
    limit_btl = None
    
    # Look for explicit mentions
    if "first-time buyer" in text or "ftb" in text or "first time buyer" in text:
        ftb_percentages = re.findall(r"(?:first[- ]time buyer|ftb).*?(\d+(?:\.\d+)?)\s*%", text, re.IGNORECASE)
        if ftb_percentages:
            limit_ftb = float(ftb_percentages[0])
    
    if "buy-to-let" in text or "btl" in text or "investment" in text or "investor" in text:
        btl_percentages = re.findall(r"(?:buy[- ]to[- ]let|btl|investment|investor).*?(\d+(?:\.\d+)?)\s*%", text, re.IGNORECASE)
        if btl_percentages:
            limit_btl = float(btl_percentages[0])
    
    # Standard limit: if multiple percentages found and no explicit FTB/BTL, store as list
    # Otherwise use single value
    if percentages:
        # Remove FTB and BTL percentages from consideration for standard
        standard_candidates = [p for p in percentages if p != limit_ftb and p != limit_btl]
        
        if len(standard_candidates) > 1:
            # Multiple standard limits - return as list
            limit_standard = sorted(standard_candidates)
        elif len(standard_candidates) == 1:
            # Single standard limit
            limit_standard = standard_candidates[0]
        else:
            # All percentages were FTB/BTL, use the highest as standard
            limit_standard = max(percentages) if percentages else None
    
    return limit_standard, limit_ftb, limit_btl


def extract_exception_quota_regex(description: str) -> Optional[str]:
    """
    Extract exception quota (speed limit) using regex.
    
    Args:
        description: Measure description
        
    Returns:
        Exception quota string (e.g., "15% of volume") or None
    """
    text = str(description or "")
    
    # Look for patterns like "15% of volume", "20% of new loans", etc.
    patterns = [
        r"(\d+(?:\.\d+)?)\s*%\s*(?:of|of the|of new|of aggregate)?\s*(?:volume|loans|lending|new loans|aggregate volume)",
        r"(?:up to|maximum|max)\s*(\d+(?:\.\d+)?)\s*%\s*(?:of|of the|of new|of aggregate)?\s*(?:volume|loans|lending|new loans)",
        r"(\d+(?:\.\d+)?)\s*%\s*(?:tolerance|flexibility|exemption|exception)",
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            percentage = match.group(1)
            # Try to extract the full context
            context = match.group(0)
            return context.strip()
    
    return None


def extract_ltv_rule_from_item(
    item: Dict[str, Any],
    analyzer: Optional[Any] = None
) -> Optional[LTVRule]:
    """
    Extract LTV rule from ESRB item using regex and AI fallback.
    
    Args:
        item: Dictionary with ESRB data (country, description, status, etc.)
        analyzer: Optional LLMAnalyzer instance for AI extraction
        
    Returns:
        LTVRule object or None if extraction fails
    """
    if not isinstance(item, dict):
        logger.warning(f"extract_ltv_rule_from_item: item is not a dict, got {type(item)}")
        return None
    
    country = str(item.get("iso2", item.get("country", ""))).strip().upper()
    description = str(item.get("description", "")).strip()
    status = str(item.get("active_status", item.get("status", "Active"))).strip()
    
    if not country or not description:
        return None
    
    # Extract using regex first
    limit_standard, limit_ftb, limit_btl = extract_ltv_limits_regex(description)
    exception_quota = extract_exception_quota_regex(description)
    implementation_status = extract_implementation_status(description, status)
    legal_form = extract_legal_form(description)
    
    # If no limits found with regex, try AI extraction
    notes = None
    if limit_standard is None and analyzer:
        try:
            ai_result = analyzer.extract_ltv_rule_ai(description, country)
            if ai_result:
                if limit_standard is None and ai_result.get("limit_standard"):
                    limit_standard = ai_result.get("limit_standard")
                if limit_ftb is None and ai_result.get("limit_ftb"):
                    limit_ftb = ai_result.get("limit_ftb")
                if limit_btl is None and ai_result.get("limit_btl"):
                    limit_btl = ai_result.get("limit_btl")
                if exception_quota is None and ai_result.get("exception_quota"):
                    exception_quota = ai_result.get("exception_quota")
                if ai_result.get("notes"):
                    notes = ai_result.get("notes")
        except Exception as e:
            logger.warning(f"AI extraction failed for {country}: {e}")
    
    # Create rule (even if limit_standard is None, we can still create a rule)
    try:
        rule = LTVRule(
            country_iso2=country,
            implementation_status=implementation_status,
            legal_form=legal_form,
            limit_standard=limit_standard,
            limit_ftb=limit_ftb,
            limit_btl=limit_btl,
            exception_quota=exception_quota,
            notes=notes,
        )
        logger.debug(f"   -> Extracted LTV rule: {country} (limit: {limit_standard})")
        return rule
    except Exception as e:
        logger.warning(f"Failed to create LTVRule for {country}: {e}")
        return None
