"""
DTI/LTI Extraction Logic.
Extracts structured DTI/LTI rules from ESRB descriptions using AI.
"""

import logging
import re
from typing import Dict, List, Optional, Any

import pandas as pd

from .dti_lti_model import (
    DTILTIRule,
    MeasureCode,
    ImplementationStatus,
    LegalForm,
    IncomeBasis,
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
    
    if "withdrawn" in status_lower or "revoked" in status_lower or "deactivated" in status_lower:
        return ImplementationStatus.WITHDRAWN
    
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


def extract_income_basis(description: str, measure_type: str) -> IncomeBasis:
    """
    Extract income basis (Gross vs Net) from description.
    
    Args:
        description: Measure description
        measure_type: "DTI" or "LTI"
        
    Returns:
        IncomeBasis enum
    """
    d = str(description or "").lower()
    
    # Explicit mentions
    if "net income" in d or "net disposable income" in d or "after-tax" in d:
        return IncomeBasis.NET
    
    if "gross income" in d or "pre-tax" in d:
        return IncomeBasis.GROSS
    
    # LTI typically uses gross income
    if measure_type == "LTI":
        return IncomeBasis.GROSS
    
    # DTI can be either, default to Gross if unclear
    return IncomeBasis.GROSS


def extract_limit_standard(description: str, measure_type: str) -> Optional[float]:
    """
    Extract standard limit (multiplier) from description.
    
    Args:
        description: Measure description
        measure_type: "DTI" or "LTI"
        
    Returns:
        Float multiplier or None if not found
    """
    s = str(description or "")
    
    # Look for explicit multipliers (e.g., "4.5x", "4.5 times", "4.5")
    patterns = [
        r"(\d+(?:\.\d+)?)\s*x\s*(?:income|debt|loan)",
        r"(\d+(?:\.\d+)?)\s*times\s*(?:income|debt|loan)",
        r"(?:limit|maximum|cap|ceiling)\s*(?:of|is|at|:)\s*(\d+(?:\.\d+)?)\s*x",
        r"(?:limit|maximum|cap|ceiling)\s*(?:of|is|at|:)\s*(\d+(?:\.\d+)?)\s*times",
        r"(\d+(?:\.\d+)?)\s*:\s*1",  # Ratio format
    ]
    
    for pattern in patterns:
        matches = re.findall(pattern, s, re.IGNORECASE)
        if matches:
            try:
                value = float(matches[0])
                # Sanity check: DTI/LTI limits are typically 2-10x
                if 1.0 <= value <= 15.0:
                    return value
            except (ValueError, IndexError):
                continue
    
    # Look for percentage-based limits and convert (e.g., 450% = 4.5x)
    percent_patterns = [
        r"(\d+(?:\.\d+)?)\s*%\s*(?:of|of\s+the)",
        r"(?:limit|maximum|cap|ceiling)\s*(?:of|is|at|:)\s*(\d+(?:\.\d+)?)\s*%",
    ]
    
    for pattern in percent_patterns:
        matches = re.findall(pattern, s, re.IGNORECASE)
        if matches:
            try:
                percent = float(matches[0])
                # Convert percentage to multiplier (450% = 4.5x)
                if 100 <= percent <= 1500:
                    return percent / 100.0
            except (ValueError, IndexError):
                continue
    
    return None


def extract_limit_ftb(description: str) -> Optional[float]:
    """
    Extract First-Time Buyer limit from description.
    
    Args:
        description: Measure description
        
    Returns:
        Float multiplier or None if not found
    """
    s = str(description or "").lower()
    
    # Look for FTB-specific limits
    ftb_markers = ["first-time buyer", "first time buyer", "ftb", "first-time buyers"]
    if not any(m in s for m in ftb_markers):
        return None
    
    # Extract number near FTB mention
    sentences = re.split(r"[.!?]", s)
    for sent in sentences:
        if any(m in sent for m in ftb_markers):
            # Look for multiplier in this sentence
            patterns = [
                r"(\d+(?:\.\d+)?)\s*x",
                r"(\d+(?:\.\d+)?)\s*times",
                r"(\d+(?:\.\d+)?)\s*:\s*1",
            ]
            for pattern in patterns:
                matches = re.findall(pattern, sent, re.IGNORECASE)
                if matches:
                    try:
                        value = float(matches[0])
                        if 1.0 <= value <= 15.0:
                            return value
                    except (ValueError, IndexError):
                        continue
    
    return None


def extract_limit_btl(description: str) -> Optional[float]:
    """
    Extract Buy-to-Let/Investor limit from description.
    
    Args:
        description: Measure description
        
    Returns:
        Float multiplier or None if not found
    """
    s = str(description or "").lower()
    
    # Look for BTL/Investor-specific limits
    btl_markers = ["buy-to-let", "buy to let", "btl", "investment property", "investor", "rental property"]
    if not any(m in s for m in btl_markers):
        return None
    
    # Extract number near BTL mention
    sentences = re.split(r"[.!?]", s)
    for sent in sentences:
        if any(m in sent for m in btl_markers):
            # Look for multiplier in this sentence
            patterns = [
                r"(\d+(?:\.\d+)?)\s*x",
                r"(\d+(?:\.\d+)?)\s*times",
                r"(\d+(?:\.\d+)?)\s*:\s*1",
            ]
            for pattern in patterns:
                matches = re.findall(pattern, sent, re.IGNORECASE)
                if matches:
                    try:
                        value = float(matches[0])
                        if 1.0 <= value <= 15.0:
                            return value
                    except (ValueError, IndexError):
                        continue
    
    return None


def extract_allowance_share(description: str) -> str:
    """
    Extract allowance share (percentage of volume allowed to exceed limit).
    
    Args:
        description: Measure description
        
    Returns:
        String like "15%" or empty string
    """
    s = str(description or "")
    
    # Look for allowance/quota mentions
    patterns = [
        r"(\d+(?:\.\d+)?)\s*%\s*(?:of|of\s+the|of\s+new)\s*(?:loans|volume|lending|mortgages)",
        r"(?:up\s+to|maximum|max)\s*(\d+(?:\.\d+)?)\s*%\s*(?:of|of\s+the|of\s+new)\s*(?:loans|volume|lending|mortgages)",
        r"(?:allowance|quota|flexibility)\s*(?:of|is|at|:)\s*(\d+(?:\.\d+)?)\s*%",
        r"(\d+(?:\.\d+)?)\s*%\s*(?:allowance|quota|flexibility)",
    ]
    
    for pattern in patterns:
        matches = re.findall(pattern, s, re.IGNORECASE)
        if matches:
            try:
                percent = float(matches[0])
                if 0 < percent <= 100:
                    return f"{percent:.0f}%"
            except (ValueError, IndexError):
                continue
    
    return ""


def extract_regulation_url(description: str, iso2: str, analyzer: Any) -> Optional[str]:
    """
    Extract regulation URL from description or use AI to find authority's dedicated page.
    
    Args:
        description: Measure description
        iso2: Country ISO2 code
        analyzer: LLMAnalyzer instance for AI extraction
        
    Returns:
        URL string or None
    """
    import re
    
    # First, try to find URL in description
    url_patterns = [
        r"https?://[^\s\)]+",
        r"www\.[^\s\)]+",
    ]
    
    for pattern in url_patterns:
        matches = re.findall(pattern, description, re.IGNORECASE)
        if matches:
            url = matches[0]
            if not url.startswith("http"):
                url = "https://" + url
            return url
    
    # If no URL found, return None (could be enhanced with AI search later)
    return None


def extract_dti_lti_rule_from_item(
    item: Dict,
    description: str,
    analyzer: Any
) -> Optional[DTILTIRule]:
    """
    Extract structured DTI/LTI rule from ESRB item using AI and regex.
    
    Args:
        item: ESRB item dictionary
        description: Measure description
        analyzer: LLMAnalyzer instance for AI extraction
        
    Returns:
        DTILTIRule object or None if extraction fails
    """
    try:
        # Ensure item is a dict
        if not isinstance(item, dict):
            logger.warning(f"Item is not a dict: {type(item)}, value: {item}")
            return None
        
        # Debug: log item keys
        if not item:
            logger.warning("Item is empty dict")
            return None
        
        iso2 = str(item.get("iso2", "")).strip()
        country_name = str(item.get("country", "")).strip()
        measure_short = str(item.get("measure_short", "")).strip().upper()
        status = str(item.get("status", "")).strip()
        
        if not iso2 or measure_short not in {"DTI", "LTI"}:
            return None
        
        # Determine measure code
        measure_code = MeasureCode.DTI if measure_short == "DTI" else MeasureCode.LTI
        
        # Extract basic fields
        implementation_status = extract_implementation_status(description, status)
        legal_form = extract_legal_form(description)
        income_basis = extract_income_basis(description, measure_short)
        
        # Extract limits
        limit_standard = extract_limit_standard(description, measure_short)
        if limit_standard is None:
            # Try AI extraction as fallback
            try:
                # extract_dti_lti_fields expects a list of dicts with 'description' key
                ai_extracted = analyzer.extract_dti_lti_fields([{"description": description}])
                if ai_extracted and len(ai_extracted) > 0:
                    ai_data = ai_extracted[0]
                    if isinstance(ai_data, dict) and "limit_standard" in ai_data:
                        try:
                            limit_standard = float(ai_data["limit_standard"])
                        except (ValueError, TypeError):
                            pass
            except Exception as e:
                logger.debug(f"AI extraction failed: {e}")
                pass
        
        # If still no limit found, try to infer from description or set a placeholder
        if limit_standard is None:
            # Look for any numeric value that could be a limit (more lenient)
            import re
            # Try to find any reasonable numeric value (2-15 range)
            numeric_patterns = [
                r"(\d+(?:\.\d+)?)\s*(?:times|x|:)\s*(?:income|debt|loan)",
                r"(?:limit|maximum|cap|ceiling|ratio)\s*(?:of|is|at|:)\s*(\d+(?:\.\d+)?)",
                r"(\d+(?:\.\d+)?)\s*%\s*(?:of|of\s+the)",
            ]
            for pattern in numeric_patterns:
                matches = re.findall(pattern, description, re.IGNORECASE)
                if matches:
                    try:
                        val = float(matches[0])
                        if 1.0 <= val <= 15.0:
                            limit_standard = val
                            break
                        # If percentage, convert
                        if 100 <= val <= 1500:
                            limit_standard = val / 100.0
                            break
                    except (ValueError, IndexError):
                        continue
        
        # If still no limit, we can still create the rule but with None limit
        # This allows the rule to appear in the table even if exact limit is unknown
        
        limit_ftb = extract_limit_ftb(description)
        limit_btl = extract_limit_btl(description)
        allowance_share = extract_allowance_share(description)
        
        # Extract regulation URL (if mentioned in description or use AI to find it)
        regulation_url = extract_regulation_url(description, iso2, analyzer)
        
        # Create rule
        rule = DTILTIRule(
            country=iso2,
            measure_code=measure_code,
            implementation_status=implementation_status,
            legal_form=legal_form,
            limit_standard=limit_standard,
            limit_ftb=limit_ftb,
            limit_btl=limit_btl,
            limit_green=None,  # Green limit will be filled by expert corrections
            income_basis=income_basis,
            allowance_share=allowance_share,
            regulation_url=regulation_url,
            notes=None,  # Notes will be filled by expert corrections or AI validation
        )
        
        return rule
        
    except Exception as e:
        import traceback
        logger.warning(f"Error extracting DTI/LTI rule: {e}")
        logger.debug(f"Traceback: {traceback.format_exc()}")
        logger.debug(f"Item type: {type(item)}, Item value: {item}")
        logger.debug(f"Description type: {type(description)}, Description value: {description[:100] if description else 'None'}")
        return None
