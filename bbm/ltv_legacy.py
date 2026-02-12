"""
Legacy LTV extraction functions.
Compatibility layer for extract_ltv_details_regex from bbm.py.
"""

import re
from typing import Tuple


def extract_ltv_details_regex(text: str) -> Tuple[str, str, str, str]:
    """
    Legacy LTV extraction function (compatibility layer).
    
    Returns:
        Tuple of (limits_str, ftb_flag, ftb_details, other_details)
    """
    text = str(text or "")
    limits = re.findall(r"(\d+(?:\.\d+)?)\s*%", text)
    limits = sorted({f"{l}%" for l in limits}, key=lambda x: float(x.strip("%")))
    limits_str = ", ".join(limits) if limits else "N/A"

    ftb_markers = ["first-time buyer", "first time buyer", "ftb", "first-time buyers", "first time buyers"]
    ftb_present = any(m in text.lower() for m in ftb_markers)
    ftb_flag = "Yes" if ftb_present else "No"

    sentences = re.split(r"(?<=[.!?])\s+", text)
    ftb_details = [s.strip() for s in sentences if any(m in s.lower() for m in ftb_markers)]
    ftb_details = " ".join(ftb_details) if ftb_details else ""

    exception_markers = [
        "exception", "exempt", "exemption", "quota", "flexibility",
        "waiver", "additional", "higher limit", "region", "renovation",
        "energy", "cap", "ceiling", "special",
    ]
    other_details = [s.strip() for s in sentences if any(m in s.lower() for m in exception_markers)]
    other_details = " ".join(other_details) if other_details else ""

    return limits_str, ftb_flag, ftb_details, other_details
