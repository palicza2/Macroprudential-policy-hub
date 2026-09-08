"""
LTV extraction from ESRB descriptions.

Percentages are classified into FTB/OOO, SSB/BTL, or other (green, secondary
home, …). Portfolio shares and speed-limit quotas are not LTV caps.
"""

from __future__ import annotations

import logging
import re
from typing import Any, Dict, List, Optional, Tuple

from .ltv_model import (
    LABEL_BTL,
    LABEL_FTB,
    LABEL_OOO,
    LABEL_SSB,
    PRIMARY_LABELS,
    SECONDARY_LABELS,
    ImplementationStatus,
    LabeledLimit,
    LegalForm,
    LTVRule,
    format_labeled_limits,
)

logger = logging.getLogger(__name__)

_PCT_RE = re.compile(r"(\d+(?:\.\d+)?)\s*%")

_FTB_RE = re.compile(r"first[- ]time|ftb|first time buyer", re.I)
_OOO_RE = re.compile(
    r"owner[- ]occup|own(?:er)?[- ]use|primary residence|principal residence|"
    r"\booo\b|owner occupied|own occupancy",
    re.I,
)
_SSB_RE = re.compile(
    r"second[- ]time|second or subsequent|subsequent buyer|subsequent home|"
    r"\bssb\b|second[- ]and[- ]subsequent|next[- ]time buyer",
    re.I,
)
_BTL_RE = re.compile(
    r"buy[- ]to[- ]let|\bbtl\b|investment propert|investor|rental|"
    r"not intended for own use|non[- ]owner|buy to let",
    re.I,
)
_GREEN_RE = re.compile(
    r"green|energy[- ]efficient|nzeb|sustainable|renovation|nearly zero",
    re.I,
)
_SECOND_HOME_RE = re.compile(
    r"secondary home|second home|holiday home|vacation home",
    re.I,
)
_FX_RE = re.compile(r"foreign currency|\bfx\b|foreign[- ]currency", re.I)
_COMMERCIAL_RE = re.compile(r"commercial|non[- ]residential", re.I)

_QUOTA_AFTER_RE = re.compile(
    r"^\s*(?:of|of the)\s+(?:volume|lending|loans|new loans|aggregate|"
    r"ftb lending|ssb lending|btl lending)",
    re.I,
)
_QUOTA_WORD_RE = re.compile(r"tolerance|flexibility|exception quota", re.I)


def _is_quota_percentage(text: str, start: int, end: int) -> bool:
    after = text[end:end + 70]
    around = text[max(0, start - 20):end + 30]
    if _QUOTA_AFTER_RE.search(after):
        return True
    if _QUOTA_WORD_RE.search(around) and not re.search(
        r"owner[- ]occup|first[- ]time|buy[- ]to[- ]let|ltv", around, re.I
    ):
        return True
    return False

_LTV_MIN = 40.0


def extract_implementation_status(description: str, status: str) -> ImplementationStatus:
    status_lower = str(status or "").lower()
    desc_lower = str(description or "").lower()

    if any(k in status_lower for k in ("withdrawn", "revoked", "deactivated", "inactive")):
        return ImplementationStatus.INACTIVE
    if "announced" in desc_lower or "will be" in desc_lower or "planned" in desc_lower:
        return ImplementationStatus.ANNOUNCED
    return ImplementationStatus.ACTIVE


def extract_legal_form(description: str) -> LegalForm:
    d = str(description or "").lower()
    if any(k in d for k in ("guideline", "best practice", "guidelines", "recommendation", "recommended")):
        return LegalForm.RECOMMENDATION
    if any(k in d for k in ("shall", "must", "cannot exceed", "shall not", "loan shall not be issued")):
        return LegalForm.BINDING
    if "limit" in d and any(
        k in d for k in ("lending can take place above", "above the limits", "share of new loans", "can be granted")
    ):
        return LegalForm.BINDING
    return LegalForm.BINDING


def extract_exception_quota_regex(description: str) -> Optional[str]:
    text = str(description or "")
    patterns = [
        r"(\d+(?:\.\d+)?)\s*%\s*(?:of|of the|of new|of aggregate)?\s*(?:volume|loans|lending|new loans|aggregate volume|"
        r"ftb lending|ssb lending|btl lending)",
        r"(?:up to|maximum|max)\s*(\d+(?:\.\d+)?)\s*%\s*(?:of|of the|of new)?\s*(?:volume|loans|lending|new loans)",
        r"(\d+(?:\.\d+)?)\s*%\s*(?:tolerance|flexibility|exemption|exception)",
    ]
    hits = []
    for pattern in patterns:
        for match in re.finditer(pattern, text, re.IGNORECASE):
            hits.append(match.group(0).strip())
    if not hits:
        return None
    # Keep unique snippets, original order
    seen = set()
    out = []
    for h in hits:
        key = h.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(h)
    return "; ".join(out)


def _nearest_label(window: str, center: int) -> Optional[str]:
    """Pick the label whose keyword is closest to the % in a local window."""
    candidates: List[Tuple[int, str]] = []

    def collect(regex: re.Pattern, label: str) -> None:
        for m in regex.finditer(window):
            candidates.append((abs(m.start() - center), label))

    collect(_GREEN_RE, "green")
    collect(_SECOND_HOME_RE, "secondary home")
    collect(_FX_RE, "FX")
    collect(_COMMERCIAL_RE, "commercial")
    collect(_FTB_RE, LABEL_FTB)
    collect(_SSB_RE, LABEL_SSB)
    collect(_BTL_RE, LABEL_BTL)
    collect(_OOO_RE, LABEL_OOO)
    if not candidates:
        return None
    candidates.sort(key=lambda x: x[0])
    return candidates[0][1]


def extract_ltv_limits_regex(
    description: str,
) -> Tuple[List[LabeledLimit], List[LabeledLimit], str]:
    """
    Returns (ftb_ooo, ssb_btl, other_limits_text).
    """
    text = str(description or "")
    if not text:
        return [], [], ""

    primary: List[LabeledLimit] = []
    secondary: List[LabeledLimit] = []
    other: List[LabeledLimit] = []

    sentences = re.split(r"(?<=[.!?])\s+", text)
    if not sentences:
        sentences = [text]

    for sent in sentences:
        for match in _PCT_RE.finditer(sent):
            value = float(match.group(1))
            start, end = match.span()
            if _is_quota_percentage(sent, start, end):
                continue
            if value < _LTV_MIN or value > 100:
                continue

            label = _nearest_label(sent, start)
            if label is None:
                label = LABEL_OOO

            item = LabeledLimit(value, label)
            if label in PRIMARY_LABELS:
                primary.append(item)
            elif label in SECONDARY_LABELS:
                secondary.append(item)
            else:
                other.append(item)

    return primary, secondary, format_labeled_limits(other)


def parse_ai_limit_field(payload: Any, default_label: str) -> List[LabeledLimit]:
    from .ltv_model import parse_labeled_limits

    if payload is None or payload == "":
        return []
    if isinstance(payload, list):
        out: List[LabeledLimit] = []
        for entry in payload:
            if isinstance(entry, dict):
                try:
                    val = float(entry.get("value"))
                except (TypeError, ValueError):
                    continue
                lab = str(entry.get("label") or default_label).strip()
                out.append(LabeledLimit(val, lab))
            else:
                for item in parse_labeled_limits(entry):
                    out.append(LabeledLimit(item.value, item.label or default_label))
        return out
    return [
        LabeledLimit(item.value, item.label or default_label)
        for item in parse_labeled_limits(payload)
    ]


def extract_ltv_rule_from_item(
    item: Dict[str, Any],
    analyzer: Optional[Any] = None,
) -> Optional[LTVRule]:
    if not isinstance(item, dict):
        logger.warning("extract_ltv_rule_from_item: item is not a dict, got %s", type(item))
        return None

    country = str(item.get("iso2", item.get("country", ""))).strip().upper()
    description = str(item.get("description", "")).strip()
    status = str(item.get("active_status", item.get("status", "Active"))).strip()

    if not country or not description:
        return None

    primary, secondary, other = extract_ltv_limits_regex(description)
    exception_quota = extract_exception_quota_regex(description)
    implementation_status = extract_implementation_status(description, status)
    legal_form = extract_legal_form(description)
    notes = None

    if not primary and not secondary and not other and analyzer:
        try:
            ai_result = analyzer.extract_ltv_rule_ai(description, country)
            if ai_result:
                primary = parse_ai_limit_field(ai_result.get("limit_ftb_ooo"), LABEL_OOO) or primary
                secondary = parse_ai_limit_field(ai_result.get("limit_ssb_btl"), LABEL_BTL) or secondary
                other = ai_result.get("other_limits") or other
                if isinstance(other, list):
                    other = format_labeled_limits(parse_ai_limit_field(other, "other"))
                if exception_quota is None and ai_result.get("exception_quota"):
                    exception_quota = ai_result.get("exception_quota")
                if ai_result.get("notes"):
                    notes = ai_result.get("notes")
        except Exception as exc:
            logger.warning("AI extraction failed for %s: %s", country, exc)

    if isinstance(other, list):
        other = format_labeled_limits(other)

    try:
        rule = LTVRule(
            country_iso2=country,
            implementation_status=implementation_status,
            legal_form=legal_form,
            limits_ftb_ooo=primary,
            limits_ssb_btl=secondary,
            other_limits=other or None,
            exception_quota=exception_quota,
            notes=notes,
        )
        logger.debug(
            "   -> Extracted LTV rule: %s (FTB/OOO: %s, SSB/BTL: %s)",
            country,
            format_labeled_limits(primary) or "—",
            format_labeled_limits(secondary) or "—",
        )
        return rule
    except Exception as exc:
        logger.warning("Failed to create LTVRule for %s: %s", country, exc)
        return None
