"""
AI inconsistency check against BBM gold rows.

Gold is trusted unless the new ESRB description clearly contradicts a field.
Richer gold fields that ESRB omits are not conflicts.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Sequence

from langchain_core.messages import HumanMessage
from langchain_core.output_parsers import StrOutputParser

from utils.json_parser import safe_json_loads_list

from .gold import DeltaItem, KIND_CHANGED

logger = logging.getLogger(__name__)

_VERDICTS = {"match", "conflict", "unclear"}


def check_gold_deltas(
    items: Sequence[DeltaItem],
    analyzer,
    chunk_size: int = 8,
) -> Dict[str, Dict[str, Any]]:
    """
    Ask the analyzer to compare gold rows with new ESRB text.

    Returns a map of state_key -> verdict dict. Missing/failed items get
    verdict "unclear" so approved hashes are not advanced.
    """
    changed = [i for i in items if i.kind == KIND_CHANGED]
    if not changed:
        return {}
    if analyzer is None or not hasattr(analyzer, "_get_llm"):
        logger.info("   -> Delta AI skipped (no analyzer); flagging as unclear")
        return {i.key: _unclear(i, "no analyzer") for i in changed}

    out: Dict[str, Dict[str, Any]] = {}
    for start in range(0, len(changed), chunk_size):
        chunk = list(changed[start:start + chunk_size])
        try:
            parsed = _invoke_chunk(chunk, analyzer)
        except Exception as exc:
            logger.warning("Delta check failed: %s", exc)
            for item in chunk:
                out[item.key] = _unclear(item, str(exc))
            continue
        by_index = parsed if isinstance(parsed, list) else []
        for idx, item in enumerate(chunk):
            raw = by_index[idx] if idx < len(by_index) and isinstance(by_index[idx], dict) else {}
            out[item.key] = _normalize_verdict(item, raw)
    return out


def _unclear(item: DeltaItem, reason: str) -> Dict[str, Any]:
    return {
        "key": item.key,
        "country": item.country,
        "verdict": "unclear",
        "conflicting_fields": [],
        "evidence_excerpt": "",
        "proposed_patch": {},
        "reason": reason,
    }


def _normalize_verdict(item: DeltaItem, raw: Dict[str, Any]) -> Dict[str, Any]:
    verdict = str(raw.get("verdict") or "").strip().lower()
    if verdict not in _VERDICTS:
        verdict = "unclear"
    fields = raw.get("conflicting_fields") or []
    if not isinstance(fields, list):
        fields = [str(fields)]
    patch = raw.get("proposed_patch") if isinstance(raw.get("proposed_patch"), dict) else {}
    return {
        "key": item.key,
        "country": item.country,
        "measure": item.measure,
        "verdict": verdict,
        "conflicting_fields": [str(f) for f in fields],
        "evidence_excerpt": str(raw.get("evidence_excerpt") or "").strip(),
        "proposed_patch": patch,
        "reason": str(raw.get("reason") or "").strip(),
    }


def _invoke_chunk(chunk: List[DeltaItem], analyzer) -> List[Any]:
    blocks = []
    for i, item in enumerate(chunk, start=1):
        gold = item.gold_row or {}
        gold_lines = "\n".join(f"  {k}: {v}" for k, v in gold.items() if v not in (None, "", "—"))
        desc = item.description[:2500]
        blocks.append(
            f"ITEM {i}\nKEY: {item.key}\nCOUNTRY: {item.country}\n"
            f"MEASURE: {item.measure or 'LTV'}\n"
            f"GOLD ROW:\n{gold_lines}\n"
            f"ESRB DESCRIPTION:\n{desc}"
        )
    prompt = f"""TASK: Compare each curated GOLD ROW with the new ESRB DESCRIPTION.
RETURN: JSON array, one object per item, same order.

Each object:
- verdict: "match" | "conflict" | "unclear"
- conflicting_fields: array of gold field names that ESRB contradicts (empty if match)
- evidence_excerpt: exact quote from the ESRB description (empty if none)
- proposed_patch: object of gold field -> new value, only for fields that must change
- reason: one short sentence

RULES:
- GOLD is the source of truth for detail ESRB omits (exemptions, legal basis, notes).
- Absence from ESRB is NOT a conflict.
- conflict ONLY if ESRB clearly states a different limit, legal form, income basis, status, or allowance than gold.
- Do not invent numbers that are not in the description.
- If the description is too vague to confirm or deny gold, use unclear.

INPUT:
{chr(10).join(blocks)}
"""
    llm = analyzer._get_llm(temperature=0.0)
    res = (llm | StrOutputParser()).invoke([HumanMessage(content=prompt)])
    parsed = safe_json_loads_list(res, default=[{} for _ in chunk])
    if len(parsed) < len(chunk):
        parsed.extend([{}] * (len(chunk) - len(parsed)))
    return parsed[: len(chunk)]
