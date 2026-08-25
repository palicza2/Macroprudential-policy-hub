"""
BBM gold tables and description-hash state.

Gold CSVs are the dashboard source of truth. The pipeline hashes ESRB
descriptions and only asks a model to review rows whose text changed,
appeared, or disappeared.
"""
from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd

logger = logging.getLogger(__name__)

STATE_VERSION = 1
FAMILY_DTI = "dti"
FAMILY_LTV = "ltv"

KIND_UNCHANGED = "unchanged"
KIND_CHANGED = "changed"
KIND_NEW = "new"
KIND_WITHDRAWN = "withdrawn"

STATUS_OK = "ok"
STATUS_CONFLICT = "conflict"
STATUS_NEW = "new"
STATUS_WITHDRAWN = "withdrawn"
STATUS_BOOTSTRAPPED = "bootstrapped"


def normalize_iso2(code: str) -> str:
    """Canonical ISO2 for gold keys. ESRB uses GB; gold tables use UK."""
    c = str(code or "").strip().upper()
    if c in {"GB", "UK"}:
        return "UK"
    return c


def esrb_iso_aliases(code: str) -> set[str]:
    """ISO2 values that may appear in ESRB parquet for this gold country."""
    n = normalize_iso2(code)
    if n == "UK":
        return {"UK", "GB"}
    return {n} if n else set()


def description_hash(text: str) -> str:
    """Stable short hash of whitespace-normalized description text."""
    norm = " ".join(str(text or "").split())
    return hashlib.sha256(norm.encode("utf-8")).hexdigest()[:16]


def _json_ready(value: Any) -> Any:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def row_to_dict(row: Any) -> Dict[str, Any]:
    """Convert a Series/mapping to a JSON-serializable dict."""
    if hasattr(row, "items") and not isinstance(row, dict):
        data = dict(row)
    else:
        data = dict(row)
    return {str(k): _json_ready(v) for k, v in data.items()}


def gold_fingerprint(row: Dict[str, Any]) -> str:
    """Hash of gold-row contents so a human CSV edit is detectable."""
    payload = json.dumps(row, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def state_key(family: str, country: str, measure: Optional[str] = None) -> str:
    c = normalize_iso2(country)
    if family == FAMILY_LTV or not measure:
        return f"{family}:{c}"
    return f"{family}:{c}:{str(measure).strip().upper()}"


@dataclass
class GoldPaths:
    data_dir: Path

    @property
    def dti_csv(self) -> Path:
        return self.data_dir / "dti_expert_table.csv"

    @property
    def ltv_csv(self) -> Path:
        return self.data_dir / "ltv_gold.csv"

    @property
    def state_json(self) -> Path:
        return self.data_dir / "bbm_gold_state.json"

    @property
    def report_json(self) -> Path:
        return self.data_dir / "bbm_delta_report.json"

    @property
    def proposals_json(self) -> Path:
        return self.data_dir / "bbm_gold_proposals.json"

    @classmethod
    def from_config(cls) -> "GoldPaths":
        from config import DATA_DIR
        return cls(DATA_DIR)


@dataclass
class DeltaItem:
    family: str
    key: str
    country: str
    measure: Optional[str]
    kind: str
    current_hash: str
    description: str
    gold_row: Optional[Dict[str, Any]] = None
    approved_hash: Optional[str] = None
    gold_fingerprint: Optional[str] = None
    reason: str = ""


def load_state(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {"version": STATE_VERSION, "entries": {}}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("Could not read gold state %s: %s", path, exc)
        return {"version": STATE_VERSION, "entries": {}}
    if not isinstance(data, dict):
        return {"version": STATE_VERSION, "entries": {}}
    data.setdefault("version", STATE_VERSION)
    data.setdefault("entries", {})
    if not isinstance(data["entries"], dict):
        data["entries"] = {}
    return data


def save_state(path: Path, state: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    out = {
        "version": STATE_VERSION,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "entries": state.get("entries") or {},
    }
    path.write_text(json.dumps(out, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def save_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, default=str) + "\n", encoding="utf-8")


def load_dti_gold(paths: GoldPaths) -> pd.DataFrame:
    """Load DTI gold CSV only (Excel is not gold)."""
    from .dti_excel_loader import load_dti_expert_table

    return load_dti_expert_table(excel_path=None, csv_path=paths.dti_csv)


def load_ltv_gold(paths: GoldPaths) -> pd.DataFrame:
    if not paths.ltv_csv.exists():
        return pd.DataFrame()
    try:
        df = pd.read_csv(paths.ltv_csv, encoding="utf-8")
    except Exception as exc:
        logger.warning("Could not load LTV gold %s: %s", paths.ltv_csv, exc)
        return pd.DataFrame()
    if "Country" in df.columns:
        df = df.copy()
        df["Country"] = df["Country"].map(normalize_iso2)
    return df


def save_ltv_gold(df: pd.DataFrame, paths: GoldPaths) -> None:
    if df is None or df.empty:
        return
    out = df.copy()
    if "Country" in out.columns:
        out["Country"] = out["Country"].map(normalize_iso2)
    paths.ltv_csv.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(paths.ltv_csv, index=False, encoding="utf-8")
    logger.info("Wrote LTV gold (%d rows) to %s", len(out), paths.ltv_csv)


def dti_gold_index(df: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
    if df is None or df.empty:
        return {}
    idx: Dict[str, Dict[str, Any]] = {}
    for _, row in df.iterrows():
        country = normalize_iso2(row.get("Country", ""))
        measure = str(row.get("Type") or row.get("Measure_Code") or "").strip().upper()
        if not country:
            continue
        key = state_key(FAMILY_DTI, country, measure or None)
        idx[key] = row_to_dict(row)
    return idx


def ltv_gold_index(df: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
    if df is None or df.empty:
        return {}
    idx: Dict[str, Dict[str, Any]] = {}
    for _, row in df.iterrows():
        country = normalize_iso2(row.get("Country", ""))
        if not country:
            continue
        idx[state_key(FAMILY_LTV, country)] = row_to_dict(row)
    return idx


def collapse_items(
    items: Iterable[dict],
    family: str,
) -> Dict[str, Dict[str, str]]:
    """One ESRB item per gold key; longer description wins, else concatenated."""
    collapsed: Dict[str, Dict[str, str]] = {}
    for it in items:
        country = normalize_iso2(it.get("iso2") or it.get("country") or "")
        if not country:
            continue
        measure = None
        if family == FAMILY_DTI:
            measure = str(it.get("measure_short") or it.get("measure") or "").strip().upper() or None
        key = state_key(family, country, measure)
        desc = str(it.get("description") or "").strip()
        existing = collapsed.get(key)
        if existing is None:
            collapsed[key] = {
                "country": country,
                "measure": measure or "",
                "description": desc,
            }
        elif desc and desc not in existing["description"]:
            if len(desc) > len(existing["description"]):
                collapsed[key]["description"] = desc + (
                    "\n\n" + existing["description"] if existing["description"] else ""
                )
            else:
                collapsed[key]["description"] = (
                    existing["description"] + "\n\n" + desc if existing["description"] else desc
                )
    return collapsed


def classify_family(
    family: str,
    items: Iterable[dict],
    gold_index: Dict[str, Dict[str, Any]],
    state: Dict[str, Any],
) -> List[DeltaItem]:
    """
    Compare current ESRB items to gold + approved hashes.

    First sight of a gold row (no state) is trusted: kind=unchanged, hash recorded.
    A human gold-CSV edit (fingerprint change) also accepts the current description.
    """
    entries: Dict[str, Any] = state.get("entries") or {}
    current = collapse_items(items, family)
    results: List[DeltaItem] = []

    for key, payload in current.items():
        country = payload["country"]
        measure = payload["measure"] or None
        desc = payload["description"]
        cur_hash = description_hash(desc)
        gold_row = gold_index.get(key)
        fp = gold_fingerprint(gold_row) if gold_row else None
        entry = entries.get(key) or {}
        approved = entry.get("approved_hash")
        prev_fp = entry.get("gold_fingerprint")

        if gold_row is None:
            results.append(
                DeltaItem(
                    family=family,
                    key=key,
                    country=country,
                    measure=measure,
                    kind=KIND_NEW,
                    current_hash=cur_hash,
                    description=desc,
                    approved_hash=approved,
                    reason="Present in ESRB, not in gold",
                )
            )
            continue

        if not approved or prev_fp != fp:
            reason = (
                "Gold row edited; accepting current ESRB text"
                if approved and prev_fp != fp
                else "First run against existing gold; recording hash"
            )
            results.append(
                DeltaItem(
                    family=family,
                    key=key,
                    country=country,
                    measure=measure,
                    kind=KIND_UNCHANGED,
                    current_hash=cur_hash,
                    description=desc,
                    gold_row=gold_row,
                    approved_hash=cur_hash,
                    gold_fingerprint=fp,
                    reason=reason,
                )
            )
            continue

        if cur_hash == approved:
            results.append(
                DeltaItem(
                    family=family,
                    key=key,
                    country=country,
                    measure=measure,
                    kind=KIND_UNCHANGED,
                    current_hash=cur_hash,
                    description=desc,
                    gold_row=gold_row,
                    approved_hash=approved,
                    gold_fingerprint=fp,
                    reason="ESRB description unchanged",
                )
            )
            continue

        results.append(
            DeltaItem(
                family=family,
                key=key,
                country=country,
                measure=measure,
                kind=KIND_CHANGED,
                current_hash=cur_hash,
                description=desc,
                gold_row=gold_row,
                approved_hash=approved,
                gold_fingerprint=fp,
                reason="ESRB description changed since gold was last accepted",
            )
        )

    for key, gold_row in gold_index.items():
        if key in current:
            continue
        country = normalize_iso2(gold_row.get("Country", ""))
        measure = str(gold_row.get("Type") or gold_row.get("Measure_Code") or "").strip().upper() or None
        results.append(
            DeltaItem(
                family=family,
                key=key,
                country=country,
                measure=measure if family == FAMILY_DTI else None,
                kind=KIND_WITHDRAWN,
                current_hash="",
                description="",
                gold_row=gold_row,
                approved_hash=(entries.get(key) or {}).get("approved_hash"),
                gold_fingerprint=gold_fingerprint(gold_row),
                reason="In gold but not in active ESRB items",
            )
        )

    return results


def apply_state_updates(
    state: Dict[str, Any],
    items: List[DeltaItem],
    verdicts: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """
    Update approved hashes.

    Unchanged / match / bootstrap: store current hash.
    Conflict / unclear / no AI: leave approved_hash so the issue stays hot.
    New: store proposal_hash only, never approved_hash.
    """
    entries = dict(state.get("entries") or {})
    verdicts = verdicts or {}
    now = datetime.now(timezone.utc).isoformat()

    for item in items:
        entry = dict(entries.get(item.key) or {})
        if item.kind == KIND_UNCHANGED:
            entry.update({
                "approved_hash": item.current_hash,
                "gold_fingerprint": item.gold_fingerprint,
                "status": STATUS_OK,
                "updated_at": now,
            })
        elif item.kind == KIND_CHANGED:
            verdict = str((verdicts.get(item.key) or {}).get("verdict") or "").lower()
            if verdict == "match":
                entry.update({
                    "approved_hash": item.current_hash,
                    "gold_fingerprint": item.gold_fingerprint,
                    "status": STATUS_OK,
                    "updated_at": now,
                })
            else:
                entry.update({
                    "gold_fingerprint": item.gold_fingerprint,
                    "status": STATUS_CONFLICT,
                    "last_seen_hash": item.current_hash,
                    "updated_at": now,
                })
                # Keep previous approved_hash
                if item.approved_hash and "approved_hash" not in entry:
                    entry["approved_hash"] = item.approved_hash
        elif item.kind == KIND_NEW:
            entry.update({
                "status": STATUS_NEW,
                "proposal_hash": item.current_hash,
                "updated_at": now,
            })
            entry.pop("approved_hash", None)
        elif item.kind == KIND_WITHDRAWN:
            entry.update({
                "status": STATUS_WITHDRAWN,
                "gold_fingerprint": item.gold_fingerprint,
                "updated_at": now,
            })
        entries[item.key] = entry

    state = dict(state)
    state["entries"] = entries
    return state


def should_skip_new_extract(state: Dict[str, Any], item: DeltaItem) -> bool:
    """True if this new country was already extracted for the same description."""
    entry = (state.get("entries") or {}).get(item.key) or {}
    return (
        item.kind == KIND_NEW
        and entry.get("status") == STATUS_NEW
        and entry.get("proposal_hash") == item.current_hash
    )


def count_kinds(items: Iterable[DeltaItem]) -> Dict[str, int]:
    counts = {
        KIND_UNCHANGED: 0,
        KIND_CHANGED: 0,
        KIND_NEW: 0,
        KIND_WITHDRAWN: 0,
    }
    for item in items:
        if item.kind in counts:
            counts[item.kind] += 1
    return counts


def dti_gold_to_structured(df: pd.DataFrame) -> pd.DataFrame:
    """Map expert gold schema onto the structured DTI/LTI columns (Supabase / AI)."""
    from .dti_lti_model import create_dti_lti_schema

    empty = create_dti_lti_schema()
    if df is None or df.empty:
        return empty
    rows = []
    for _, row in df.iterrows():
        rows.append({
            "Country": normalize_iso2(row.get("Country", "")),
            "Measure_Code": str(row.get("Type") or row.get("Measure_Code") or "").strip().upper() or None,
            "Implementation_Status": "Active",
            "Legal_Form": _json_ready(row.get("Legal Form") or row.get("Legal_Form")),
            "Limit_Standard": _json_ready(row.get("Standard Limit") or row.get("Limit_Standard")),
            "Limit_FTB": None,
            "Limit_BTL": None,
            "Limit_Green": None,
            "Income_Basis": _json_ready(row.get("Income Basis") or row.get("Income_Basis")),
            "Allowance_Share": _json_ready(row.get("Portfolio Limit") or row.get("Allowance_Share")),
            "Regulation_URL": _json_ready(row.get("Regulation Link") or row.get("Regulation_URL")),
            "Notes": _json_ready(row.get("Preferential Limit (FTB/Green/Age)")),
        })
    out = pd.DataFrame(rows)
    for col in empty.columns:
        if col not in out.columns:
            out[col] = None
    return out[list(empty.columns)]


def build_gold_list_html(dti_gold: pd.DataFrame, items: List[DeltaItem]) -> str:
    """Country list from gold, with a short ESRB quote when available. No LLM."""
    if dti_gold is None or dti_gold.empty:
        return "<p class='no-data'>No DTI/LTI gold table.</p>"
    desc_by_key = {i.key: i.description for i in items if i.family == FAMILY_DTI and i.description}
    lines = []
    for _, row in dti_gold.iterrows():
        country = normalize_iso2(row.get("Country", ""))
        measure = str(row.get("Type") or "").strip().upper()
        limit = row.get("Standard Limit") or "—"
        key = state_key(FAMILY_DTI, country, measure)
        desc = desc_by_key.get(key, "")
        excerpt = " ".join(desc.split())[:220]
        extra = (
            f"<br><span style='color:#64748b;font-size:0.85em'>\"{excerpt}…\"</span>"
            if excerpt
            else ""
        )
        lines.append(
            f"<li><strong>{country}</strong> — <strong>{measure}</strong> "
            f"({limit}){extra}</li>"
        )
    if not lines:
        return "<p class='no-data'>No DTI/LTI gold rows.</p>"
    return "<ul style='margin:0; padding-left: 18px;'>" + "".join(lines) + "</ul>"


def summarize_report(
    dti_items: List[DeltaItem],
    ltv_items: List[DeltaItem],
    verdicts: Dict[str, Dict[str, Any]],
    proposals: List[Dict[str, Any]],
    notes: Optional[List[str]] = None,
) -> Dict[str, Any]:
    all_items = dti_items + ltv_items
    review = []
    for item in all_items:
        if item.kind in {KIND_UNCHANGED}:
            continue
        payload = {
            "key": item.key,
            "family": item.family,
            "country": item.country,
            "measure": item.measure,
            "kind": item.kind,
            "reason": item.reason,
            "current_hash": item.current_hash,
            "approved_hash": item.approved_hash,
        }
        if item.kind == KIND_CHANGED:
            payload["verdict"] = verdicts.get(item.key)
            payload["gold_row"] = item.gold_row
            payload["description_excerpt"] = " ".join(item.description.split())[:500]
        if item.kind == KIND_NEW:
            payload["description_excerpt"] = " ".join(item.description.split())[:500]
        if item.kind == KIND_WITHDRAWN:
            payload["gold_row"] = item.gold_row
        review.append(payload)

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "dti": count_kinds(dti_items),
        "ltv": count_kinds(ltv_items),
        "review": review,
        "proposals": proposals,
        "notes": notes or [],
    }
