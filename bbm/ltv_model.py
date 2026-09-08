"""
LTV data model.

Limits are grouped by borrower/use, not a dubious "standard / FTB / BTL" split:

- FTB / OOO: first-time buyer or owner-occupied
- SSB / BTL: second/subsequent buyer or buy-to-let
- Other: green, secondary home, FX, and similar differentiations
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional

import pandas as pd


class ImplementationStatus(str, Enum):
    ACTIVE = "Active"
    INACTIVE = "Inactive"
    ANNOUNCED = "Announced"


class LegalForm(str, Enum):
    BINDING = "Binding"
    RECOMMENDATION = "Recommendation"


LABEL_FTB = "FTB"
LABEL_OOO = "OOO"
LABEL_SSB = "SSB"
LABEL_BTL = "BTL"

PRIMARY_LABELS = {LABEL_FTB, LABEL_OOO}
SECONDARY_LABELS = {LABEL_SSB, LABEL_BTL}

_PCT_RE = re.compile(r"(\d+(?:\.\d+)?)\s*%")
_EM_DASH = "—"


def format_pct(value: float) -> str:
    if value is None or pd.isna(value):
        return ""
    v = float(value)
    if abs(v - round(v)) < 0.05:
        return f"{int(round(v))}%"
    return f"{v:.1f}%"


def format_labeled_limits(items: List["LabeledLimit"]) -> str:
    parts = []
    seen = set()
    for item in items:
        if item is None or item.value is None:
            continue
        key = (round(float(item.value), 1), item.label)
        if key in seen:
            continue
        seen.add(key)
        label = (item.label or "").strip()
        cell = f"{format_pct(item.value)} ({label})" if label else format_pct(item.value)
        parts.append(cell)
    return "; ".join(parts)


def parse_labeled_limits(text: object) -> List["LabeledLimit"]:
    """Parse '90% (FTB); 80% (OOO)' or a bare '90%' / 90.0."""
    if text is None or (isinstance(text, float) and pd.isna(text)):
        return []
    if isinstance(text, (int, float)):
        return [LabeledLimit(float(text), "")]
    raw = str(text).strip()
    if not raw or raw in {_EM_DASH, "-", "None", "nan"}:
        return []
    out: List[LabeledLimit] = []
    for chunk in re.split(r"[;|]", raw):
        chunk = chunk.strip()
        if not chunk:
            continue
        m = re.search(r"(\d+(?:\.\d+)?)\s*%", chunk)
        if not m:
            try:
                out.append(LabeledLimit(float(chunk), ""))
            except ValueError:
                continue
            continue
        value = float(m.group(1))
        label_m = re.search(r"\(([^)]+)\)", chunk)
        label = label_m.group(1).strip() if label_m else ""
        out.append(LabeledLimit(value, label))
    return out


def parse_limit_number(text: object) -> Optional[float]:
    """Single numeric LTV from gold/display cells ('90.0%', 90, '—')."""
    if text is None or (isinstance(text, float) and pd.isna(text)):
        return None
    if isinstance(text, (int, float)):
        v = float(text)
        return v if 0 <= v <= 100 else None
    raw = str(text).strip()
    if not raw or raw in {_EM_DASH, "-", "None", "nan"}:
        return None
    m = _PCT_RE.search(raw)
    if m:
        v = float(m.group(1))
        return v if 0 <= v <= 100 else None
    try:
        v = float(raw)
        return v if 0 <= v <= 100 else None
    except ValueError:
        return None


def _is_plausible_ltv(value: float) -> bool:
    return 40.0 <= float(value) <= 100.0


def migrate_legacy_ltv_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Map old Standard / FTB / BTL columns onto FTB/OOO, SSB/BTL, Other.

    Comma-separated "standard" dumps are not copied — they were not real LTV caps.
    """
    if df is None or df.empty:
        return create_ltv_schema()

    out = df.copy()
    rename = {
        "Status": "Implementation_Status",
        "Legal Form": "Legal_Form",
        "Standard Limit": "Limit_Standard",
        "FTB Limit": "Limit_FTB",
        "BTL Limit": "Limit_BTL",
        "Exception Quota": "Exception_Quota",
        "FTB / OOO": "Limit_FTB_OOO",
        "SSB / BTL": "Limit_SSB_BTL",
        "Other limits": "Other_Limits",
        "Other Limits": "Other_Limits",
    }
    out = out.rename(columns={k: v for k, v in rename.items() if k in out.columns})

    if "Limit_FTB_OOO" not in out.columns:
        out["Limit_FTB_OOO"] = ""
    if "Limit_SSB_BTL" not in out.columns:
        out["Limit_SSB_BTL"] = ""
    if "Other_Limits" not in out.columns:
        out["Other_Limits"] = ""

    has_legacy = any(c in out.columns for c in ("Limit_Standard", "Limit_FTB", "Limit_BTL"))
    if has_legacy:
        for idx, row in out.iterrows():
            existing_primary = str(row.get("Limit_FTB_OOO") or "").strip()
            existing_secondary = str(row.get("Limit_SSB_BTL") or "").strip()
            if existing_primary or existing_secondary:
                continue

            primary: List[LabeledLimit] = []
            secondary: List[LabeledLimit] = []

            ftb = parse_limit_number(row.get("Limit_FTB"))
            btl = parse_limit_number(row.get("Limit_BTL"))
            if ftb is not None and _is_plausible_ltv(ftb):
                primary.append(LabeledLimit(ftb, LABEL_FTB))
            if btl is not None and _is_plausible_ltv(btl):
                secondary.append(LabeledLimit(btl, LABEL_BTL))

            used = {round(x.value, 1) for x in primary + secondary}
            leftover: List[float] = []
            std = row.get("Limit_Standard")
            if std is not None and not (isinstance(std, float) and pd.isna(std)):
                raw = str(std).strip()
                if raw and "," not in raw and ";" not in raw:
                    v = parse_limit_number(raw)
                    if v is not None and _is_plausible_ltv(v) and round(v, 1) not in used:
                        leftover.append(v)

            for v in leftover:
                if primary:
                    secondary.append(LabeledLimit(v, LABEL_SSB))
                else:
                    primary.append(LabeledLimit(v, LABEL_OOO))

            if primary:
                out.at[idx, "Limit_FTB_OOO"] = format_labeled_limits(primary)
            if secondary:
                out.at[idx, "Limit_SSB_BTL"] = format_labeled_limits(secondary)

    keep = [
        "Country",
        "Implementation_Status",
        "Legal_Form",
        "Limit_FTB_OOO",
        "Limit_SSB_BTL",
        "Other_Limits",
        "Exception_Quota",
        "Notes",
    ]
    for col in keep:
        if col not in out.columns:
            out[col] = pd.Series(dtype="string")
    return out[keep].copy()


@dataclass
class LabeledLimit:
    value: float
    label: str = ""


def _clean_optional_text(value: object) -> Optional[str]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    text = str(value).strip()
    if not text or text in {_EM_DASH, "-", "None", "nan"}:
        return None
    return text


@dataclass
class LTVRule:
    country_iso2: str
    implementation_status: ImplementationStatus
    legal_form: LegalForm
    limits_ftb_ooo: List[LabeledLimit] = field(default_factory=list)
    limits_ssb_btl: List[LabeledLimit] = field(default_factory=list)
    other_limits: Optional[str] = None
    exception_quota: Optional[str] = None
    notes: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "Country": self.country_iso2,
            "Implementation_Status": self.implementation_status.value,
            "Legal_Form": self.legal_form.value,
            "Limit_FTB_OOO": format_labeled_limits(self.limits_ftb_ooo),
            "Limit_SSB_BTL": format_labeled_limits(self.limits_ssb_btl),
            "Other_Limits": self.other_limits,
            "Exception_Quota": self.exception_quota,
            "Notes": self.notes,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "LTVRule":
        return cls(
            country_iso2=str(data.get("Country", "")).strip(),
            implementation_status=ImplementationStatus(data.get("Implementation_Status", "Active")),
            legal_form=LegalForm(data.get("Legal_Form", "Binding")),
            limits_ftb_ooo=parse_labeled_limits(data.get("Limit_FTB_OOO")),
            limits_ssb_btl=parse_labeled_limits(data.get("Limit_SSB_BTL")),
            other_limits=_clean_optional_text(data.get("Other_Limits")),
            exception_quota=_clean_optional_text(data.get("Exception_Quota")),
            notes=_clean_optional_text(data.get("Notes")),
        )


def create_ltv_schema() -> pd.DataFrame:
    return pd.DataFrame({
        "Country": pd.Series(dtype="string"),
        "Implementation_Status": pd.Series(dtype="string"),
        "Legal_Form": pd.Series(dtype="string"),
        "Limit_FTB_OOO": pd.Series(dtype="string"),
        "Limit_SSB_BTL": pd.Series(dtype="string"),
        "Other_Limits": pd.Series(dtype="string"),
        "Exception_Quota": pd.Series(dtype="string"),
        "Notes": pd.Series(dtype="string"),
    })


def rules_to_dataframe(rules: list[LTVRule]) -> pd.DataFrame:
    if not rules:
        return create_ltv_schema()
    df = pd.DataFrame([rule.to_dict() for rule in rules])
    for col in create_ltv_schema().columns:
        if col not in df.columns:
            df[col] = pd.Series(dtype="string")
    return df[list(create_ltv_schema().columns)]


def dataframe_to_rules(df: pd.DataFrame) -> list[LTVRule]:
    if df is None or df.empty:
        return []
    migrated = migrate_legacy_ltv_columns(df)
    rules = []
    for _, row in migrated.iterrows():
        try:
            rules.append(LTVRule.from_dict(row.to_dict()))
        except Exception:
            continue
    return rules
