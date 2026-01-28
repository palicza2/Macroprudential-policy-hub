from __future__ import annotations

import re
from typing import Dict, Tuple

import pandas as pd


RENAME_MAP: Dict[str, str] = {
    "Loan-to-value (LTV)": "LTV",
    "Debt-service-to-income (DSTI)": "DSTI",
    "Loan-to-income (LTI)": "LTI",
    "DTI": "DTI",
    "Loan maturity": "Maturity",
    "Loan amortisation": "Amort.",
    "Flexibility quota": "Flex.",
    "Stress test / sensitivity test": "Stress T.",
}


def extract_ltv_details_regex(text: str) -> Tuple[str, str, str, str]:
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


def build_bbm_matrix_html(bbm_full: pd.DataFrame) -> Tuple[str, str]:
    if bbm_full is None or bbm_full.empty:
        return "", ""

    bbm_ref_date = ""
    max_date = bbm_full["date"].max() if "date" in bbm_full.columns else None
    if pd.notna(max_date):
        bbm_ref_date = max_date.strftime("%Y-%m-%d")

    bbm_matrix = bbm_full.copy()
    bbm_matrix["measure_short"] = bbm_matrix["measure_type"].map(lambda x: RENAME_MAP.get(x, x))

    def status_flag(row):
        status_text = f"{row.get('active_status','')} {row.get('status','')}".lower()
        if "active" in status_text or "applicable" in status_text:
            return "active"
        if any(k in status_text for k in ["announc", "planned", "pending", "future", "not yet"]):
            return "announced"
        return ""

    bbm_matrix["status_flag"] = bbm_matrix.apply(status_flag, axis=1)

    def pick_flag(values):
        vals = [v for v in values if v]
        if "active" in vals:
            return "<span class='dot dot--active'></span>"
        if "announced" in vals:
            return "<span class='dot dot--announced'></span>"
        return ""

    pivot_df = bbm_matrix.pivot_table(
        index="iso2",
        columns="measure_short",
        values="status_flag",
        aggfunc=pick_flag,
    ).fillna("")

    pivot_df.index.name = "COUNTRY"
    pivot_df.columns.name = None

    preferred_order = [
        "LTV",
        "DSTI",
        "DTI",
        "LTI",
        "Maturity",
        "Amort.",
        "Amortization",
        "Stress T.",
        "Stress Test",
        "Flex.",
        "Flexibility",
    ]
    ordered_cols = [c for c in preferred_order if c in pivot_df.columns]
    ordered_cols += [c for c in pivot_df.columns if c not in ordered_cols]
    pivot_df = pivot_df[ordered_cols].sort_index(axis=0)

    return pivot_df.to_html(classes="display-table bbm-pivot", escape=False), bbm_ref_date

