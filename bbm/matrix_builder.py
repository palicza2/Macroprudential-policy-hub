"""
BBM Matrix Builder.
Builds HTML pivot table for BBM measures matrix.
"""

from typing import Dict, Tuple
import pandas as pd

# Rename map for measure types
RENAME_MAP: Dict[str, str] = {
    "Loan-to-value (LTV)": "LTV",
    "Debt-service-to-income (DSTI)": "DSTI",
    "Loan-to-income (LTI)": "LTI",
    "DTI": "DTI",  # Already short in Excel
    "LTI": "LTI",  # Already short in Excel (if present)
    "Loan maturity": "Maturity",
    "Loan amortisation": "Amort.",
    "Flexibility quota": "Flex.",
    "Stress test / sensitivity test": "Stress T.",
}


def build_bbm_matrix_html(bbm_full: pd.DataFrame) -> Tuple[str, str]:
    """
    Build BBM matrix HTML pivot table.
    
    Args:
        bbm_full: Full BBM dataframe
        
    Returns:
        Tuple of (HTML string, reference date string)
    """
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

    # Exclude Amortization and Stress Test columns
    exclude_cols = ["Amort.", "Amortization", "Stress T.", "Stress Test"]
    pivot_df = pivot_df.drop(columns=[c for c in exclude_cols if c in pivot_df.columns], errors='ignore')
    
    preferred_order = [
        "LTV",
        "DSTI",
        "DTI",
        "LTI",
        "Maturity",
        "Flex.",
        "Flexibility",
    ]
    ordered_cols = [c for c in preferred_order if c in pivot_df.columns]
    ordered_cols += [c for c in pivot_df.columns if c not in ordered_cols]
    pivot_df = pivot_df[ordered_cols].sort_index(axis=0)

    return pivot_df.to_html(classes="display-table bbm-pivot", escape=False), bbm_ref_date
