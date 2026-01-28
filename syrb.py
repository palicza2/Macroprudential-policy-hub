from __future__ import annotations

import logging
from typing import Optional, Tuple

import pandas as pd

logger = logging.getLogger(__name__)


def prepare_syrb_tables(syrb_full: Optional[pd.DataFrame], analyzer) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Returns (active_syrb, syrb_decisions) formatted for rendering.
    """
    if syrb_full is None or syrb_full.empty:
        return pd.DataFrame(), pd.DataFrame()

    today = pd.Timestamp.now()
    cols_needed = ["date", "iso2", "syrb_type", "exposure_type", "rate_text", "description"]

    syrb_decisions = syrb_full.sort_values("date", ascending=False).head(10).copy()
    syrb_decisions = syrb_decisions[[c for c in cols_needed if c in syrb_decisions.columns]]
    if "date" in syrb_decisions.columns:
        syrb_decisions["date"] = pd.to_datetime(syrb_decisions["date"]).dt.strftime("%Y-%m-%d")

    status_str = syrb_full["status"].astype(str)
    mask_active = (
        status_str.str.contains("applicable|active", case=False, na=False)
        | (syrb_full["date"] > today)
    ) & (~status_str.str.contains("Deactivated|Revoked|No longer", case=False, na=False))

    df_active = syrb_full[mask_active].sort_values("date", ascending=False)
    active_syrb = df_active.groupby(["iso2", "exposure_type"]).head(1).copy()
    if "rate_numeric" in active_syrb.columns:
        active_syrb = active_syrb[active_syrb["rate_numeric"] > 0].copy()

    active_syrb = active_syrb[[c for c in cols_needed if c in active_syrb.columns]]
    if "date" in active_syrb.columns:
        active_syrb["date"] = pd.to_datetime(active_syrb["date"]).dt.strftime("%Y-%m-%d")

    def enrich(df: pd.DataFrame, label: str) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame()
        logger.info(f"   -> SyRB AI cleaning ({label})...")
        combined_text = "Rate col: " + df["rate_text"].astype(str) + " | Desc: " + df["description"].astype(str)
        df["rate_text"] = analyzer.extract_clean_rates(combined_text.tolist())
        df["description"] = analyzer.extract_keywords(df["description"].astype(str).tolist(), "targeted risk or background")
        df.columns = [c.upper() for c in df.columns]
        return df.rename(
            columns={
                "DATE": "EFFECTIVE FROM",
                "ISO2": "COUNTRY",
                "SYRB_TYPE": "TYPE",
                "RATE_TEXT": "RATE",
                "DESCRIPTION": "DETAILS",
            }
        )

    return enrich(active_syrb, "Active"), enrich(syrb_decisions, "Decisions")

