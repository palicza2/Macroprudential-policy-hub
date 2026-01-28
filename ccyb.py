from __future__ import annotations

import logging
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)


def prepare_ccyb_decisions(ccyb_full: Optional[pd.DataFrame], analyzer) -> pd.DataFrame:
    """
    Returns a formatted CCyB decisions dataframe ready for rendering.
    Expects ETL columns: iso2, decision_date, date, rate, justification
    """
    if ccyb_full is None or ccyb_full.empty:
        return pd.DataFrame()

    temp = ccyb_full.sort_values(["decision_date", "date"], ascending=[False, False]).head(10).copy()
    req = ["iso2", "decision_date", "date", "rate", "justification"]
    ccyb_decisions = temp[[c for c in req if c in temp.columns]].copy()

    for col in ["decision_date", "date"]:
        if col in ccyb_decisions.columns:
            ccyb_decisions[col] = pd.to_datetime(ccyb_decisions[col]).dt.strftime("%Y-%m-%d")

    if not ccyb_decisions.empty and "justification" in ccyb_decisions.columns:
        logger.info("   -> CCyB AI keywords generation...")
        raw_justs = ccyb_decisions["justification"].fillna("").astype(str).tolist()
        if any(len(j.strip()) > 5 for j in raw_justs):
            ccyb_decisions["justification"] = analyzer.extract_keywords(raw_justs, "justification")
        else:
            logger.warning("   -> No substantial justification text found to process.")

    ccyb_decisions.columns = [c.upper() for c in ccyb_decisions.columns]
    return ccyb_decisions.rename(
        columns={
            "ISO2": "COUNTRY",
            "DECISION_DATE": "ANNOUNCEMENT",
            "DATE": "IMPLEMENTATION",
            "JUSTIFICATION": "JUSTIFICATION",
        }
    )

