from __future__ import annotations

import logging
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)


def prepare_ccyb_decisions(ccyb_full: Optional[pd.DataFrame], analyzer) -> pd.DataFrame:
    """
    Returns a formatted CCyB decisions dataframe ready for rendering.
    Expects ETL columns: iso2, decision_date, date, rate, justification
    
    Adds:
    - Decision type (increase / decrease / release / maintain)
    - Previous rate
    - Rate change
    """
    if ccyb_full is None or ccyb_full.empty:
        return pd.DataFrame()

    # Sort by decision date and get latest 10 decisions
    temp = ccyb_full.sort_values(["decision_date", "date"], ascending=[False, False]).head(10).copy()
    req = ["iso2", "decision_date", "date", "rate", "justification"]
    ccyb_decisions = temp[[c for c in req if c in temp.columns]].copy()

    # Calculate previous rate and decision type for each decision
    if not ccyb_decisions.empty:
        ccyb_decisions['previous_rate'] = None
        ccyb_decisions['decision_type'] = None
        ccyb_decisions['rate_change'] = None
        
        for idx, row in ccyb_decisions.iterrows():
            # Try both iso2 and country columns
            country_iso2 = row.get('iso2')
            country_name = row.get('country') if 'country' in row else None
            current_date = pd.to_datetime(row.get('date'), errors='coerce')
            current_rate = float(row.get('rate', 0)) if pd.notna(row.get('rate')) else 0.0
            
            if (country_iso2 or country_name) and pd.notna(current_date):
                # Find previous decision for the same country before this date
                if country_iso2 and 'iso2' in ccyb_full.columns:
                    country_filter = ccyb_full['iso2'] == country_iso2
                elif country_name and 'country' in ccyb_full.columns:
                    country_filter = ccyb_full['country'] == country_name
                else:
                    continue
                
                country_data = ccyb_full[
                    country_filter &
                    (pd.to_datetime(ccyb_full['date'], errors='coerce') < current_date)
                ].sort_values('date', ascending=False)
                
                if not country_data.empty:
                    previous_rate = float(country_data.iloc[0].get('rate', 0)) if pd.notna(country_data.iloc[0].get('rate')) else 0.0
                    ccyb_decisions.at[idx, 'previous_rate'] = previous_rate
                    
                    # Calculate rate change
                    rate_change = current_rate - previous_rate
                    ccyb_decisions.at[idx, 'rate_change'] = rate_change
                    
                    # Determine decision type
                    if rate_change > 0:
                        if current_rate > 0 and previous_rate == 0:
                            decision_type = "Activation"
                        else:
                            decision_type = "Increase"
                    elif rate_change < 0:
                        if current_rate == 0 and previous_rate > 0:
                            decision_type = "Release"
                        else:
                            decision_type = "Decrease"
                    else:
                        decision_type = "Maintain"
                    
                    ccyb_decisions.at[idx, 'decision_type'] = decision_type
                else:
                    # First decision for this country or no previous data
                    if current_rate > 0:
                        ccyb_decisions.at[idx, 'decision_type'] = "Activation"
                        ccyb_decisions.at[idx, 'previous_rate'] = 0.0
                        ccyb_decisions.at[idx, 'rate_change'] = current_rate
                    else:
                        ccyb_decisions.at[idx, 'decision_type'] = "Maintain"
                        ccyb_decisions.at[idx, 'previous_rate'] = 0.0
                        ccyb_decisions.at[idx, 'rate_change'] = 0.0

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

    # Format rate columns
    if 'previous_rate' in ccyb_decisions.columns:
        ccyb_decisions['previous_rate'] = ccyb_decisions['previous_rate'].apply(
            lambda x: f"{x:.2f}%" if pd.notna(x) and x is not None else "N/A"
        )
    
    if 'rate_change' in ccyb_decisions.columns:
        ccyb_decisions['rate_change'] = ccyb_decisions['rate_change'].apply(
            lambda x: f"{x:+.2f}%" if pd.notna(x) and x is not None else "N/A"
        )
    
    if 'rate' in ccyb_decisions.columns:
        ccyb_decisions['rate'] = ccyb_decisions['rate'].apply(
            lambda x: f"{x:.2f}%" if pd.notna(x) and x is not None else "N/A"
        )

    ccyb_decisions.columns = [c.upper() for c in ccyb_decisions.columns]
    return ccyb_decisions.rename(
        columns={
            "ISO2": "COUNTRY",
            "DECISION_DATE": "ANNOUNCEMENT",
            "DATE": "IMPLEMENTATION",
            "RATE": "CURRENT_RATE",
            "PREVIOUS_RATE": "PREVIOUS_RATE",
            "DECISION_TYPE": "DECISION",
            "RATE_CHANGE": "CHANGE",
            "JUSTIFICATION": "JUSTIFICATION",
        }
    )

