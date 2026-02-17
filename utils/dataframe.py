"""
DataFrame utilities.
Data cleaning and manipulation functions.
"""

import re
import pandas as pd


def clean_columns(df):
    """Clean DataFrame column names."""
    df.columns = df.columns.astype(str).str.strip().str.replace('\n', ' ').str.replace('  ', ' ')
    return df


def find_header_row(df, keyword="Country"):
    """Find the header row in a DataFrame."""
    for i in range(min(20, len(df))):
        if any(keyword.lower() in str(val).lower() for val in df.iloc[i].values):
            return i
    return 0


def extract_rate(text):
    """
    Extract rate value from text.
    
    Args:
        text: Text to extract rate from
        
    Returns:
        Extracted rate as float, or 0.0 if not found
    """
    if pd.isna(text):
        return 0.0
    text_str = str(text).lower().replace(',', '.')
    matches = re.findall(r'(\d+(?:\.\d+)?)', text_str)
    valid_rates = []
    for m in matches:
        val = float(m)
        # Skip years (1990-2030) and values > 50 (likely not rates)
        if (val.is_integer() and 1990 <= val <= 2030) or val > 50:
            continue
        valid_rates.append(val)
    return max(valid_rates) if valid_rates else 0.0


def ccyb_change_only_points(ccyb_df: pd.DataFrame) -> pd.DataFrame:
    """
    Reduce CCyB dataframe to change-only points per country: keep the first date
    and then only rows where the rate actually changes. Between these points the
    buffer is assumed to remain unchanged (step behaviour; e.g. COVID releases
    to 0% and subsequent re-activations are preserved).
    Expects columns: country (or iso2), date, rate.
    """
    if ccyb_df is None or ccyb_df.empty or "rate" not in ccyb_df.columns:
        return pd.DataFrame()
    df = ccyb_df.copy()
    date_col = "decision_date" if "decision_date" in df.columns else "date"
    if date_col not in df.columns:
        date_col = "date"
    df["_dt"] = pd.to_datetime(df[date_col], errors="coerce")
    df = df.dropna(subset=["_dt"])
    country_col = "country" if "country" in df.columns else "iso2"
    if country_col not in df.columns:
        return pd.DataFrame()
    df["rate"] = pd.to_numeric(df["rate"], errors="coerce").fillna(0.0)
    keep_cols = [c for c in [country_col, "date", "rate", "credit_gap", "iso2", "iso3"] if c in df.columns]
    out = []
    for _, group in df.groupby(country_col):
        g = group.sort_values("_dt").reset_index(drop=True)
        if g.empty:
            continue
        prev_rate = None
        for _, row in g.iterrows():
            r = float(row["rate"])
            if prev_rate is None or r != prev_rate:
                rec = {c: row[c] for c in keep_cols if c in row.index}
                rec["date"] = row["_dt"]
                rec["rate"] = r
                if country_col not in rec:
                    rec[country_col] = row[country_col]
                out.append(rec)
                prev_rate = r
    if not out:
        return pd.DataFrame()
    result = pd.DataFrame(out)
    return result
