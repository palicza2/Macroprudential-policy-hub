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
