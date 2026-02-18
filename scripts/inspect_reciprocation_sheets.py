"""
Inspect reciprocation-related sheets in the ESRB macroprudential measures Excel file.
Run after the pipeline has downloaded the file (or place the xlsx in data/).
Usage: python scripts/inspect_reciprocation_sheets.py
"""
import sys
from pathlib import Path

import pandas as pd

# Project root
ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
FILE = DATA_DIR / "esrb.measures_overview_macroprudential_measures.xlsx"


def safe_str(s):
    """Avoid UnicodeEncodeError when printing to console (e.g. Windows cp1250)."""
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return ""
    return str(s).encode("ascii", errors="replace").decode("ascii")


def main():
    if not FILE.exists():
        print(f"File not found: {FILE}")
        print("Run the pipeline once to download it, or place the xlsx in data/.")
        sys.exit(1)

    xl = pd.ExcelFile(FILE)
    print("All sheet names:", xl.sheet_names)
    print()

    # Sheets related to reciprocation
    keywords = ["reciproc", "recognition", "matrix"]
    for sheet_name in xl.sheet_names:
        if any(k in sheet_name.lower() for k in keywords):
            print("=" * 60)
            print("Sheet:", sheet_name)
            print("=" * 60)
            df_raw = xl.parse(sheet_name, header=None)
            print("Shape:", df_raw.shape)
            print("First 25 rows (raw, non-ASCII replaced):")
            pd.set_option("display.max_columns", 30)
            pd.set_option("display.width", 200)
            pd.set_option("display.max_colwidth", 40)
            # Avoid console encoding errors: convert to safe strings for display
            df_display = df_raw.head(25).map(
                lambda x: safe_str(x) if pd.notna(x) and x != "" else x
            )
            print(df_display.to_string())
            print()
            # Try with first row as header
            df_header = xl.parse(sheet_name, header=0, nrows=5)
            print("With row 0 as header, columns:", list(df_header.columns))
            print()


if __name__ == "__main__":
    main()
