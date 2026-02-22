"""
DTI Expert Table Loader.
Loads the expert-verified DTI table from the BBM táblázatok.xlsx schema.
Schema: Country, Type, Debt Counted, Legal Form, Standard Limit, Preferential Limit,
        Income Basis, Portfolio Limit, Nature of Breach, Exemptions, Regulation Link.
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

# Target English column names (canonical schema)
ENGLISH_COLUMNS = [
    "Country", "Type", "Debt Counted (Numerator)", "Legal Form", "Standard Limit",
    "Preferential Limit (FTB/Green/Age)", "Income Basis", "Portfolio Limit",
    "Nature of Breach (Hard Cap)", "Exemptions", "Regulation Link",
]

# Column mapping: Hungarian header -> English display
EXCEL_COL_MAP = {
    "ország": "Country",
    "típus": "Type",
    "beszámítandó adósság (számláló)": "Debt Counted (Numerator)",
    "jogi forma": "Legal Form",
    "standard limit": "Standard Limit",
    "preferenciális limit (ftb/green/age)": "Preferential Limit (FTB/Green/Age)",
    "jöv. alap": "Income Basis",
    "portfólió limit": "Portfolio Limit",
    "túllépés jellege (hard cap)": "Nature of Breach (Hard Cap)",
    "hatály alóli mentesség (nem kell vizsgálni)": "Exemptions",
    "link a szabályozáshoz (forrás)": "Regulation Link",
}

# Canonical English data (expert-verified, derived from Excel)
DTI_EXPERT_DATA = [
    {
        "Country": "IE",
        "Type": "LTI",
        "Debt Counted (Numerator)": "Only the requested mortgage amount",
        "Legal Form": "Binding",
        "Standard Limit": "3.5x",
        "Preferential Limit (FTB/Green/Age)": "4.0x (FTB)",
        "Income Basis": "Gross",
        "Portfolio Limit": "15% (on total new lending volume)",
        "Nature of Breach (Hard Cap)": "Fully exceedable; no upper cap",
        "Exemptions": "Bridging loans; Equity release",
        "Regulation Link": "Central Bank of Ireland - Mortgage Measures",
    },
    {
        "Country": "NO",
        "Type": "DTI",
        "Debt Counted (Numerator)": "All outstanding debt",
        "Legal Form": "Binding",
        "Standard Limit": "5.0x",
        "Preferential Limit (FTB/Green/Age)": "—",
        "Income Basis": "Gross",
        "Portfolio Limit": "10% (8% in Oslo) quarterly",
        "Nature of Breach (Hard Cap)": "Fully exceedable; no upper cap",
        "Exemptions": "Debt restructuring loans if principal does not increase",
        "Regulation Link": "Utlånsforskriften (Lending Regulation) § 12",
    },
    {
        "Country": "UK",
        "Type": "LTI",
        "Debt Counted (Numerator)": "Only the requested mortgage amount",
        "Legal Form": "Binding",
        "Standard Limit": "4.5x",
        "Preferential Limit (FTB/Green/Age)": "—",
        "Income Basis": "Gross",
        "Portfolio Limit": "15% (on number of loans)",
        "Nature of Breach (Hard Cap)": "Fully exceedable; no upper cap",
        "Exemptions": "Existing loan refinancing if debt does not increase",
        "Regulation Link": "Bank of England FPC / FCA Regulations",
    },
    {
        "Country": "SK",
        "Type": "DTI",
        "Debt Counted (Numerator)": "All outstanding debt",
        "Legal Form": "Binding",
        "Standard Limit": "8.0x",
        "Preferential Limit (FTB/Green/Age)": "9.0x (under 35 / FTB)",
        "Income Basis": "Net",
        "Portfolio Limit": "5% (on portfolio volume)",
        "Nature of Breach (Hard Cap)": "Partially; absolute cap (Max: 9.0x)",
        "Exemptions": "Loans under 2000 EUR; certain state-supported schemes",
        "Regulation Link": "NBS Decree No. 10/2016 (consolidated)",
    },
    {
        "Country": "LV",
        "Type": "DTI",
        "Debt Counted (Numerator)": "All outstanding debt",
        "Legal Form": "Binding",
        "Standard Limit": "6.0x",
        "Preferential Limit (FTB/Green/Age)": "8.0x (Green / Energy-efficient)",
        "Income Basis": "Gross",
        "Portfolio Limit": "10% (on new disbursements)",
        "Nature of Breach (Hard Cap)": "Fully exceedable; no upper cap",
        "Exemptions": "—",
        "Regulation Link": "Latvijas Banka Macroprudential Framework",
    },
]


def load_dti_expert_table(
    excel_path: Optional[Path] = None,
    csv_path: Optional[Path] = None,
) -> pd.DataFrame:
    """
    Load DTI expert table. Tries Excel first, then CSV, then embedded data.
    
    Args:
        excel_path: Path to BBM táblázatok.xlsx (optional)
        csv_path: Path to data/dti_expert_table.csv (optional)
        
    Returns:
        DataFrame with English schema for DTI table display
    """
    # Try project data folder first
    proj_data = Path(__file__).resolve().parent.parent / "data"
    project_excel = proj_data / "BBM táblázatok.xlsx"
    if not excel_path and project_excel.exists():
        excel_path = project_excel

    # Try Excel
    if excel_path and excel_path.exists():
        try:
            df = pd.read_excel(excel_path, sheet_name="DTI", header=0)
            df = _normalize_excel_to_english(df)
            if not df.empty:
                logger.info("Loaded DTI expert table from Excel: %s", excel_path)
                return df
        except Exception as e:
            logger.warning("Could not load DTI from Excel %s: %s", excel_path, e)

    # Try CSV in project data
    csv = csv_path or Path(__file__).resolve().parent.parent / "data" / "dti_expert_table.csv"
    if csv.exists():
        try:
            df = pd.read_csv(csv, encoding="utf-8")
            if not df.empty:
                logger.info("Loaded DTI expert table from CSV: %s", csv)
                return df
        except Exception as e:
            logger.warning("Could not load DTI from CSV %s: %s", csv, e)

    # Fallback to embedded expert data
    df = pd.DataFrame(DTI_EXPERT_DATA)
    logger.info("Using embedded DTI expert table (%d rows)", len(df))
    return df


def _normalize_excel_to_english(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize Excel columns (Hungarian or English) to English display schema."""
    out = pd.DataFrame()
    for col in df.columns:
        col_str = str(col).strip()
        col_lower = col_str.lower()
        matched = False
        # Pass-through: already English
        if col_str in ENGLISH_COLUMNS:
            out[col_str] = df[col].values
            matched = True
        # Map Hungarian -> English
        if not matched:
            for hu_key, en_name in EXCEL_COL_MAP.items():
                if hu_key in col_lower or col_lower in hu_key:
                    out[en_name] = df[col].values
                    matched = True
                    break
    # Strip country flags (e.g. "IE 🇮🇪" -> "IE")
    if "Country" in out.columns:
        out["Country"] = out["Country"].astype(str).str.replace(r"\s*[🇦-🇿\s]+", "", regex=True).str.strip()
        # GB -> UK
        out.loc[out["Country"] == "GB", "Country"] = "UK"
    return out
