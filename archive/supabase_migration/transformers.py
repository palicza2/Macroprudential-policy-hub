"""
Data Transformers Module.

Transforms Parquet/CSV data to Supabase-compatible format.
"""

import pandas as pd
import numpy as np
from typing import Dict, Any, Optional, List
import logging
import re
import country_converter as coco

logger = logging.getLogger(__name__)


def normalize_iso2(value: Any) -> Optional[str]:
    """
    Convert country name or code to ISO2 format.
    
    Handles:
    - Already ISO2 codes (e.g., "AT", "GB")
    - Country names (e.g., "Austria", "United Kingdom")
    - Special cases: "UK" -> "GB"
    - Authority names (e.g., "Norwegian Ministry of Finance" -> "NO")
    
    Args:
        value: Country name or ISO2 code
        
    Returns:
        ISO2 code or None if conversion fails
    """
    if pd.isna(value) or value is None:
        return None
    
    value_str = str(value).strip()
    
    # Special case: UK -> GB
    if value_str.upper() in ["UK", "UNITED KINGDOM"]:
        return "GB"
    
    # If already a 2-letter uppercase code, return it
    if len(value_str) == 2 and value_str.isalpha() and value_str.isupper():
        return value_str
    
    # If it's a 2-letter lowercase code, uppercase it
    if len(value_str) == 2 and value_str.isalpha():
        return value_str.upper()
    
    # Handle authority names - extract country name
    # Common patterns: "Norwegian Ministry of Finance" -> "Norway" -> "NO"
    authority_patterns = {
        "norwegian": "NO",
        "swedish": "SE",
        "danish": "DK",
        "finnish": "FI",
        "icelandic": "IS",
        "liechtenstein": "LI",
        "british": "GB",
        "uk": "GB",
    }
    
    value_lower = value_str.lower()
    for pattern, iso2 in authority_patterns.items():
        if pattern in value_lower:
            return iso2
    
    # Try to convert country name to ISO2 using country_converter
    try:
        iso2 = coco.convert(names=[value_str], to='ISO2', not_found=None)
        if iso2 and len(iso2) > 0 and iso2[0]:
            result = iso2[0] if isinstance(iso2, list) else iso2
            if result and result != "not found":
                return result
    except Exception as e:
        logger.debug(f"Failed to convert '{value_str}' to ISO2: {e}")
    
    # Last resort: try to extract country name from authority names
    # Remove common authority suffixes
    cleaned = value_str
    for suffix in ["Ministry of Finance", "Central Bank", "National Bank", "Bank", "Authority", "Ministry"]:
        cleaned = cleaned.replace(suffix, "").strip()
    
    if cleaned and cleaned != value_str:
        try:
            iso2 = coco.convert(names=[cleaned], to='ISO2', not_found=None)
            if iso2 and len(iso2) > 0 and iso2[0]:
                result = iso2[0] if isinstance(iso2, list) else iso2
                if result and result != "not found":
                    return result
        except:
            pass
    
    return None


def extract_measure_short(measure_type: str) -> Optional[str]:
    """
    Extract short measure code from full measure type name.
    
    Examples:
    - "Loan-to-value (LTV)" -> "LTV"
    - "Debt-to-income (DTI)" -> "DTI"
    - "Loan-to-income (LTI)" -> "LTI"
    - "Debt service-to-income (DSTI)" -> "DSTI"
    
    Args:
        measure_type: Full measure type string
        
    Returns:
        Short code (LTV, DTI, LTI, DSTI) or None
    """
    if pd.isna(measure_type) or not measure_type:
        return None
    
    measure_str = str(measure_type).strip()
    
    # Check for patterns like "(LTV)", "(DTI)", etc.
    match = re.search(r'\(([A-Z]+)\)', measure_str)
    if match:
        return match.group(1)
    
    # Direct match for common abbreviations
    measure_upper = measure_str.upper()
    if "LTV" in measure_upper:
        return "LTV"
    elif "DTI" in measure_upper and "DSTI" not in measure_upper:
        return "DTI"
    elif "LTI" in measure_upper:
        return "LTI"
    elif "DSTI" in measure_upper:
        return "DSTI"
    
    return None


def convert_to_date(value: Any) -> Optional[str]:
    """
    Convert pandas Timestamp or date string to PostgreSQL DATE format (YYYY-MM-DD).
    
    Args:
        value: Timestamp, date string, or None
        
    Returns:
        Date string in YYYY-MM-DD format or None
    """
    if pd.isna(value) or value is None:
        return None
    
    try:
        if isinstance(value, pd.Timestamp):
            return value.strftime("%Y-%m-%d")
        elif isinstance(value, str):
            # Try to parse and reformat
            parsed = pd.to_datetime(value, errors='coerce')
            if pd.notna(parsed):
                return parsed.strftime("%Y-%m-%d")
        return None
    except Exception:
        return None


def convert_to_boolean(value: Any) -> bool:
    """
    Convert various boolean representations to Python bool.
    
    Args:
        value: String, bool, int, or None
        
    Returns:
        Boolean value
    """
    if pd.isna(value) or value is None:
        return False
    
    if isinstance(value, bool):
        return value
    
    if isinstance(value, (int, float)):
        return bool(value)
    
    value_str = str(value).strip().lower()
    return value_str in ["yes", "true", "1", "y"]


def transform_ccyb_data(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    Transform CCyB parquet data to Supabase format.
    
    Args:
        df: CCyB DataFrame from parquet
        
    Returns:
        List of dictionaries ready for Supabase insertion
    """
    if df.empty:
        return []
    
    records = []
    
    for _, row in df.iterrows():
        # Normalize ISO2
        iso2 = normalize_iso2(row.get("iso2") or row.get("country"))
        if not iso2:
            logger.warning(f"Skipping CCyB row: could not determine ISO2 for {row.get('country')}")
            continue
        
        record = {
            "country_iso2": iso2,
            "effective_date": convert_to_date(row.get("date")),
            "decision_date": convert_to_date(row.get("decision_date")),
            "announcement_date": convert_to_date(row.get("Date of Announcement")),
            "rate": float(row.get("rate", 0)) if pd.notna(row.get("rate")) else None,
            "status": str(row.get("status", "")).strip() if pd.notna(row.get("status")) else None,
            "credit_gap": float(row.get("credit_gap") or row.get("Credit Gap", 0)) if pd.notna(row.get("credit_gap") or row.get("Credit Gap")) else None,
            "credit_to_gdp": float(row.get("Credit-to-GDP", 0)) if pd.notna(row.get("Credit-to-GDP")) else None,
            "buffer_guide": float(row.get("Buffer Guide", 0)) if pd.notna(row.get("Buffer Guide")) else None,
            "justification": str(row.get("justification", "")).strip() if pd.notna(row.get("justification")) else None,
            "justification_exceptional": str(row.get("Justification exceptional circumstances", "")).strip() if pd.notna(row.get("Justification exceptional circumstances")) else None,
            "link": str(row.get("Link", "")).strip() if pd.notna(row.get("Link")) else None,
            "reference_date": convert_to_date(row.get("Reference date")),
        }
        
        records.append(record)
    
    logger.info(f"Transformed {len(records)} CCyB records")
    return records


def transform_syrb_data(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    Transform SyRB parquet data to Supabase format.
    
    Args:
        df: SyRB DataFrame from parquet
        
    Returns:
        List of dictionaries ready for Supabase insertion
    """
    if df.empty:
        return []
    
    records = []
    
    for _, row in df.iterrows():
        # Normalize ISO2
        iso2 = normalize_iso2(row.get("iso2") or row.get("country"))
        if not iso2:
            logger.warning(f"Skipping SyRB row: could not determine ISO2 for {row.get('country')}")
            continue
        
        record = {
            "country_iso2": iso2,
            "measure_type": str(row.get("syrb_type", "")).strip() if pd.notna(row.get("syrb_type")) else None,
            "sector": str(row.get("exposure_type", "")).strip() if pd.notna(row.get("exposure_type")) else None,
            "rate": float(row.get("rate_numeric", 0)) if pd.notna(row.get("rate_numeric")) else None,
            "effective_date": convert_to_date(row.get("date")),
            "decision_date": convert_to_date(row.get("Decision made on")),
            "status": str(row.get("status", "")).strip() if pd.notna(row.get("status")) else None,
            "description": str(row.get("description", "")).strip() if pd.notna(row.get("description")) else None,
            "basis_in_union_law": str(row.get("Basis in Union law", "")).strip() if pd.notna(row.get("Basis in Union law")) else None,
            "related_links": str(row.get("Related links", "")).strip() if pd.notna(row.get("Related links")) else None,
            "revocation_date": convert_to_date(row.get("revocation_date")),
            "revocation_note": str(row.get("Note of revocation/ replacement", "")).strip() if pd.notna(row.get("Note of revocation/ replacement")) else None,
        }
        
        records.append(record)
    
    logger.info(f"Transformed {len(records)} SyRB records")
    return records


def transform_bbm_data(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    Transform BBM parquet data to Supabase format.
    
    Args:
        df: BBM DataFrame from parquet
        
    Returns:
        List of dictionaries ready for Supabase insertion
    """
    if df.empty:
        return []
    
    records = []
    
    for _, row in df.iterrows():
        # Normalize ISO2
        iso2 = normalize_iso2(row.get("iso2") or row.get("country"))
        if not iso2:
            logger.warning(f"Skipping BBM row: could not determine ISO2 for {row.get('country')}")
            continue
        
        measure_type = str(row.get("measure_type", "")).strip() if pd.notna(row.get("measure_type")) else None
        measure_short = extract_measure_short(measure_type) if measure_type else None
        
        record = {
            "country_iso2": iso2,
            "measure_type": measure_type,
            "measure_short": measure_short,
            "status": str(row.get("status", "")).strip() if pd.notna(row.get("status")) else None,
            "active_status": str(row.get("active_status", "")).strip() if pd.notna(row.get("active_status")) else None,
            "description": str(row.get("description", "")).strip() if pd.notna(row.get("description")) else None,
            "intermediate_objective": str(row.get("Intermediate Objective", "")).strip() if pd.notna(row.get("Intermediate Objective")) else None,
            "basis_in_union_law": str(row.get("Basis in Union law", "")).strip() if pd.notna(row.get("Basis in Union law")) else None,
            "effective_date": convert_to_date(row.get("date")),
            "decision_date": convert_to_date(row.get("Decision made on")),
            "authority": str(row.get("Authority", "")).strip() if pd.notna(row.get("Authority")) else None,
            "year_initiative": int(row.get("Year initiative", 0)) if pd.notna(row.get("Year initiative")) else None,
            "parent_measure": str(row.get("Parent measure", "")).strip() if pd.notna(row.get("Parent measure")) else None,
            "has_been_revoked": convert_to_boolean(row.get("Has the measure been revoked or replaced?")),
            "revocation_date": convert_to_date(row.get("revocation_date")),
            "revocation_note": str(row.get("Note of revocation/ replacement", "")).strip() if pd.notna(row.get("Note of revocation/ replacement")) else None,
            "related_links": str(row.get("Related links", "")).strip() if pd.notna(row.get("Related links")) else None,
        }
        
        records.append(record)
    
    logger.info(f"Transformed {len(records)} BBM records")
    return records


def transform_osii_data(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    Transform OSII parquet data to Supabase format.
    
    Args:
        df: OSII DataFrame from parquet
        
    Returns:
        List of dictionaries ready for Supabase insertion
    """
    if df.empty:
        return []
    
    records = []
    
    for _, row in df.iterrows():
        # Normalize ISO2
        iso2 = normalize_iso2(row.get("iso2") or row.get("country"))
        if not iso2:
            logger.warning(f"Skipping OSII row: could not determine ISO2 for {row.get('country')}")
            continue
        
        record = {
            "country_iso2": iso2,
            "bank_name": str(row.get("bank_name", "")).strip() if pd.notna(row.get("bank_name")) else None,
            "lei_code": str(row.get("lei_code", "")).strip() if pd.notna(row.get("lei_code")) else None,
            "buffer_type": str(row.get("buffer_type", "")).strip() if pd.notna(row.get("buffer_type")) else None,
            "rate": float(row.get("rate_numeric", 0)) if pd.notna(row.get("rate_numeric")) else None,
            "effective_date": convert_to_date(row.get("date")),
            "status": str(row.get("status", "")).strip() if pd.notna(row.get("status")) else None,
        }
        
        records.append(record)
    
    logger.info(f"Transformed {len(records)} OSII records")
    return records


def _safe_float(v: Any) -> Optional[float]:
    """Convert to float or None; never return NaN (JSON-incompatible)."""
    if v is None or pd.isna(v):
        return None
    try:
        f = float(v)
        return None if (np.isnan(f) or np.isinf(f)) else f
    except (TypeError, ValueError):
        return None


def transform_dti_lti_data(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    Transform DTI/LTI CSV data to Supabase format.
    
    Args:
        df: DTI/LTI DataFrame from CSV
        
    Returns:
        List of dictionaries ready for Supabase insertion
    """
    if df.empty:
        return []
    
    records = []
    
    for _, row in df.iterrows():
        # Normalize ISO2 (handle "UK" -> "GB")
        country = str(row.get("Country", "")).strip()
        iso2 = normalize_iso2(country)
        if not iso2:
            logger.warning(f"Skipping DTI/LTI row: could not determine ISO2 for {country}")
            continue
        
        # Limit_standard can be string (e.g., "3.0x, 8.0x") - keep as TEXT
        limit_standard = row.get("Limit_Standard")
        if pd.notna(limit_standard):
            # Keep as string if it's already a string representation of a list
            if isinstance(limit_standard, str):
                limit_standard_str = limit_standard.strip()
            else:
                # Convert float to string with "x" suffix
                limit_standard_str = f"{float(limit_standard):.1f}x"
        else:
            limit_standard_str = None
        
        record = {
            "country_iso2": iso2,
            "measure_code": str(row.get("Measure_Code", "")).strip() if pd.notna(row.get("Measure_Code")) else None,
            "implementation_status": str(row.get("Implementation_Status", "")).strip() if pd.notna(row.get("Implementation_Status")) else None,
            "legal_form": str(row.get("Legal_Form", "")).strip() if pd.notna(row.get("Legal_Form")) else None,
            "limit_standard": limit_standard_str,
            "limit_ftb": _safe_float(row.get("Limit_FTB")),
            "limit_btl": _safe_float(row.get("Limit_BTL")),
            "limit_green": _safe_float(row.get("Limit_Green")),
            "income_basis": str(row.get("Income_Basis", "")).strip() if pd.notna(row.get("Income_Basis")) else None,
            "allowance_share": str(row.get("Allowance_Share", "")).strip() if pd.notna(row.get("Allowance_Share")) else None,
            "regulation_url": str(row.get("Regulation_URL", "")).strip() if pd.notna(row.get("Regulation_URL")) else None,
            "notes": str(row.get("Notes", "")).strip() if pd.notna(row.get("Notes")) else None,
        }
        
        records.append(record)
    
    logger.info(f"Transformed {len(records)} DTI/LTI records")
    return records


def transform_ltv_data(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    Transform LTV DataFrame to Supabase format.
    
    Args:
        df: LTV DataFrame (generated by pipeline)
        
    Returns:
        List of dictionaries ready for Supabase insertion
    """
    if df.empty:
        return []
    
    records = []
    
    for _, row in df.iterrows():
        # Normalize ISO2
        country = str(row.get("Country", "")).strip()
        iso2 = normalize_iso2(country)
        if not iso2:
            logger.warning(f"Skipping LTV row: could not determine ISO2 for {country}")
            continue
        
        # Limit_standard can be string (e.g., "80.0%, 90.0%") - keep as TEXT
        limit_standard = row.get("Limit_Standard")
        if pd.notna(limit_standard):
            if isinstance(limit_standard, str):
                limit_standard_str = limit_standard.strip()
            else:
                # Convert float to string with "%" suffix
                limit_standard_str = f"{float(limit_standard):.1f}%"
        else:
            limit_standard_str = None
        
        record = {
            "country_iso2": iso2,
            "implementation_status": str(row.get("Implementation_Status", "")).strip() if pd.notna(row.get("Implementation_Status")) else None,
            "legal_form": str(row.get("Legal_Form", "")).strip() if pd.notna(row.get("Legal_Form")) else None,
            "limit_standard": limit_standard_str,
            "limit_ftb": _safe_float(row.get("Limit_FTB")),
            "limit_btl": _safe_float(row.get("Limit_BTL")),
            "exception_quota": str(row.get("Exception_Quota", "")).strip() if pd.notna(row.get("Exception_Quota")) else None,
            "notes": str(row.get("Notes", "")).strip() if pd.notna(row.get("Notes")) else None,
        }
        
        records.append(record)
    
    logger.info(f"Transformed {len(records)} LTV records")
    return records


def transform_countries(ccyb_df: pd.DataFrame, syrb_df: pd.DataFrame, bbm_df: pd.DataFrame, osii_df: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    Generate countries lookup table from all data sources.
    
    Args:
        ccyb_df: CCyB DataFrame
        syrb_df: SyRB DataFrame
        bbm_df: BBM DataFrame
        osii_df: OSII DataFrame
        
    Returns:
        List of country dictionaries ready for Supabase insertion
    """
    # Collect all unique ISO2 codes
    iso2_set = set()
    
    for df in [ccyb_df, syrb_df, bbm_df, osii_df]:
        if not df.empty:
            if "iso2" in df.columns:
                iso2_set.update(df["iso2"].dropna().unique())
            if "country" in df.columns:
                for country in df["country"].dropna().unique():
                    iso2 = normalize_iso2(country)
                    if iso2:
                        iso2_set.add(iso2)
    
    # EEA and EU membership mapping
    eea_members = {
        "AT", "BE", "BG", "HR", "CY", "CZ", "DK", "EE", "FI", "FR", "DE", "GR", "HU",
        "IE", "IT", "LV", "LT", "LU", "MT", "NL", "PL", "PT", "RO", "SK", "SI", "ES",
        "SE", "NO", "IS", "LI"
    }
    
    eu_members = eea_members - {"NO", "IS", "LI"}
    
    # Normalize all ISO2 codes and remove duplicates
    normalized_iso2_set = set()
    for iso2_raw in iso2_set:
        if not iso2_raw:
            continue
        iso2 = normalize_iso2(iso2_raw)
        if iso2:
            normalized_iso2_set.add(iso2)
    
    records = []
    for iso2 in sorted(normalized_iso2_set):
        # Get country name from country_converter
        try:
            country_name = coco.convert(names=iso2, to='name_short', not_found=None)
            if not country_name or country_name == "not found":
                country_name = iso2
        except:
            country_name = iso2
        
        # Get ISO3
        try:
            iso3 = coco.convert(names=iso2, to='ISO3', not_found=None)
            if not iso3 or iso3 == "not found":
                iso3 = None
        except:
            iso3 = None
        
        record = {
            "iso2": iso2,
            "country_name": country_name,
            "iso3": iso3,
            "region": None,  # Can be enhanced later
            "eea_member": iso2 in eea_members,
            "eu_member": iso2 in eu_members,
        }
        
        records.append(record)
    
    logger.info(f"Generated {len(records)} country records")
    return records


def transform_snapshots(latest_ccyb: pd.DataFrame, latest_syrb: pd.DataFrame, latest_osii: pd.DataFrame) -> Dict[str, List[Dict[str, Any]]]:
    """
    Transform latest snapshot data to Supabase format.
    
    Args:
        latest_ccyb: Latest CCyB snapshot DataFrame
        latest_syrb: Latest SyRB snapshot DataFrame
        latest_osii: Latest OSII snapshot DataFrame
        
    Returns:
        Dictionary with keys: 'ccyb', 'syrb', 'osii' containing lists of records
    """
    from supabase_migration.transformers import normalize_iso2
    
    snapshots = {
        "ccyb": [],
        "syrb": [],
        "osii": [],
    }
    
    # CCyB Snapshot
    if not latest_ccyb.empty:
        for _, row in latest_ccyb.iterrows():
            iso2 = normalize_iso2(row.get("iso2") or row.get("country"))
            if iso2:
                snapshots["ccyb"].append({
                    "country_iso2": iso2,
                    "rate": float(row.get("rate", 0)) if pd.notna(row.get("rate")) else None,
                    "effective_date": convert_to_date(row.get("date")),
                    "credit_gap": float(row.get("credit_gap", 0)) if pd.notna(row.get("credit_gap")) else None,
                    "credit_to_gdp": float(row.get("Credit-to-GDP", 0)) if pd.notna(row.get("Credit-to-GDP")) else None,
                })
    
    # SyRB Snapshot (needs aggregation - simplified for now)
    if not latest_syrb.empty:
        for _, row in latest_syrb.iterrows():
            iso2 = normalize_iso2(row.get("iso2") or row.get("country"))
            if iso2:
                # Simplified - would need proper aggregation
                snapshots["syrb"].append({
                    "country_iso2": iso2,
                    "total_rate": None,  # Would need aggregation
                    "general_rate": None,
                    "sectoral_rate": None,
                })
    
    # OSII Snapshot (needs aggregation - simplified for now)
    if not latest_osii.empty:
        for _, row in latest_osii.iterrows():
            iso2 = normalize_iso2(row.get("iso2") or row.get("country"))
            if iso2:
                snapshots["osii"].append({
                    "country_iso2": iso2,
                    "total_rate": None,  # Would need aggregation
                    "osii_count": None,
                    "gsii_count": None,
                })
    
    logger.info(f"Generated snapshots: CCyB={len(snapshots['ccyb'])}, SyRB={len(snapshots['syrb'])}, OSII={len(snapshots['osii'])}")
    return snapshots


def transform_trends(
    agg_trend: pd.DataFrame,
    syrb_trend: pd.DataFrame,
    bbm_trend: pd.DataFrame,
    ccyb_df: pd.DataFrame = None,
    syrb_df: pd.DataFrame = None,
    bbm_df: pd.DataFrame = None,
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Transform aggregated trend data to Supabase format.
    
    Args:
        agg_trend: CCyB diffusion trend DataFrame (with 'date' and 'n_positive')
        syrb_trend: SyRB trend DataFrame
        bbm_trend: BBM diffusion trend DataFrame
        ccyb_df: Full CCyB DataFrame for calculating rate statistics
        syrb_df: Full SyRB DataFrame for calculating rate statistics
        bbm_df: Full BBM DataFrame for calculating statistics
        
    Returns:
        Dictionary with keys: 'ccyb', 'syrb', 'bbm' containing lists of records
    """
    trends = {
        "ccyb": [],
        "syrb": [],
        "bbm": [],
    }
    
    # CCyB Trend - calculate statistics from full CCyB data
    if not agg_trend.empty:
        for _, row in agg_trend.iterrows():
            date_str = convert_to_date(row.get("date"))
            if not date_str:
                continue
            
            # Calculate rate statistics for this date from full CCyB data
            avg_rate = None
            max_rate = None
            min_rate = None
            
            if ccyb_df is not None and not ccyb_df.empty:
                try:
                    date_val = pd.to_datetime(date_str)
                    # Get all CCyB records for this date (or closest)
                    date_records = ccyb_df[ccyb_df['date'] == date_val]
                    if date_records.empty:
                        # Try to find closest date
                        date_records = ccyb_df[ccyb_df['date'] <= date_val].sort_values('date').tail(1)
                    
                    if not date_records.empty:
                        rates = date_records['rate'].dropna()
                        if not rates.empty:
                            avg_rate = float(rates.mean())
                            max_rate = float(rates.max())
                            min_rate = float(rates.min())
                except Exception as e:
                    logger.debug(f"Error calculating CCyB stats for {date_str}: {e}")
            
            trends["ccyb"].append({
                "date": date_str,
                "countries_with_buffer": int(row.get("n_positive", 0)) if pd.notna(row.get("n_positive")) else 0,
                "avg_rate": avg_rate,
                "max_rate": max_rate,
                "min_rate": min_rate,
            })
    
    # SyRB Trend - calculate from full SyRB data
    if syrb_df is not None and not syrb_df.empty:
        try:
            # Group by date and calculate statistics
            syrb_by_date = syrb_df.groupby('date').agg({
                'rate_numeric': ['mean', 'max', 'min'],
                'syrb_type': lambda x: x.tolist()
            }).reset_index()
            
            # Count countries with General and Sectoral
            for date_val in syrb_df['date'].dropna().unique():
                date_records = syrb_df[syrb_df['date'] == date_val]
                general_countries = date_records[date_records['syrb_type'] == 'General']['country'].nunique()
                sectoral_countries = date_records[date_records['syrb_type'] == 'Sectoral']['country'].nunique()
                
                general_rates = date_records[date_records['syrb_type'] == 'General']['rate_numeric'].dropna()
                sectoral_rates = date_records[date_records['syrb_type'] == 'Sectoral']['rate_numeric'].dropna()
                
                trends["syrb"].append({
                    "date": convert_to_date(date_val),
                    "countries_with_general": int(general_countries) if general_countries > 0 else None,
                    "countries_with_sectoral": int(sectoral_countries) if sectoral_countries > 0 else None,
                    "avg_general_rate": float(general_rates.mean()) if not general_rates.empty else None,
                    "avg_sectoral_rate": float(sectoral_rates.mean()) if not sectoral_rates.empty else None,
                })
        except Exception as e:
            logger.warning(f"Error calculating SyRB trend stats: {e}")
    
    # BBM Trend - calculate from full BBM data
    if bbm_df is not None and not bbm_df.empty:
        try:
            # Group by date and measure type
            for date_val in bbm_df['date'].dropna().unique():
                date_records = bbm_df[bbm_df['date'] == date_val]
                active_records = date_records[date_records['active_status'] == 'Active']
                
                countries_with_bbm = active_records['country'].nunique()
                ltv_count = active_records[active_records['measure_type'].str.contains('LTV', case=False, na=False)].shape[0]
                dti_lti_count = active_records[active_records['measure_type'].str.contains('DTI|LTI', case=False, na=False)].shape[0]
                dsti_count = active_records[active_records['measure_type'].str.contains('DSTI', case=False, na=False)].shape[0]
                
                trends["bbm"].append({
                    "date": convert_to_date(date_val),
                    "countries_with_bbm": int(countries_with_bbm) if countries_with_bbm > 0 else None,
                    "ltv_count": int(ltv_count) if ltv_count > 0 else None,
                    "dti_lti_count": int(dti_lti_count) if dti_lti_count > 0 else None,
                    "dsti_count": int(dsti_count) if dsti_count > 0 else None,
                })
        except Exception as e:
            logger.warning(f"Error calculating BBM trend stats: {e}")
    
    logger.info(f"Generated trends: CCyB={len(trends['ccyb'])}, SyRB={len(trends['syrb'])}, BBM={len(trends['bbm'])}")
    return trends
