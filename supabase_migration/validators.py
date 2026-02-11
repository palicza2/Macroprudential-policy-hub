"""
Data Validators Module.

Validates data before migration to ensure data integrity.
"""

import pandas as pd
from typing import Dict, Any, List, Tuple
import logging

logger = logging.getLogger(__name__)


def validate_iso2(iso2: str) -> bool:
    """
    Validate ISO2 code format.
    
    Args:
        iso2: ISO2 code to validate
        
    Returns:
        True if valid, False otherwise
    """
    if not iso2 or not isinstance(iso2, str):
        return False
    return len(iso2) == 2 and iso2.isalpha() and iso2.isupper()


def validate_date(date_str: str) -> bool:
    """
    Validate date string format (YYYY-MM-DD).
    
    Args:
        date_str: Date string to validate
        
    Returns:
        True if valid, False otherwise
    """
    if not date_str or not isinstance(date_str, str):
        return False
    try:
        pd.to_datetime(date_str)
        return True
    except:
        return False


def validate_rate(rate: float, min_val: float = 0, max_val: float = 20) -> bool:
    """
    Validate rate value.
    
    Args:
        rate: Rate value to validate
        min_val: Minimum allowed value
        max_val: Maximum allowed value
        
    Returns:
        True if valid, False otherwise
    """
    if pd.isna(rate):
        return True  # NULL is allowed
    try:
        rate_float = float(rate)
        return min_val <= rate_float <= max_val
    except:
        return False


def validate_data_before_migration(
    ccyb_records: List[Dict[str, Any]],
    syrb_records: List[Dict[str, Any]],
    bbm_records: List[Dict[str, Any]],
    osii_records: List[Dict[str, Any]],
    dti_lti_records: List[Dict[str, Any]],
    ltv_records: List[Dict[str, Any]],
    countries_records: List[Dict[str, Any]],
) -> Tuple[bool, List[str]]:
    """
    Validate all data records before migration.
    
    Args:
        ccyb_records: CCyB records to validate
        syrb_records: SyRB records to validate
        bbm_records: BBM records to validate
        osii_records: OSII records to validate
        dti_lti_records: DTI/LTI records to validate
        ltv_records: LTV records to validate
        countries_records: Countries records to validate
        
    Returns:
        Tuple of (is_valid, list_of_errors)
    """
    errors = []
    
    # Validate countries first (foreign key dependency)
    logger.info("Validating countries...")
    for record in countries_records:
        iso2 = record.get("iso2")
        if not validate_iso2(iso2):
            errors.append(f"Invalid ISO2 in countries: {iso2}")
    
    # Validate CCyB
    logger.info("Validating CCyB records...")
    for i, record in enumerate(ccyb_records):
        iso2 = record.get("country_iso2")
        if not validate_iso2(iso2):
            errors.append(f"CCyB record {i}: Invalid country_iso2: {iso2}")
        
        effective_date = record.get("effective_date")
        if effective_date and not validate_date(effective_date):
            errors.append(f"CCyB record {i}: Invalid effective_date: {effective_date}")
        
        rate = record.get("rate")
        if rate is not None and not validate_rate(rate, 0, 20):
            errors.append(f"CCyB record {i}: Invalid rate: {rate}")
    
    # Validate SyRB
    logger.info("Validating SyRB records...")
    for i, record in enumerate(syrb_records):
        iso2 = record.get("country_iso2")
        if not validate_iso2(iso2):
            errors.append(f"SyRB record {i}: Invalid country_iso2: {iso2}")
        
        rate = record.get("rate")
        if rate is not None and not validate_rate(rate, 0, 20):
            errors.append(f"SyRB record {i}: Invalid rate: {rate}")
    
    # Validate BBM
    logger.info("Validating BBM records...")
    for i, record in enumerate(bbm_records):
        iso2 = record.get("country_iso2")
        if not validate_iso2(iso2):
            errors.append(f"BBM record {i}: Invalid country_iso2: {iso2}")
    
    # Validate OSII
    logger.info("Validating OSII records...")
    for i, record in enumerate(osii_records):
        iso2 = record.get("country_iso2")
        if not validate_iso2(iso2):
            errors.append(f"OSII record {i}: Invalid country_iso2: {iso2}")
        
        rate = record.get("rate")
        if rate is not None and not validate_rate(rate, 0, 5):
            errors.append(f"OSII record {i}: Invalid rate: {rate} (should be 0-5)")
    
    # Validate DTI/LTI
    logger.info("Validating DTI/LTI records...")
    for i, record in enumerate(dti_lti_records):
        iso2 = record.get("country_iso2")
        if not validate_iso2(iso2):
            errors.append(f"DTI/LTI record {i}: Invalid country_iso2: {iso2}")
        
        measure_code = record.get("measure_code")
        if measure_code not in ["DTI", "LTI"]:
            errors.append(f"DTI/LTI record {i}: Invalid measure_code: {measure_code}")
    
    # Validate LTV
    logger.info("Validating LTV records...")
    for i, record in enumerate(ltv_records):
        iso2 = record.get("country_iso2")
        if not validate_iso2(iso2):
            errors.append(f"LTV record {i}: Invalid country_iso2: {iso2}")
        
        limit_ftb = record.get("limit_ftb")
        if limit_ftb is not None and not validate_rate(limit_ftb, 0, 100):
            errors.append(f"LTV record {i}: Invalid limit_ftb: {limit_ftb} (should be 0-100)")
        
        limit_btl = record.get("limit_btl")
        if limit_btl is not None and not validate_rate(limit_btl, 0, 100):
            errors.append(f"LTV record {i}: Invalid limit_btl: {limit_btl} (should be 0-100)")
    
    is_valid = len(errors) == 0
    
    if is_valid:
        logger.info("✅ All data validation passed!")
    else:
        logger.warning(f"⚠️ Found {len(errors)} validation errors")
        for error in errors[:10]:  # Show first 10 errors
            logger.warning(f"  - {error}")
        if len(errors) > 10:
            logger.warning(f"  ... and {len(errors) - 10} more errors")
    
    return is_valid, errors
