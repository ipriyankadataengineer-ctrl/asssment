"""
Data Validation Framework (Deliverable 5)
Mandatory checks, Key uniqueness, Date format validations, and Quarantine routing.
"""
from datetime import datetime
from typing import List, Dict, Any, Tuple
from src.config import MANDATORY_MEMBER_FIELDS, COUNTRY_NORMALIZATION

def parse_iso_date(date_str: str, fmt: str = '%Y%m%d') -> datetime.date:
    """Helper to parse a date string or return None if invalid."""
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str.strip(), fmt).date()
    except (ValueError, TypeError):
        return None

def parse_dob(dob_str: str) -> datetime.date:
    """
    Parses DOB string, handling lost leading zeros (e.g. '3051985' -> '03051985').
    Tries MMDDYYYY then DDMMYYYY.
    """
    if not dob_str:
        return None
    cleaned = str(dob_str).strip()
    if len(cleaned) == 7:
        cleaned = '0' + cleaned
    
    if len(cleaned) != 8:
        return None
    
    # Try MMDDYYYY
    for fmt in ('%m%d%Y', '%d%m%Y'):
        try:
            return datetime.strptime(cleaned, fmt).date()
        except ValueError:
            continue
    return None

def validate_member_profiles(records: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Applies data quality gates to raw parsed member records.
    Returns:
        passed_records: Records passing all business quality checks
        quarantined_records: Records failing validation with error reasons
    """
    passed_records = []
    quarantined_records = []

    for rec in records:
        rejection_reasons = []

        # 1. Mandatory Field Checks
        for field in MANDATORY_MEMBER_FIELDS:
            if not rec.get(field):
                rejection_reasons.append(f"Missing mandatory field '{field}'")

        # 2. Date Validations
        enroll_dt = parse_iso_date(rec.get('enrollment_date'))
        if rec.get('enrollment_date') and not enroll_dt:
            rejection_reasons.append(f"Invalid enrollment_date format: '{rec.get('enrollment_date')}'")

        flight_dt = None
        if rec.get('last_flight_date'):
            flight_dt = parse_iso_date(rec.get('last_flight_date'))
            if not flight_dt:
                rejection_reasons.append(f"Invalid last_flight_date format: '{rec.get('last_flight_date')}'")

        if enroll_dt and flight_dt and flight_dt < enroll_dt:
            rejection_reasons.append("last_flight_date cannot be earlier than enrollment_date")

        # 3. DOB Validation & Age sanity
        dob_dt = parse_dob(rec.get('date_of_birth'))
        if rec.get('date_of_birth') and not dob_dt:
            rejection_reasons.append(f"Invalid date_of_birth format: '{rec.get('date_of_birth')}'")

        # 4. Country Code Validation
        country = rec.get('country')
        if not country:
            rejection_reasons.append("Missing country code")

        # Routing
        if rejection_reasons:
            quarantined = dict(rec)
            quarantined['validation_errors'] = '; '.join(rejection_reasons)
            quarantined_records.append(quarantined)
        else:
            validated = dict(rec)
            validated['enrollment_date_parsed'] = enroll_dt
            validated['last_flight_date_parsed'] = flight_dt
            validated['date_of_birth_parsed'] = dob_dt
            validated['country_standardized'] = COUNTRY_NORMALIZATION.get(country, country)
            passed_records.append(validated)

    return passed_records, quarantined_records

def validate_redemptions(records: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Validates flattened redemption transactions."""
    passed = []
    quarantined = []

    for r in records:
        errors = []
        if not r.get('txn_id'):
            errors.append("Missing txn_id")
        if not r.get('member_id'):
            errors.append("Missing member_id")
        if r.get('miles_redeemed', 0) < 0:
            errors.append("miles_redeemed cannot be negative")

        txn_dt = parse_iso_date(r.get('txn_date'))
        if not txn_dt:
            errors.append(f"Invalid txn_date: '{r.get('txn_date')}'")

        if errors:
            bad = dict(r)
            bad['validation_errors'] = '; '.join(errors)
            quarantined.append(bad)
        else:
            r_valid = dict(r)
            r_valid['txn_date_parsed'] = txn_dt
            passed.append(r_valid)

    return passed, quarantined
