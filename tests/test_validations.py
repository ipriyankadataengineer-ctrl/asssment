"""
Unit Tests for Data Validations & Quality Gates (Deliverable 5)
"""
from src.validators import validate_member_profiles, parse_dob

def test_mandatory_field_validation():
    records = [
        {'member_id': '101', 'member_name': 'Valid User', 'enrollment_date': '20230101', 'country': 'USA'},
        {'member_id': None, 'member_name': 'No ID User', 'enrollment_date': '20230101', 'country': 'USA'},
        {'member_id': '102', 'member_name': None, 'enrollment_date': '20230101', 'country': 'USA'},
    ]
    valid, quarantined = validate_member_profiles(records)
    assert len(valid) == 1
    assert len(quarantined) == 2
    assert "Missing mandatory field 'member_id'" in quarantined[0]['validation_errors']
    assert "Missing mandatory field 'member_name'" in quarantined[1]['validation_errors']

def test_dob_parsing_leading_zero():
    # Lost leading zero: '3051985' -> 03-05-1985
    dt = parse_dob('3051985')
    assert dt is not None
    assert dt.year == 1985

def test_flight_before_enrollment_rejected():
    records = [{
        'member_id': '105',
        'member_name': 'Time Traveler',
        'enrollment_date': '20230501',
        'last_flight_date': '20210101',  # Earlier than enrollment!
        'country': 'USA'
    }]
    valid, quarantined = validate_member_profiles(records)
    assert len(valid) == 0
    assert len(quarantined) == 1
    assert "last_flight_date cannot be earlier than enrollment_date" in quarantined[0]['validation_errors']
