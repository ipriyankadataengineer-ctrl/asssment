"""
Unit Tests for Staging Enrichment, Deduplication & Routing
"""
from datetime import date
from src.transformations import (
    calculate_age,
    enrich_staging_records,
    deduplicate_latest_record_wins,
    split_members_by_country
)

def test_calculate_age():
    dob = date(1990, 5, 10)
    ref_date = date(2026, 9, 23)
    assert calculate_age(dob, ref_date) == 36

def test_stale_member_flag():
    ref_date = date(2026, 9, 23)
    records = [
        {
            'member_id': '1', 'member_name': 'Recent',
            'date_of_birth_parsed': date(1995, 1, 1),
            'last_flight_date_parsed': date(2026, 9, 1)  # 22 days ago (< 90)
        },
        {
            'member_id': '2', 'member_name': 'Stale',
            'date_of_birth_parsed': date(1995, 1, 1),
            'last_flight_date_parsed': date(2025, 1, 1)  # > 90 days ago
        },
        {
            'member_id': '3', 'member_name': 'NeverFlown',
            'date_of_birth_parsed': date(1995, 1, 1),
            'last_flight_date_parsed': None              # Never flown
        }
    ]
    enriched = enrich_staging_records(records, ref_date=ref_date)
    assert enriched[0]['stale_member'] == 'N'
    assert enriched[1]['stale_member'] == 'Y'
    assert enriched[2]['stale_member'] == 'Y'

def test_latest_record_wins_when_member_moved_country():
    # Elena lived in USA in 2012, moved to CAN with flight in 2024
    records = [
        {
            'member_id': '223457',
            'member_name': 'Elena',
            'country_standardized': 'USA',
            'enrollment_date_parsed': date(2010, 10, 12),
            'last_flight_date_parsed': date(2012, 10, 13),
            'raw_line_number': 2
        },
        {
            'member_id': '223457',
            'member_name': 'Elena',
            'country_standardized': 'CAN',
            'enrollment_date_parsed': date(2010, 10, 12),
            'last_flight_date_parsed': date(2024, 1, 10),  # Newer flight!
            'raw_line_number': 7
        }
    ]
    deduped = deduplicate_latest_record_wins(records)
    assert len(deduped) == 1
    winner = deduped[0]
    assert winner['country_standardized'] == 'CAN'
    assert winner['last_flight_date_parsed'] == date(2024, 1, 10)

    # Country split should place Elena ONLY in table_can
    tables = split_members_by_country(deduped)
    assert 'table_can' in tables
    assert 'table_usa' not in tables
    assert len(tables['table_can']) == 1
