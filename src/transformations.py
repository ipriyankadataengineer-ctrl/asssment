"""
Business Logic & Transformations (Deliverables 2, 3 & 4)
- Staging Enrichment: Age & Stale_Member calculations
- Deduplication: Latest Record Wins across country moves
- Routing: Splitting members into per-country target tables
- Member 360: Joining profile data to flattened redemptions
"""
from datetime import date
from typing import List, Dict, Any
from collections import defaultdict
from src.config import STALE_DAYS_THRESHOLD

def calculate_age(dob: date, ref_date: date = None) -> int:
    """Computes exact age based on date of birth."""
    if not dob:
        return None
    if ref_date is None:
        ref_date = date.today()
    age = ref_date.year - dob.year
    if (ref_date.month, ref_date.day) < (dob.month, dob.day):
        age -= 1
    return max(0, age)

def enrich_staging_records(records: List[Dict[str, Any]], ref_date: date = None) -> List[Dict[str, Any]]:
    """
    Deliverable 2:
    Populates staging records with:
      - age: Computed from date_of_birth
      - stale_member: 'Y' if days since last_flight_date > 90 (or null), else 'N'
    """
    if ref_date is None:
        ref_date = date.today()

    enriched = []
    for rec in records:
        r = dict(rec)
        dob = r.get('date_of_birth_parsed')
        flight_dt = r.get('last_flight_date_parsed')

        # 1. Compute Age
        r['age'] = calculate_age(dob, ref_date)

        # 2. Compute Stale_Member Flag (> 90 days or never flown)
        if flight_dt is None:
            r['stale_member'] = 'Y'
            r['days_since_flight'] = None
        else:
            delta_days = (ref_date - flight_dt).days
            r['days_since_flight'] = delta_days
            r['stale_member'] = 'Y' if delta_days > STALE_DAYS_THRESHOLD else 'N'

        enriched.append(r)
    return enriched

def deduplicate_latest_record_wins(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Deliverable 3:
    Applies the 'latest record wins' rule when a member has moved countries
    or has multiple profile records.
    Order precedence:
      1. last_flight_date_parsed (latest flight date wins)
      2. enrollment_date_parsed
      3. raw_line_number (latest feed line)
    """
    grouped = defaultdict(list)
    for rec in records:
        member_id = rec['member_id']
        grouped[member_id].append(rec)

    deduplicated = []
    for member_id, member_records in grouped.items():
        # Sort descending by latest flight date, enrollment date, and line number
        def sort_key(x):
            flight_dt = x.get('last_flight_date_parsed') or date.min
            enroll_dt = x.get('enrollment_date_parsed') or date.min
            line_no = x.get('raw_line_number', 0)
            return (flight_dt, enroll_dt, line_no)

        sorted_records = sorted(member_records, key=sort_key, reverse=True)
        winner = dict(sorted_records[0])
        deduplicated.append(winner)

    return deduplicated

def split_members_by_country(records: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """
    Deliverable 3:
    Splits deduplicated members into their corresponding country-specific target tables.
    """
    country_tables = defaultdict(list)
    for rec in records:
        country = rec.get('country_standardized', 'OTHER')
        table_name = f"table_{country.lower()}"
        country_tables[table_name].append(rec)
    return dict(country_tables)

def build_member_redemptions_360(
    members: List[Dict[str, Any]], 
    redemptions: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """
    Deliverable 4:
    Joins member profile data with flattened redemption metrics.
    """
    # Aggregate redemptions by member_id
    redemption_stats = defaultdict(lambda: {
        'total_redemptions': 0,
        'total_miles_redeemed': 0,
        'completed_miles': 0,
        'pending_miles': 0,
        'last_redemption_date': None
    })

    for r in redemptions:
        m_id = r['member_id']
        miles = r.get('miles_redeemed', 0)
        status = r.get('status', 'UNKNOWN')
        txn_dt = r.get('txn_date')

        stats = redemption_stats[m_id]
        stats['total_redemptions'] += 1
        stats['total_miles_redeemed'] += miles
        if status == 'COMPLETED':
            stats['completed_miles'] += miles
        elif status == 'PENDING':
            stats['pending_miles'] += miles

        if not stats['last_redemption_date'] or txn_dt > stats['last_redemption_date']:
            stats['last_redemption_date'] = txn_dt

    # Join back to member profile
    member_360 = []
    for m in members:
        m_id = m['member_id']
        stats = redemption_stats[m_id]
        row = dict(m)
        row.update(stats)
        member_360.append(row)

    return member_360
