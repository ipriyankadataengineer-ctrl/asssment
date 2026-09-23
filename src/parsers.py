"""
Parsers for Flat Files and Semi-Structured JSON Feeds
Handles Header/Detail records, column alignment, and nested array flattening.
"""
import json
from pathlib import Path
from typing import List, Dict, Any, Tuple

def parse_member_flat_file(file_path: str) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Parses pipe-delimited member profile file.
    Filters Header (|H|) lines, parses Detail (|D|) lines.
    
    Returns:
        valid_records: List of parsed record dictionaries
        corrupt_records: List of malformed lines with line number and error
    """
    valid_records = []
    corrupt_records = []
    path = Path(file_path)

    with open(path, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, start=1):
            line_str = line.strip()
            if not line_str:
                continue

            tokens = line_str.split('|')
            # Sample: |D|Elena|223457|20101012|20121013|GLD|Sam|CA|USA|03051985|A
            # If leading pipe, tokens[0] is empty, record_type is tokens[1]
            if tokens and tokens[0] == '':
                tokens = tokens[1:]

            if not tokens:
                continue

            record_type = tokens[0].strip().upper()
            if record_type == 'H':
                # Skip Header record
                continue
            elif record_type == 'D':
                try:
                    # Expected fields:
                    # 1: Member Name
                    # 2: Member ID
                    # 3: Enrollment Date (YYYYMMDD)
                    # 4: Last Flight Date (YYYYMMDD)
                    # 5: Tier Code
                    # 6: Agent Name
                    # 7: State
                    # 8: Country
                    # 9: DOB (MMDDYYYY or DDMMYYYY, e.g. 03051985)
                    # 10: Is_Active (A / I)
                    # Note: Spec table mentioned Post Code, but sample layout omits it.
                    # We handle varying token lengths gracefully.
                    record = {
                        'raw_line_number': line_num,
                        'record_type': record_type,
                        'member_name': tokens[1].strip() if len(tokens) > 1 and tokens[1].strip() else None,
                        'member_id': tokens[2].strip() if len(tokens) > 2 and tokens[2].strip() else None,
                        'enrollment_date': tokens[3].strip() if len(tokens) > 3 and tokens[3].strip() else None,
                        'last_flight_date': tokens[4].strip() if len(tokens) > 4 and tokens[4].strip() else None,
                        'tier_code': tokens[5].strip() if len(tokens) > 5 and tokens[5].strip() else None,
                        'agent_name': tokens[6].strip() if len(tokens) > 6 and tokens[6].strip() else None,
                        'state': tokens[7].strip() if len(tokens) > 7 and tokens[7].strip() else None,
                        'country': tokens[8].strip().upper() if len(tokens) > 8 and tokens[8].strip() else None,
                        'date_of_birth': tokens[9].strip() if len(tokens) > 9 and tokens[9].strip() else None,
                        'is_active': tokens[10].strip().upper() if len(tokens) > 10 and tokens[10].strip() else 'A',
                        'post_code': None,
                        'source_file_name': path.name
                    }
                    valid_records.append(record)
                except Exception as ex:
                    corrupt_records.append({
                        'line_number': line_num,
                        'raw_payload': line_str,
                        'error_message': str(ex)
                    })
            else:
                corrupt_records.append({
                    'line_number': line_num,
                    'raw_payload': line_str,
                    'error_message': f"Unknown record type '{record_type}'"
                })

    return valid_records, corrupt_records

def parse_redemptions_json(file_path: str) -> List[Dict[str, Any]]:
    """
    Parses semi-structured JSON redemption feed and flattens
    the nested 'redemptions' array into individual queryable transaction rows.
    """
    flattened_txns = []
    path = Path(file_path)

    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # If single object, wrap in list
    if isinstance(data, dict):
        data = [data]

    for feed_item in data:
        member_id = str(feed_item.get('member_id', '')).strip()
        feed_date = str(feed_item.get('feed_date', '')).strip()
        redemptions = feed_item.get('redemptions', [])

        for r in redemptions:
            flattened_txns.append({
                'txn_id': str(r.get('txn_id', '')).strip(),
                'member_id': member_id,
                'feed_date': feed_date,
                'txn_date': str(r.get('txn_date', '')).strip(),
                'partner': str(r.get('partner', '')).strip(),
                'miles_redeemed': int(r.get('miles_redeemed', 0)),
                'status': str(r.get('status', 'UNKNOWN')).strip().upper(),
                'source_file_name': path.name
            })

    return flattened_txns
