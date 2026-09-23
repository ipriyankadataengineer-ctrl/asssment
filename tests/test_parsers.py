"""
Unit Tests for Parsers
"""
import pytest
from src.parsers import parse_member_flat_file, parse_redemptions_json
from src.config import DATA_DIR

def test_parse_member_flat_file_filters_header():
    feed_path = DATA_DIR / 'sample_member_feed.dat'
    valid, corrupt = parse_member_flat_file(str(feed_path))
    
    # Check that none of the valid records have record_type == 'H'
    assert all(r['record_type'] == 'D' for r in valid)
    # Check that Elena exists
    assert any(r['member_name'] == 'Elena' for r in valid)

def test_parse_redemptions_json_flattening():
    json_path = DATA_DIR / 'sample_redemptions.json'
    flattened = parse_redemptions_json(str(json_path))
    
    # 4 transactions across 3 members
    assert len(flattened) == 4
    # Check transaction IDs
    txn_ids = [t['txn_id'] for t in flattened]
    assert 'RX10091' in txn_ids
    assert 'RX10092' in txn_ids
    assert 'RX10093' in txn_ids
    assert 'RX10094' in txn_ids
