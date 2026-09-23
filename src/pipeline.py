"""
End-to-End Orchestration Pipeline for SkyPoints Airline Loyalty
Executes the Medallion flow: Landing -> Staging -> Gold Country Marts & Redemptions
"""
import sys
import json
from pathlib import Path
from datetime import date
from src.config import DATA_DIR, CONTAINER_LANDING, CONTAINER_SILVER, CONTAINER_GOLD
from src.parsers import parse_member_flat_file, parse_redemptions_json
from src.validators import validate_member_profiles, validate_redemptions
from src.transformations import (
    enrich_staging_records,
    deduplicate_latest_record_wins,
    split_members_by_country,
    build_member_redemptions_360
)
from src.azure_client import AzureLakehouseClient

def run_pipeline(
    member_feed_path: str = None, 
    redemption_feed_path: str = None,
    output_dir: str = None,
    ref_date: date = None
):
    """Executes the complete SkyPoints ETL pipeline."""
    if not member_feed_path:
        for ext in ['.csv', '.txt', '.dat']:
            cand = DATA_DIR / f'sample_member_feed{ext}'
            if cand.exists():
                member_feed_path = str(cand)
                break
        if not member_feed_path:
            member_feed_path = str(DATA_DIR / 'sample_member_feed.csv')
    if not redemption_feed_path:
        redemption_feed_path = str(DATA_DIR / 'sample_redemptions.json')
    if not output_dir:
        output_dir = str(DATA_DIR / 'output')

    out_path = Path(output_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    print("=" * 70)
    print("SKYPOINTS LOYALTY ETL PIPELINE - EXECUTION START")
    print("=" * 70)

    # ---------------------------------------------------------
    # STEP 1: RAW INGESTION & PARSING
    # ---------------------------------------------------------
    print(f"\n[1/5] Ingesting raw flat file: {member_feed_path}")
    raw_members, raw_corrupt = parse_member_flat_file(member_feed_path)
    print(f"      Parsed {len(raw_members)} detail records (Header lines filtered)")

    print(f"\n[2/5] Ingesting raw JSON redemption feed: {redemption_feed_path}")
    raw_redemptions = parse_redemptions_json(redemption_feed_path)
    print(f"      Flattened {len(raw_redemptions)} individual redemption transactions")

    # ---------------------------------------------------------
    # STEP 2: DATA VALIDATIONS & QUARANTINE (Deliverable 5)
    # ---------------------------------------------------------
    print("\n[3/5] Applying Data Quality Gates & Validation Rules...")
    valid_members, quarantined_members = validate_member_profiles(raw_members)
    valid_redemptions, quarantined_redemptions = validate_redemptions(raw_redemptions)

    print(f"      Members: {len(valid_members)} Passed, {len(quarantined_members)} Quarantined")
    if quarantined_members:
        for q in quarantined_members:
            print(f"      [Quarantine Reason]: {q.get('member_name')} -> {q.get('validation_errors')}")

    print(f"      Redemptions: {len(valid_redemptions)} Passed, {len(quarantined_redemptions)} Quarantined")

    # ---------------------------------------------------------
    # STEP 3: STAGING ENRICHMENT (Deliverable 2)
    # ---------------------------------------------------------
    print("\n[4/5] Computing Derived Metrics in Staging...")
    stg_enriched = enrich_staging_records(valid_members, ref_date=ref_date)
    for m in stg_enriched:
        print(f"      Member: {m['member_name']} (ID: {m['member_id']}) | Age: {m['age']} | Stale: {m['stale_member']} (Days since flight: {m['days_since_flight']})")

    # ---------------------------------------------------------
    # STEP 4: DEDUPLICATION & COUNTRY SPLIT (Deliverable 3)
    # ---------------------------------------------------------
    print("\n[5/5] Applying 'Latest Record Wins' Deduplication & Country Routing...")
    deduped_members = deduplicate_latest_record_wins(stg_enriched)
    print(f"      Deduplication: {len(stg_enriched)} staging records -> {len(deduped_members)} unique active members")

    country_tables = split_members_by_country(deduped_members)
    for table_name, rows in country_tables.items():
        print(f"      Target Table [{table_name.upper()}]: {len(rows)} members")
        for r in rows:
            print(f"        -> {r['member_name']} ({r['member_id']}), Tier: {r['tier_code']}, Country: {r['country_standardized']}")

    # ---------------------------------------------------------
    # STEP 5: JOIN REDEMPTIONS BACK TO MEMBER PROFILE (Deliverable 4)
    # ---------------------------------------------------------
    member_360 = build_member_redemptions_360(deduped_members, valid_redemptions)

    # ---------------------------------------------------------
    # EXPORT LOCAL RESULTS
    # ---------------------------------------------------------
    # Save Staging
    with open(out_path / 'stg_member_profiles.json', 'w', encoding='utf-8') as f:
        json.dump(stg_enriched, f, default=str, indent=2)

    # Save Country Tables
    country_out_dir = out_path / 'country_tables'
    country_out_dir.mkdir(exist_ok=True)
    for table_name, rows in country_tables.items():
        with open(country_out_dir / f"{table_name}.json", 'w', encoding='utf-8') as f:
            json.dump(rows, f, default=str, indent=2)

    # Save Flattened Redemptions & Member 360
    with open(out_path / 'fact_redemptions.json', 'w', encoding='utf-8') as f:
        json.dump(valid_redemptions, f, default=str, indent=2)

    with open(out_path / 'member_redemptions_360.json', 'w', encoding='utf-8') as f:
        json.dump(member_360, f, default=str, indent=2)

    # Save Quarantine Sinks
    if quarantined_members:
        with open(out_path / 'quarantine_members.json', 'w', encoding='utf-8') as f:
            json.dump(quarantined_members, f, default=str, indent=2)

    print("\n" + "=" * 70)
    print("PIPELINE EXECUTION COMPLETED SUCCESSFULLY!")
    print(f"Outputs written to: {out_path.resolve()}")
    print("=" * 70)

    # ---------------------------------------------------------
    # OPTIONAL: SYNC TO AZURE ADLS GEN2
    # ---------------------------------------------------------
    azure_client = AzureLakehouseClient()
    if azure_client.client:
        print("\n[Azure Live Sync] Uploading processed datasets to Azure Data Lake Storage Gen2...")
        azure_client.ensure_containers([CONTAINER_LANDING, CONTAINER_SILVER, CONTAINER_GOLD])
        azure_client.upload_json_records(CONTAINER_SILVER, stg_enriched, 'staging/stg_member_profiles.json')
        for table_name, rows in country_tables.items():
            azure_client.upload_json_records(CONTAINER_GOLD, rows, f"country_tables/{table_name}.json")
        azure_client.upload_json_records(CONTAINER_GOLD, valid_redemptions, 'marts/fact_redemptions.json')
        azure_client.upload_json_records(CONTAINER_GOLD, member_360, 'marts/member_redemptions_360.json')
        print("[Azure Live Sync] All tables synced to Azure Lakehouse successfully.")

    return {
        'staging': stg_enriched,
        'country_tables': country_tables,
        'redemptions': valid_redemptions,
        'member_360': member_360,
        'quarantined': quarantined_members
    }

if __name__ == '__main__':
    run_pipeline()
