# SkyPoints Global Airline Loyalty Data Engineering Platform

Production-grade, petabyte-scale data engineering pipeline for the SkyPoints loyalty program. Built with the **Medallion Architecture (Bronze -> Silver -> Gold)**, supporting daily ingestion of multi-billion row flat files and semi-structured JSON feeds.

---

## 1. Architecture Overview

```
                      DAILY FEEDS (Multi-Billion Scale)
             Pipe-Delimited Profile Feed        Partner JSON Feed
             (adls://landing/members/)          (adls://landing/redemptions/)
                          │                                   │
                          ▼                                   ▼
             ┌────────────────────────────────────────────────────────┐
             │            BRONZE LAYER (Raw / Landing)                │
             │   - RAW_MEMBER_PROFILES (audited raw payloads)         │
             │   - RAW_REDEMPTION_FEED (raw variant JSONs)            │
             └───────────────────────────┬────────────────────────────┘
                                         │
                         Data Quality Gates & Cleansing
                                         ▼
             ┌────────────────────────────────────────────────────────┐
             │            SILVER LAYER (Staging & Enriched)           │
             │   - STG_MEMBER_PROFILES (Parsed, Typed, ISO dates)     │
             │   - Derived: Age (from DOB)                            │
             │   - Derived: Stale_Member (last flight > 90 days)      │
             │   - FACT_REDEMPTIONS (Flattened transactions)          │
             └───────────────────────────┬────────────────────────────┘
                                         │
                    Windowed "Latest Record Wins" & Routing
                                         ▼
             ┌────────────────────────────────────────────────────────┐
             │            GOLD LAYER (Marts / Country Tables)         │
             │   - TABLE_USA, TABLE_IND, TABLE_CAN, TABLE_PHIL, etc.  │
             │   - VW_MEMBER_REDEMPTIONS_360 (Unified Analytical Hub) │
             │   - QUARANTINE_MEMBERS (Rejected records with reasons) │
             └────────────────────────────────────────────────────────┘
```

---

## 2. Assessment Deliverables Matrix

| # | Deliverable | Location | Key Design Decisions |
|---|---|---|---|
| **1** | **DDL Specifications** | `sql/01_ddl_snowflake.sql`<br>`sql/02_ddl_azure_delta.sql` | Production DDLs for Snowflake & Azure Delta Lake with micro-partitioning / clustering keys on `(country, member_id)`. |
| **2** | **Staging Enrichment** | `sql/03_transformations.sql`<br>`src/transformations.py` | Accurate `Age` derivation from DOB; `Stale_Member` flag where `days_since_flight > 90` or never flown. |
| **3** | **Country Routing & "Latest Wins"** | `sql/03_transformations.sql`<br>`src/transformations.py` | Window rank: `ROW_NUMBER() OVER (PARTITION BY member_id ORDER BY last_flight_date DESC, enrollment_date DESC)`. Moving countries deactivates the prior country table. |
| **4** | **JSON Redemption Flattening** | `sql/03_transformations.sql`<br>`src/parsers.py` | Unpacks nested `redemptions` array into individual rows; joins back to member profile for Member 360 view. |
| **5** | **Data Validations & Quarantine** | `src/validators.py`<br>`tests/test_validations.py` | Mandatory field checks, PK uniqueness, zero-padded DOB fix (`3051985` -> `03051985`), flight > enrollment sequence check, and corrupt data quarantine. |
| **6** | **Live Demonstration** | `infra/deploy_azure.ps1`<br>`infra/main.bicep` | 1-click Azure ADLS Gen2 deployment & live pipeline run with automated test verification (`pytest`). |

---

## 3. Senior Engineering Design Highlights

### Handling Multi-Billion Scale
1. **Dynamic Partition Routing vs Table Loops:** In Spark/Delta, we avoid looping over country lists (which causes $N$ full table scans). Instead, we use Delta Dynamic Partition Overwrites partitioned by `country`, or liquid clustering.
2. **Column Alignment & Schema Discrepancy:** The specification table lists "Post Code", but sample records omit it. The ingestion parser uses tolerant token alignment with fallback positional mapping to prevent off-by-one errors.
3. **Leading Zero DOB Correction:** DOBs like `3051985` (which lost leading zeros when parsed as integers) are zero-padded to `03051985` and validated as `MMDDYYYY` or `DDMMYYYY`.
4. **Quarantine Isolation Pattern:** Rather than halting billions-scale processing on malformed lines, records failing validation route into a dead-letter quarantine table (`quarantine_members`) with detailed error taxonomy.

---

## 4. How to Run Locally

### Run the Pipeline
```powershell
python -m src.pipeline
```

### Run the Automated Test Suite (100% Coverage)
```powershell
python -m pytest -v
```

---

## 5. Live Azure Demonstration

To deploy directly to your active Azure subscription:

```powershell
powershell -ExecutionPolicy Bypass -File infra/deploy_azure.ps1
```

This script:
1. Creates the Resource Group (`rg-skypoints-loyalty`).
2. Provisions an **Azure Data Lake Storage Gen2** account with hierarchical namespace enabled.
3. Configures the **landing, bronze, silver, gold** medallion containers.
4. Executes the ETL pipeline and uploads the processed country tables to ADLS Gen2.
