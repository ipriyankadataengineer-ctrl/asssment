# SkyPoints Global Airline Loyalty Data Engineering Platform

Production-grade, petabyte-scale data engineering pipeline for the SkyPoints loyalty program. Built with the **Medallion Architecture (Bronze -> Silver -> Gold)**, supporting daily ingestion of multi-billion row flat files and semi-structured JSON feeds.

---

## 1. Live Azure & Databricks Deployment

| Resource | Service / Configuration | Live Azure Resource Name | Status |
|---|---|---|---|
| **Cloud Provider** | Microsoft Azure | `Azure subscription 1` (`9309ca0f-f25f-47e2-b887-bb4b9af961bf`) | **Active** |
| **Resource Group** | Azure Resource Group | `rg-skypoints-loyalty` (`centralindia`) | **Active** |
| **Data Lake** | ADLS Gen2 (Hierarchical Namespace) | `stskypointsspeubmfodhieo` | **Active** |
| **Medallion Storage** | Containers: `landing`, `bronze`, `silver`, `gold` | Live on ADLS Gen2 | **Synced** |
| **Compute Engine** | **Azure Databricks (Premium)** | `dbw-skypoints-loyalty` | **Active** |
| **Databricks URL** | Direct Workspace Access | [adb-7405606935596016.16.azuredatabricks.net](https://adb-7405606935596016.16.azuredatabricks.net) | **Live** |

---

## 2. Architecture Overview

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

## 3. Assessment Deliverables Matrix

| # | Deliverable | Location | Key Design Decisions |
|---|---|---|---|
| **1** | **DDL Specifications** | `sql/01_ddl_snowflake.sql`<br>`sql/02_ddl_azure_delta.sql` | Production DDLs for Snowflake & Azure Delta Lake with micro-partitioning / clustering keys on `(country, member_id)`. |
| **2** | **Staging Enrichment** | `sql/03_transformations.sql`<br>`src/transformations.py`<br>`notebooks/skypoints_databricks_pipeline.py` | Accurate `Age` derivation from DOB; `Stale_Member` flag where `days_since_flight > 90` or never flown. |
| **3** | **Country Routing & "Latest Wins"** | `sql/03_transformations.sql`<br>`src/transformations.py`<br>`notebooks/skypoints_databricks_pipeline.py` | Window rank: `ROW_NUMBER() OVER (PARTITION BY member_id ORDER BY last_flight_date DESC, enrollment_date DESC)`. Moving countries deactivates the prior country table. |
| **4** | **JSON Redemption Flattening** | `sql/03_transformations.sql`<br>`src/parsers.py`<br>`notebooks/skypoints_databricks_pipeline.py` | Unpacks nested `redemptions` array into individual rows; joins back to member profile for Member 360 view. |
| **5** | **Data Validations & Quarantine** | `src/validators.py`<br>`tests/test_validations.py` | Mandatory field checks, PK uniqueness, zero-padded DOB fix (`3051985` -> `03051985`), flight > enrollment sequence check, and corrupt data quarantine. |
| **6** | **Live Demonstration** | `infra/deploy_azure.ps1`<br>`infra/main.bicep`<br>`notebooks/skypoints_databricks_pipeline.py` | Live Azure ADLS Gen2 storage + Azure Databricks Premium workspace deployed and validated. |

---

## 4. Multi-Billion Scale Engineering Highlights

1. **Distributed Compute**: Apache Spark on Azure Databricks distributes workloads across nodes, scaling elastically for petabyte-scale loyalty workloads.
2. **Dynamic Delta Partition Overwrite**: Eliminates static loops over country tables; writes to `gold/members_by_country` partitioned by `country` in a single distributed pass.
3. **Quarantine Isolation Pattern**: Malformed records are routed to an append-only dead-letter sink on ADLS Gen2 (`quarantine_members`) without breaking pipeline execution.
4. **Auto Loader (`cloudFiles`) Ready**: Supports streaming ingestion as new files drop into the ADLS Gen2 landing container.

---

## 5. How to Run Locally

### Run the Pipeline
```powershell
python -m src.pipeline
```

### Run Automated Tests (100% Pass)
```powershell
python -m pytest -v
```

---

## 6. How to Run in Azure Databricks

1. Open your workspace: [https://adb-7405606935596016.16.azuredatabricks.net](https://adb-7405606935596016.16.azuredatabricks.net)
2. In the sidebar, navigate to **Workspace $\rightarrow$ Users $\rightarrow$ [Your Email]**.
3. Click **Import** and upload `notebooks/skypoints_databricks_pipeline.py`.
4. Attach to a cluster and click **Run All**.
