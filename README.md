# SkyPoints Global Airline Loyalty Platform — Azure Cloud Native Architecture

Production-grade, petabyte-scale data engineering platform for the SkyPoints loyalty program. Built and configured natively on **Microsoft Azure Cloud** using the **Medallion Architecture (Bronze -> Silver -> Gold)**, supporting daily ingestion of multi-billion row flat files and semi-structured JSON feeds.

---

## 1. Enterprise Azure Cloud Architecture

```
                          DAILY AIRLINE FEEDS (Multi-Billion Scale)
               Pipe-Delimited Profile Feed        Partner JSON Feed
               (adls://landing/members/)          (adls://landing/redemptions/)
                            │                                   │
                            ▼                                   ▼
               ┌────────────────────────────────────────────────────────┐
               │            BRONZE LAYER (ADLS Gen2 Landing)            │
               │   - RAW_MEMBER_PROFILES (audited raw payloads)         │
               │   - RAW_REDEMPTION_FEED (raw variant JSONs)            │
               └───────────────────────────┬────────────────────────────┘
                                           │
                        Azure Data Factory Orchestration & DQ Gates
                                           ▼
               ┌────────────────────────────────────────────────────────┐
               │         SILVER LAYER (Databricks / Delta Lake)         │
               │   - STG_MEMBER_PROFILES (Parsed, Typed, ISO dates)     │
               │   - Derived: Age (from DOB, leap-year adjusted)        │
               │   - Derived: Stale_Member (last flight > 90 days)      │
               │   - FACT_REDEMPTIONS (Flattened transaction items)     │
               └───────────────────────────┬────────────────────────────┘
                                           │
                      Windowed "Latest Record Wins" & Dynamic Routing
                                           ▼
               ┌────────────────────────────────────────────────────────┐
               │          GOLD LAYER (Azure Serving Marts & Hub)        │
               │   - TABLE_USA, TABLE_IND, TABLE_CAN, TABLE_PHIL, etc.  │
               │   - VW_MEMBER_REDEMPTIONS_360 (Unified Analytical Hub) │
               │   - QUARANTINE_MEMBERS (Dead-letter error sink)        │
               └────────────────────────────────────────────────────────┘
```

---

## 2. Live Azure Cloud Infrastructure Components

| Cloud Service | Azure Resource Name | Region | Configuration & Role |
|---|---|---|---|
| **Data Lake Storage** | `stskypointsspeubmfodhieo` | Central India | **ADLS Gen2** with Hierarchical Namespace enabled. Hosts Medallion containers: `landing`, `bronze`, `silver`, `gold`. |
| **Compute & ETL** | `dbw-skypoints-loyalty` | Central India | **Azure Databricks (Premium)** with Unity Catalog & Serverless Spark execution for distributed Delta Lake processing. |
| **Orchestration** | `adf-skypoints-loyalty` | Central India | **Azure Data Factory** orchestrator running `pipeline_skypoints_orchestrator` with Event-Based Trigger (`tr_on_file_upload_landing`) via Azure Event Grid on ADLS Gen2 landing file uploads. |
| **Data Warehouse DDL** | Snowflake / Delta Lake | Cloud DW | Production DDLs with micro-partitioning/clustering on `(country, member_id)` and SCD Type 1/2 tracking. |

---

## 3. Assessment Deliverables Matrix

| # | Assessment Deliverable | Azure Implementation Asset | Technical Highlights |
|---|---|---|---|
| **1** | **DDL Specifications** | `sql/01_ddl_snowflake.sql`<br>`sql/02_ddl_azure_delta.sql` | Production DDLs for Snowflake & Delta Lake on ADLS Gen2 with micro-partitioning / clustering keys on `(country, member_id)`. |
| **2** | **Staging Derived Metrics** | `notebooks/skypoints_databricks_pipeline.py`<br>`sql/03_transformations.sql` | • **Age**: Derived from DOB accounting for multi-decade leap year drift.<br>• **Stale_Member**: `Y` if `last_flight_date` is NULL or $>90$ days ago; otherwise `N`. |
| **3** | **Country Routing & "Latest Wins"** | `notebooks/skypoints_databricks_pipeline.py`<br>`sql/03_transformations.sql` | Window ranking: `ROW_NUMBER() OVER (PARTITION BY member_id ORDER BY last_flight_date DESC)`. Elena moved from USA to Canada $\rightarrow$ automatically routes to `table_can` and deactivates `table_usa`. |
| **4** | **JSON Redemption Flattening** | `notebooks/skypoints_databricks_pipeline.py`<br>`sql/03_transformations.sql` | Unpacks nested `redemptions: [...]` array into individual rows; joins back to member profile via **Broadcast Hash Join** for Member 360 view. |
| **5** | **Data Validations & Quarantine** | `src/validators.py`<br>`notebooks/skypoints_databricks_pipeline.py` | Mandatory field checks, PK uniqueness, zero-padded DOB fix (`3051985` &rarr; `03051985`), flight > enrollment sequence check, and corrupt data quarantine. |
| **6** | **Cloud Orchestration & Demo** | `adf/pipeline/pipeline_skypoints_orchestrator.json`<br>`adf/linkedService/` | Master Azure Data Factory pipeline with GetMetadata gatekeeper validation, Databricks dispatch, and recurring schedule triggers. |

---

## 4. Multi-Billion Scale Engineering Strategy

1. **Databricks Auto Loader (`cloudFiles`)**: Scalable event-driven file discovery via Azure Event Grid notifications, eliminating directory crawl bottlenecks on petabytes of incoming daily files.
2. **Delta Dynamic Partition Overwrites**: Organizes data into `country=<CODE>` storage partitions in a single distributed pass, eliminating 50 sequential table scan loops.
3. **Compaction & Z-Ordering**: Runs `OPTIMIZE members_by_country ZORDER BY (member_id)` to resolve the small-file problem and co-locate records on disk.
4. **Broadcast Hash Joins**: Broadcasts the compact aggregated redemptions table across worker nodes, eliminating cluster-wide network shuffle when joining against billions of member profiles.
5. **Dead-Letter Quarantine Pattern**: Isolates corrupted records into `quarantine_members` with failure reason tags, ensuring multi-hour batch pipelines complete without interruption.

---

## 5. Live Inspection in Azure Cloud UI

* **Azure Data Lake Storage Gen2**: In Azure Portal, open **Storage accounts &rarr; `stskypointsspeubmfodhieo` &rarr; Storage browser**. Inspect the `gold/country_tables/` and `gold/marts/` directories.
* **Azure Databricks Studio**: In Databricks, navigate to **Workspace &rarr; Users &rarr; `skypoints_databricks_pipeline`** to view the live PySpark execution cells.
* **Azure Data Factory Studio**: In ADF Studio, open **Author &rarr; Pipelines &rarr; `pipeline_skypoints_orchestrator`** to view the visual DAG, or **Monitor** to view the verified execution run.
