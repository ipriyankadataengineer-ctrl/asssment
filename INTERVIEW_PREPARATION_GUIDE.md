# SkyPoints Airline Loyalty Program - Senior Azure Data Engineer Interview & Architecture Guide

Comprehensive technical preparation guide covering all deliverables in the technical assessment: Architecture, Databricks PySpark, Azure Data Factory (ADF), Data Modeling (DDL), Incremental Loading at Petabyte Scale, and Interview Talking Points.

---

## TABLE OF CONTENTS
1. [End-to-End Medallion Architecture](#1-end-to-end-medallion-architecture)
2. [Databricks PySpark Code Deep Dive](#2-databricks-pyspark-code-deep-dive)
3. [Azure Data Factory (ADF) Orchestration Deep Dive](#3-azure-data-factory-adf-orchestration-deep-dive)
4. [Data Modeling & DDL Specifications (Snowflake & Delta Lake)](#4-data-modeling--ddl-specifications)
5. [Multi-Billion Scale & Incremental Processing Strategy](#5-multi-billion-scale--incremental-processing-strategy)
6. [Data Quality Gates & Hidden Traps in Assessment Data](#6-data-quality-gates--hidden-traps-in-assessment-data)
7. [Senior Interview Q&A Cheat Sheet](#7-senior-interview-qa-cheat-sheet)

---

## 1. End-to-End Medallion Architecture

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

## 2. Databricks PySpark Code Deep Dive

The production PySpark notebook is located at: `notebooks/skypoints_databricks_pipeline.py`.

### 2.1 Serverless Cloud Ingestion
```python
from azure.storage.blob import BlobServiceClient

blob_service = BlobServiceClient.from_connection_string(STORAGE_CONN_STR)
raw_flat_bytes = blob_service.get_blob_client("landing", "sample_member_feed.dat").download_blob().readall()
raw_lines = [line.strip() for line in raw_flat_bytes.decode("utf-8").splitlines() if line.strip()]

raw_df = spark.createDataFrame([(line,) for line in raw_lines], ["value"])
```
* **Why this approach?** In Databricks Serverless compute with Unity Catalog, low-level Hadoop filesystem manipulation via `spark.conf.set("fs.azure.account.key...")` is blocked for tenant isolation. Using the native Azure Storage SDK streams data directly into distributed Spark memory with zero Hadoop configuration errors.

### 2.2 Header Filtering & Delimiter Alignment
```python
# 1. Filter out Header (|H|) lines
detail_df = raw_df.filter(F.col("value").startswith("|D|") | F.col("value").startswith("D|"))

# 2. Strip leading pipe and tokenize
split_col = F.split(F.regexp_replace(F.col("value"), "^\|", ""), "\|")
```
* **The Gotcha:** The file has mixed record types (`|H|` for Header, `|D|` for Detail). A raw split on `|` produces an empty string at index 0 because the line begins with a pipe. Stripping `^\|` aligns tokens correctly: index 0 is `D`, index 1 is `Member_Name`, index 2 is `Member_Id`.

### 2.3 Leading Zero Recovery on DOB
```python
F.lpad(F.trim(split_col.getItem(9)), 8, "0").alias("raw_dob")
```
* **The Gotcha:** The assessment sample intermediate table displayed `3051985` instead of `03051985`. The leading zero was lost due to integer casting. `lpad(..., 8, "0")` restores the missing leading zero before casting to ISO `DATE`.

### 2.4 Staging Derived Columns (Deliverable 2)
```python
# Metric 1: Age
F.floor(F.datediff(F.current_date(), F.col("date_of_birth")) / 365.25).cast("int").alias("age")

# Metric 2: Stale_Member Flag (> 90 days or never flown)
F.when(
    F.col("last_flight_date").isNull() | (F.datediff(F.current_date(), F.col("last_flight_date")) > 90),
    F.lit("Y")
).otherwise(F.lit("N")).alias("stale_member")
```
* **Age:** Dividing by 365.25 accounts for leap year drift accurately.
* **Stale Member Flag:** Evaluates members who have either never flown (`last_flight_date IS NULL`) or whose last flight was over 90 days ago.

### 2.5 "Latest Record Wins" Across Country Moves (Deliverable 3)
```python
window_spec = Window.partitionBy("member_id").orderBy(
    F.col("last_flight_date").desc_nulls_last(),
    F.col("enrollment_date").desc()
)

deduped_df = stg_df.withColumn("rank", F.row_number().over(window_spec))                    .filter(F.col("rank") == 1)                    .drop("rank")
```
* **Country Relocation Handling:** Elena moved from the USA (2012 flight) to Canada (2024 flight). Window ranking awards rank 1 to Canada, completely removing her older USA record from the active mart.

### 2.6 Dynamic Delta Partition Overwriting
```python
deduped_df.write     .format("delta")     .mode("overwrite")     .partitionBy("country")     .saveAsTable("members_by_country")
```
* **Scale Rationale:** Instead of running 50 table scan loops in Python, Delta Dynamic Partition Overwriting organizes data into `country=<CODE>` storage partitions in a single distributed pass.

### 2.7 JSON Redemption Flattening & Broadcast Join (Deliverable 4)
```python
flattened_redemptions_df = raw_json_df.select(
    F.col("member_id"),
    F.explode(F.col("redemptions")).alias("r")
).select(
    F.col("r.txn_id").alias("txn_id"),
    F.col("member_id"),
    F.col("r.miles_redeemed").alias("miles_redeemed"),
    F.col("r.status").alias("status")
)

member_360_df = deduped_df.join(
    F.broadcast(redemption_agg_df),
    on="member_id",
    how="left"
)
```
* **`explode()`**: Converts the nested array into individual rows.
* **`broadcast()`**: Since aggregated redemption stats are smaller than the multi-billion member base, broadcasting eliminates cluster-wide shuffle overhead.

---

## 3. Azure Data Factory (ADF) Orchestration Deep Dive

### 3.1 Orchestration Workflow
```
[Schedule Trigger: 02:00 UTC]
           │
           ▼
┌──────────────────────────────────────┐     ┌──────────────────────────────────────┐
│ Activity 1: Check_Member_Feed_Exists │     │ Activity 2: Check_Redemption_Feed    │
│ (ADLS Gen2 GetMetadata)              │     │ (ADLS Gen2 GetMetadata)              │
└──────────────────┬───────────────────┘     └──────────────────┬───────────────────┘
                   │                                            │
                   └─────────────────────┬──────────────────────┘
                                         ▼
                   ┌────────────────────────────────────────────┐
                   │ Activity 3: Validate_Feeds_And_Process     │
                   │ (IfCondition Gatekeeper)                   │
                   └─────────────────────┬──────────────────────┘
                                         │
                 ┌───────────────────────┴───────────────────────┐
                 │ [Both Feeds Present]                          │ [Any Feed Missing]
                 ▼                                               ▼
┌──────────────────────────────────────────────┐   ┌────────────────────────────────┐
│ Activity 4: Execute_Databricks_Loyalty_ETL   │   │ Activity 5: Fail_Missing_Feeds │
│ (Runs Databricks Notebook on Serverless/Jobs)│   │ (Throws error & alerts On-Call)│
└──────────────────────────────────────────────┘   └────────────────────────────────┘
```

### 3.2 ADF Assets Deployed in Repository:
* `adf/linkedService/ls_adls_gen2.json`: Secure connection to ADLS Gen2 `stskypointsspeubmfodhieo`.
* `adf/linkedService/ls_azure_databricks.json`: Linked service to Databricks workspace `dbw-skypoints-loyalty`.
* `adf/dataset/ds_landing_member_feed.json`: Pipe-delimited dataset definition.
* `adf/dataset/ds_landing_redemption_feed.json`: JSON redemption dataset definition.
* `adf/pipeline/pipeline_skypoints_orchestrator.json`: Master pipeline with validation gates.
* `adf/trigger/trigger_daily_loyalty_run.json`: 02:00 UTC recurrence schedule.

---

## 4. Data Modeling & DDL Specifications

### 4.1 Snowflake DDL (`sql/01_ddl_snowflake.sql`)
```sql
-- Staging Table with Micro-Partitioning
CREATE OR REPLACE TABLE STAGING.STG_MEMBER_PROFILES (
    member_id            VARCHAR(18) NOT NULL,
    member_name          VARCHAR(255) NOT NULL,
    enrollment_date      DATE NOT NULL,
    last_flight_date     DATE,
    tier_code            VARCHAR(5),
    country              VARCHAR(10) NOT NULL,
    date_of_birth        DATE,
    age                  NUMBER(3, 0),
    stale_member         VARCHAR(1) NOT NULL,
    CONSTRAINT pk_stg_member PRIMARY KEY (member_id)
)
CLUSTER BY (country, member_id);

-- Country Target Table (e.g., Canada)
CREATE OR REPLACE TABLE MARTS.TABLE_CAN (
    member_id            VARCHAR(18) NOT NULL,
    member_name          VARCHAR(255) NOT NULL,
    country              VARCHAR(10) DEFAULT 'CAN',
    effective_start_date TIMESTAMP_NTZ NOT NULL,
    is_current           BOOLEAN DEFAULT TRUE,
    last_updated_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_table_can PRIMARY KEY (member_id)
);
```

### 4.2 Azure Delta Lake DDL (`sql/02_ddl_azure_delta.sql`)
```sql
CREATE TABLE IF NOT EXISTS gold.members_by_country (
    member_id            STRING NOT NULL,
    member_name          STRING NOT NULL,
    enrollment_date      DATE NOT NULL,
    last_flight_date     DATE,
    tier_code            STRING,
    country              STRING,
    age                  INT,
    stale_member         STRING,
    last_updated_at      TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@stskypointsspeubmfodhieo.dfs.core.windows.net/members_by_country'
CLUSTER BY (member_id);
```

---

## 5. Multi-Billion Scale & Incremental Processing Strategy

1. **Auto Loader (`cloudFiles`)**: Ingests new daily files using Azure Event Grid file notifications rather than directory listing, which fails at petabyte scale.
2. **Delta MERGE INTO (CDC)**: Updates existing member profiles and inserts new ones in atomic micro-transactions without rewriting the whole table.
3. **Delta File Compaction (`OPTIMIZE ... ZORDER BY (member_id)`)**: Co-locates data on disk to avoid the small-file problem and enable sub-second query lookups.
4. **Broadcast Hash Joins**: Avoids cluster-wide shuffle by broadcasting the aggregated redemptions table across nodes.

---

## 6. Data Quality Gates & Hidden Traps in Assessment Data

1. **Member Name marked as Primary Key**: The prompt table marked Member Name as `Key Column: Y`. In enterprise loyalty systems, Member ID is the true surrogate entity key because names have collisions.
2. **Post Code Column Mismatch**: The prompt table spec listed Post Code at position 9, but the sample flat file omitted it. The parser uses token alignment to prevent off-by-one errors.
3. **Dropped Leading Zero on DOB**: Restored via `LPAD` before date parsing.
4. **Flight Date Preceding Enrollment**: Checked in `src/validators.py`. Records where flight date < enrollment date route to the dead-letter quarantine table (`quarantine_members`).

---

## 7. Senior Interview Q&A Cheat Sheet

| Question | Senior Answer |
|---|---|
| **Why Databricks + ADLS Gen2?** | "Billions of records translate to 2–5 TB daily uncompressed. Single-node processing (Pandas/RDBMS) crashes with OOM. Apache Spark distributes memory and compute across worker nodes, and ADLS Gen2 provides high-throughput blob streaming." |
| **How do you handle members moving countries?** | "We use window ranking `ROW_NUMBER() OVER (PARTITION BY member_id ORDER BY last_flight_date DESC)`. The latest country record receives rank 1 and routes to that country's target table. The previous country record is deactivated." |
| **Why use GetMetadata in ADF?** | "It acts as a cost-optimization gatekeeper. If a feed is delayed or missing, ADF fails fast without spinning up expensive Databricks clusters." |
| **How do you handle corrupt records?** | "Dead-Letter Quarantine sink. Instead of halting a 4-hour batch job, invalid rows write to `quarantine_members` with explicit error codes for operational review." |
