-- ============================================================================
-- SKYPOINTS AIRLINE LOYALTY PLATFORM - AZURE DATABRICKS / DELTA LAKE DDL
-- Multi-Billion Scale Medallion Architecture (Bronze -> Silver -> Gold)
-- Storage: Azure Data Lake Storage Gen2 (ADLS Gen2)
-- Format: Delta Lake with Liquid Clustering & Partition Pruning
-- ============================================================================

CREATE CATALOG IF NOT EXISTS skypoints_catalog;
USE CATALOG skypoints_catalog;

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- ============================================================================
-- 1. BRONZE LAYER (ADLS Gen2: /bronze/)
-- ============================================================================

CREATE TABLE IF NOT EXISTS bronze.raw_member_profiles (
    raw_record_id        STRING,
    record_type          STRING,
    raw_payload          STRING,
    source_file_name     STRING,
    file_row_number      BIGINT,
    ingestion_timestamp  TIMESTAMP
)
USING DELTA
LOCATION 'abfss://bronze@<storage_account>.dfs.core.windows.net/raw_member_profiles'
CLUSTER BY (DATE(ingestion_timestamp));

CREATE TABLE IF NOT EXISTS bronze.raw_redemption_feed (
    raw_feed_id          STRING,
    payload_json         STRING,
    source_file_name     STRING,
    ingestion_timestamp  TIMESTAMP
)
USING DELTA
LOCATION 'abfss://bronze@<storage_account>.dfs.core.windows.net/raw_redemption_feed'
CLUSTER BY (DATE(ingestion_timestamp));

-- ============================================================================
-- 2. SILVER LAYER (ADLS Gen2: /silver/)
-- Enriched staging table with Age, Stale_Member, and cleaned data types
-- ============================================================================

CREATE TABLE IF NOT EXISTS silver.stg_member_profiles (
    member_id            STRING NOT NULL,
    member_name          STRING NOT NULL,
    enrollment_date      DATE NOT NULL,
    last_flight_date     DATE,
    tier_code            STRING,
    agent_name           STRING,
    state                STRING,
    country              STRING NOT NULL,
    post_code            STRING,
    date_of_birth        DATE,
    is_active            STRING,
    age                  INT,
    stale_member         STRING,
    days_since_flight    INT,
    source_file_name     STRING,
    ingestion_timestamp  TIMESTAMP
)
USING DELTA
LOCATION 'abfss://silver@<storage_account>.dfs.core.windows.net/stg_member_profiles'
CLUSTER BY (country, member_id);

-- ============================================================================
-- 3. GOLD LAYER - COUNTRY TARGET TABLES (ADLS Gen2: /gold/)
-- Deduplicated with 'latest record wins' rule
-- ============================================================================

CREATE TABLE IF NOT EXISTS gold.table_usa (
    member_id            STRING NOT NULL,
    member_name          STRING NOT NULL,
    enrollment_date      DATE NOT NULL,
    last_flight_date     DATE,
    tier_code            STRING,
    agent_name           STRING,
    state                STRING,
    country              STRING,
    post_code            STRING,
    date_of_birth        DATE,
    is_active            STRING,
    age                  INT,
    stale_member         STRING,
    effective_start_date TIMESTAMP,
    is_current           BOOLEAN,
    last_updated_at      TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@<storage_account>.dfs.core.windows.net/table_usa'
CLUSTER BY (member_id);

CREATE TABLE IF NOT EXISTS gold.table_ind (
    member_id            STRING NOT NULL,
    member_name          STRING NOT NULL,
    enrollment_date      DATE NOT NULL,
    last_flight_date     DATE,
    tier_code            STRING,
    agent_name           STRING,
    state                STRING,
    country              STRING,
    post_code            STRING,
    date_of_birth        DATE,
    is_active            STRING,
    age                  INT,
    stale_member         STRING,
    effective_start_date TIMESTAMP,
    is_current           BOOLEAN,
    last_updated_at      TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@<storage_account>.dfs.core.windows.net/table_ind'
CLUSTER BY (member_id);

CREATE TABLE IF NOT EXISTS gold.table_can (
    member_id            STRING NOT NULL,
    member_name          STRING NOT NULL,
    enrollment_date      DATE NOT NULL,
    last_flight_date     DATE,
    tier_code            STRING,
    agent_name           STRING,
    state                STRING,
    country              STRING,
    post_code            STRING,
    date_of_birth        DATE,
    is_active            STRING,
    age                  INT,
    stale_member         STRING,
    effective_start_date TIMESTAMP,
    is_current           BOOLEAN,
    last_updated_at      TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@<storage_account>.dfs.core.windows.net/table_can'
CLUSTER BY (member_id);

CREATE TABLE IF NOT EXISTS gold.table_phil (
    member_id            STRING NOT NULL,
    member_name          STRING NOT NULL,
    enrollment_date      DATE NOT NULL,
    last_flight_date     DATE,
    tier_code            STRING,
    agent_name           STRING,
    state                STRING,
    country              STRING,
    post_code            STRING,
    date_of_birth        DATE,
    is_active            STRING,
    age                  INT,
    stale_member         STRING,
    effective_start_date TIMESTAMP,
    is_current           BOOLEAN,
    last_updated_at      TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@<storage_account>.dfs.core.windows.net/table_phil'
CLUSTER BY (member_id);

CREATE TABLE IF NOT EXISTS gold.table_au (
    member_id            STRING NOT NULL,
    member_name          STRING NOT NULL,
    enrollment_date      DATE NOT NULL,
    last_flight_date     DATE,
    tier_code            STRING,
    agent_name           STRING,
    state                STRING,
    country              STRING,
    post_code            STRING,
    date_of_birth        DATE,
    is_active            STRING,
    age                  INT,
    stale_member         STRING,
    effective_start_date TIMESTAMP,
    is_current           BOOLEAN,
    last_updated_at      TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@<storage_account>.dfs.core.windows.net/table_au'
CLUSTER BY (member_id);

-- ============================================================================
-- 4. GOLD LAYER - REDEMPTIONS FACT TABLE
-- ============================================================================

CREATE TABLE IF NOT EXISTS gold.fact_redemptions (
    txn_id               STRING NOT NULL,
    member_id            STRING NOT NULL,
    feed_date            DATE NOT NULL,
    txn_date             DATE NOT NULL,
    partner              STRING NOT NULL,
    miles_redeemed       BIGINT NOT NULL,
    status               STRING NOT NULL,
    source_file_name     STRING,
    ingestion_timestamp  TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@<storage_account>.dfs.core.windows.net/fact_redemptions'
CLUSTER BY (txn_date, partner);