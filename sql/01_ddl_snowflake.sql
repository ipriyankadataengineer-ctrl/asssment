-- ============================================================================
-- SKYPOINTS AIRLINE LOYALTY PLATFORM - SNOWFLAKE DDL SPECIFICATION
-- Deliverable 1: Raw Landing, Staging, Country Target Tables & Redemptions
-- Designed for Multi-Billion Scale with Clustering and Micro-Partitioning
-- ============================================================================

CREATE DATABASE IF NOT EXISTS SKYPOINTS_DB;
USE DATABASE SKYPOINTS_DB;

CREATE SCHEMA IF NOT EXISTS RAW;
CREATE SCHEMA IF NOT EXISTS STAGING;
CREATE SCHEMA IF NOT EXISTS MARTS;

-- ============================================================================
-- 1. RAW / LANDING LAYER (Bronze)
-- Captures data verbatim from daily feeds for immutable auditability.
-- ============================================================================

CREATE OR REPLACE TABLE RAW.RAW_MEMBER_PROFILES (
    raw_record_id        VARCHAR(36) DEFAULT UUID_STRING(),
    record_type          VARCHAR(10),       -- 'H' for Header, 'D' for Detail
    raw_payload          VARCHAR,           -- Complete unparsed line for audit & replay
    source_file_name     VARCHAR(255) NOT NULL,
    file_row_number      NUMBER(18, 0) NOT NULL,
    ingestion_timestamp  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (DATE_TRUNC('DAY', ingestion_timestamp));

CREATE OR REPLACE TABLE RAW.RAW_REDEMPTION_FEED (
    raw_feed_id          VARCHAR(36) DEFAULT UUID_STRING(),
    payload              VARIANT NOT NULL,  -- Semi-structured JSON blob
    source_file_name     VARCHAR(255) NOT NULL,
    ingestion_timestamp  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (DATE_TRUNC('DAY', ingestion_timestamp));

-- ============================================================================
-- 2. STAGING LAYER (Silver)
-- Cleaned, typed, enriched with derived metrics (Age, Stale_Member), and validated.
-- ============================================================================

CREATE OR REPLACE TABLE STAGING.STG_MEMBER_PROFILES (
    member_id            VARCHAR(18) NOT NULL,
    member_name          VARCHAR(255) NOT NULL,
    enrollment_date      DATE NOT NULL,
    last_flight_date     DATE,
    tier_code            VARCHAR(5),
    agent_name           VARCHAR(255),
    state                VARCHAR(10),
    country              VARCHAR(10) NOT NULL,
    post_code            VARCHAR(10),
    date_of_birth        DATE,
    is_active            VARCHAR(1) DEFAULT 'A',
    
    -- Deliverable 2 Derived Columns
    age                  NUMBER(3, 0),        -- Computed from Date of Birth
    stale_member         VARCHAR(1) NOT NULL, -- 'Y' if days since last_flight_date > 90 or never flown
    
    -- Audit & Lineage Metadata
    days_since_flight    NUMBER(10, 0),
    source_file_name     VARCHAR(255) NOT NULL,
    ingestion_timestamp  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    CONSTRAINT pk_stg_member PRIMARY KEY (member_id)
)
CLUSTER BY (country, member_id);

-- ============================================================================
-- 3. COUNTRY TARGET TABLES (Gold / Marts)
-- Separate physical tables per country as specified in requirements.
-- Deduplicated with 'latest record wins' rule.
-- ============================================================================

CREATE OR REPLACE TABLE MARTS.TABLE_USA (
    member_id            VARCHAR(18) NOT NULL,
    member_name          VARCHAR(255) NOT NULL,
    enrollment_date      DATE NOT NULL,
    last_flight_date     DATE,
    tier_code            VARCHAR(5),
    agent_name           VARCHAR(255),
    state                VARCHAR(10),
    country              VARCHAR(10) DEFAULT 'USA',
    post_code            VARCHAR(10),
    date_of_birth        DATE,
    is_active            VARCHAR(1),
    age                  NUMBER(3, 0),
    stale_member         VARCHAR(1) NOT NULL,
    effective_start_date TIMESTAMP_NTZ NOT NULL,
    is_current           BOOLEAN DEFAULT TRUE,
    last_updated_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_table_usa PRIMARY KEY (member_id)
)
CLUSTER BY (member_id, last_flight_date);

CREATE OR REPLACE TABLE MARTS.TABLE_IND (
    member_id            VARCHAR(18) NOT NULL,
    member_name          VARCHAR(255) NOT NULL,
    enrollment_date      DATE NOT NULL,
    last_flight_date     DATE,
    tier_code            VARCHAR(5),
    agent_name           VARCHAR(255),
    state                VARCHAR(10),
    country              VARCHAR(10) DEFAULT 'IND',
    post_code            VARCHAR(10),
    date_of_birth        DATE,
    is_active            VARCHAR(1),
    age                  NUMBER(3, 0),
    stale_member         VARCHAR(1) NOT NULL,
    effective_start_date TIMESTAMP_NTZ NOT NULL,
    is_current           BOOLEAN DEFAULT TRUE,
    last_updated_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_table_ind PRIMARY KEY (member_id)
)
CLUSTER BY (member_id, last_flight_date);

CREATE OR REPLACE TABLE MARTS.TABLE_CAN (
    member_id            VARCHAR(18) NOT NULL,
    member_name          VARCHAR(255) NOT NULL,
    enrollment_date      DATE NOT NULL,
    last_flight_date     DATE,
    tier_code            VARCHAR(5),
    agent_name           VARCHAR(255),
    state                VARCHAR(10),
    country              VARCHAR(10) DEFAULT 'CAN',
    post_code            VARCHAR(10),
    date_of_birth        DATE,
    is_active            VARCHAR(1),
    age                  NUMBER(3, 0),
    stale_member         VARCHAR(1) NOT NULL,
    effective_start_date TIMESTAMP_NTZ NOT NULL,
    is_current           BOOLEAN DEFAULT TRUE,
    last_updated_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_table_can PRIMARY KEY (member_id)
)
CLUSTER BY (member_id, last_flight_date);

CREATE OR REPLACE TABLE MARTS.TABLE_PHIL (
    member_id            VARCHAR(18) NOT NULL,
    member_name          VARCHAR(255) NOT NULL,
    enrollment_date      DATE NOT NULL,
    last_flight_date     DATE,
    tier_code            VARCHAR(5),
    agent_name           VARCHAR(255),
    state                VARCHAR(10),
    country              VARCHAR(10) DEFAULT 'PHIL',
    post_code            VARCHAR(10),
    date_of_birth        DATE,
    is_active            VARCHAR(1),
    age                  NUMBER(3, 0),
    stale_member         VARCHAR(1) NOT NULL,
    effective_start_date TIMESTAMP_NTZ NOT NULL,
    is_current           BOOLEAN DEFAULT TRUE,
    last_updated_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_table_phil PRIMARY KEY (member_id)
)
CLUSTER BY (member_id, last_flight_date);

CREATE OR REPLACE TABLE MARTS.TABLE_AU (
    member_id            VARCHAR(18) NOT NULL,
    member_name          VARCHAR(255) NOT NULL,
    enrollment_date      DATE NOT NULL,
    last_flight_date     DATE,
    tier_code            VARCHAR(5),
    agent_name           VARCHAR(255),
    state                VARCHAR(10),
    country              VARCHAR(10) DEFAULT 'AU',
    post_code            VARCHAR(10),
    date_of_birth        DATE,
    is_active            VARCHAR(1),
    age                  NUMBER(3, 0),
    stale_member         VARCHAR(1) NOT NULL,
    effective_start_date TIMESTAMP_NTZ NOT NULL,
    is_current           BOOLEAN DEFAULT TRUE,
    last_updated_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_table_au PRIMARY KEY (member_id)
)
CLUSTER BY (member_id, last_flight_date);

-- ============================================================================
-- 4. FLATTENED REDEMPTIONS FACT TABLE (Deliverable 4)
-- ============================================================================

CREATE OR REPLACE TABLE MARTS.FACT_REDEMPTIONS (
    txn_id               VARCHAR(50) NOT NULL,
    member_id            VARCHAR(18) NOT NULL,
    feed_date            DATE NOT NULL,
    txn_date             DATE NOT NULL,
    partner              VARCHAR(100) NOT NULL,
    miles_redeemed       NUMBER(12, 0) NOT NULL,
    status               VARCHAR(30) NOT NULL,
    source_file_name     VARCHAR(255) NOT NULL,
    ingestion_timestamp  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_redemptions PRIMARY KEY (txn_id)
)
CLUSTER BY (txn_date, partner);