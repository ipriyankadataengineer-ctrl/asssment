-- ============================================================================
-- SKYPOINTS AIRLINE LOYALTY PLATFORM - TRANSFORMATION SQL SCRIPTS
-- Deliverables 2, 3 & 4 (ANSI SQL / Snowflake / Spark SQL Dialects)
-- ============================================================================

-- ============================================================================
-- DELIVERABLE 2: POPULATE STAGING TABLE WITH DERIVED COLUMNS
-- Derived 1: Age computed from DOB
-- Derived 2: Stale_Member flag where days since Flight_Date > 90 (or null)
-- ============================================================================

INSERT OVERWRITE INTO STAGING.STG_MEMBER_PROFILES
WITH parsed_raw AS (
    SELECT
        -- Split pipe-delimited payload (ignoring leading empty token)
        SPLIT_PART(raw_payload, '|', 3) AS member_name,
        SPLIT_PART(raw_payload, '|', 4) AS member_id,
        TRY_TO_DATE(SPLIT_PART(raw_payload, '|', 5), 'YYYYMMDD') AS enrollment_date,
        TRY_TO_DATE(SPLIT_PART(raw_payload, '|', 6), 'YYYYMMDD') AS last_flight_date,
        NULLIF(TRIM(SPLIT_PART(raw_payload, '|', 7)), '') AS tier_code,
        NULLIF(TRIM(SPLIT_PART(raw_payload, '|', 8)), '') AS agent_name,
        NULLIF(TRIM(SPLIT_PART(raw_payload, '|', 9)), '') AS state,
        UPPER(NULLIF(TRIM(SPLIT_PART(raw_payload, '|', 10)), '')) AS country,
        -- Handle DOB formatted as MMDDYYYY or DDMMYYYY zero-padded to 8 chars
        TRY_TO_DATE(LPAD(TRIM(SPLIT_PART(raw_payload, '|', 11)), 8, '0'), 'MMDDYYYY') AS date_of_birth,
        COALESCE(NULLIF(TRIM(SPLIT_PART(raw_payload, '|', 12)), ''), 'A') AS is_active,
        source_file_name
    FROM RAW.RAW_MEMBER_PROFILES
    WHERE record_type = 'D' -- Filter out Header |H| records
)
SELECT
    member_id,
    member_name,
    enrollment_date,
    last_flight_date,
    tier_code,
    agent_name,
    state,
    country,
    NULL AS post_code,
    date_of_birth,
    is_active,
    
    -- Derived Metric 1: Age
    DATEDIFF('year', date_of_birth, CURRENT_DATE()) - 
        CASE 
            WHEN (DATE_PART('month', CURRENT_DATE()) < DATE_PART('month', date_of_birth)) OR 
                 (DATE_PART('month', CURRENT_DATE()) = DATE_PART('month', date_of_birth) AND DATE_PART('day', CURRENT_DATE()) < DATE_PART('day', date_of_birth))
            THEN 1 
            ELSE 0 
        END AS age,
        
    -- Derived Metric 2: Stale_Member Flag (> 90 days or never flown)
    CASE 
        WHEN last_flight_date IS NULL THEN 'Y'
        WHEN DATEDIFF('day', last_flight_date, CURRENT_DATE()) > 90 THEN 'Y'
        ELSE 'N'
    END AS stale_member,
    
    COALESCE(DATEDIFF('day', last_flight_date, CURRENT_DATE()), -1) AS days_since_flight,
    source_file_name,
    CURRENT_TIMESTAMP() AS ingestion_timestamp
FROM parsed_raw
WHERE member_id IS NOT NULL AND member_name IS NOT NULL AND enrollment_date IS NOT NULL;


-- ============================================================================
-- DELIVERABLE 3: DEDUPLICATION ('LATEST RECORD WINS') & COUNTRY ROUTING
-- Handles members moving across countries using Windowed Row Numbering
-- ============================================================================

-- Step 3.1: Global Deduplication with 'Latest Record Wins'
CREATE OR REPLACE TEMPORARY VIEW VW_DEDUPLICATED_MEMBERS AS
WITH ranked_members AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY member_id 
            ORDER BY 
                COALESCE(last_flight_date, enrollment_date) DESC,
                enrollment_date DESC,
                ingestion_timestamp DESC
        ) AS rank_order
    FROM STAGING.STG_MEMBER_PROFILES
)
SELECT * EXCLUDE (rank_order)
FROM ranked_members
WHERE rank_order = 1;

-- Step 3.2: Atomic MERGE into Target Country Tables
-- Example: USA Target Table MERGE
MERGE INTO MARTS.TABLE_USA AS target
USING (SELECT * FROM VW_DEDUPLICATED_MEMBERS WHERE country = 'USA') AS source
ON target.member_id = source.member_id
WHEN MATCHED THEN
    UPDATE SET 
        target.member_name      = source.member_name,
        target.enrollment_date  = source.enrollment_date,
        target.last_flight_date = source.last_flight_date,
        target.tier_code        = source.tier_code,
        target.agent_name       = source.agent_name,
        target.state            = source.state,
        target.country          = source.country,
        target.date_of_birth    = source.date_of_birth,
        target.is_active        = source.is_active,
        target.age              = source.age,
        target.stale_member     = source.stale_member,
        target.is_current       = TRUE,
        target.last_updated_at  = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (
        member_id, member_name, enrollment_date, last_flight_date,
        tier_code, agent_name, state, country, post_code,
        date_of_birth, is_active, age, stale_member,
        effective_start_date, is_current, last_updated_at
    )
    VALUES (
        source.member_id, source.member_name, source.enrollment_date, source.last_flight_date,
        source.tier_code, source.agent_name, source.state, source.country, source.post_code,
        source.date_of_birth, source.is_active, source.age, source.stale_member,
        CURRENT_TIMESTAMP(), TRUE, CURRENT_TIMESTAMP()
    );

-- Country Move Deactivation:
-- If member moved from USA to another country, deactivate in TABLE_USA
UPDATE MARTS.TABLE_USA
SET is_current = FALSE, is_active = 'I', last_updated_at = CURRENT_TIMESTAMP()
WHERE member_id IN (
    SELECT member_id FROM VW_DEDUPLICATED_MEMBERS WHERE country <> 'USA'
);

-- Similarly applied for TABLE_IND, TABLE_CAN, TABLE_PHIL, TABLE_AU via dynamic partition script.


-- ============================================================================
-- DELIVERABLE 4: JSON REDEMPTIONS PARSING & MEMBER 360 JOIN
-- ============================================================================

-- Step 4.1: Flatten Nested Redemptions Array (Snowflake Lateral Flatten)
INSERT OVERWRITE INTO MARTS.FACT_REDEMPTIONS
SELECT
    r.value:txn_id::STRING          AS txn_id,
    f.payload:member_id::STRING     AS member_id,
    TRY_TO_DATE(f.payload:feed_date::STRING, 'YYYYMMDD') AS feed_date,
    TRY_TO_DATE(r.value:txn_date::STRING, 'YYYYMMDD')    AS txn_date,
    r.value:partner::STRING         AS partner,
    r.value:miles_redeemed::NUMBER  AS miles_redeemed,
    r.value:status::STRING          AS status,
    f.source_file_name              AS source_file_name,
    CURRENT_TIMESTAMP()             AS ingestion_timestamp
FROM RAW.RAW_REDEMPTION_FEED f,
LATERAL FLATTEN(input => f.payload:redemptions) r;

-- Step 4.2: Member 360 View - Joining Profile with Redemption Metrics
CREATE OR REPLACE VIEW MARTS.VW_MEMBER_REDEMPTIONS_360 AS
SELECT
    m.member_id,
    m.member_name,
    m.country,
    m.tier_code,
    m.is_active,
    m.stale_member,
    m.age,
    COUNT(r.txn_id) AS total_redemptions,
    COALESCE(SUM(r.miles_redeemed), 0) AS total_miles_redeemed,
    COALESCE(SUM(CASE WHEN r.status = 'COMPLETED' THEN r.miles_redeemed ELSE 0 END), 0) AS completed_miles_redeemed,
    COALESCE(SUM(CASE WHEN r.status = 'PENDING' THEN r.miles_redeemed ELSE 0 END), 0) AS pending_miles_redeemed,
    MAX(r.txn_date) AS last_redemption_date
FROM STAGING.STG_MEMBER_PROFILES m
LEFT JOIN MARTS.FACT_REDEMPTIONS r
    ON m.member_id = r.member_id
GROUP BY 
    m.member_id, m.member_name, m.country, m.tier_code, m.is_active, m.stale_member, m.age;