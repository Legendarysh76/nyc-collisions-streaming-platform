-- ==========================================================
-- MODEL: crash_daily_cause_metrics
--
-- OUTPUT:
--   GOLD.CRASH_DAILY_CAUSE_METRICS
--
-- PURPOSE:
--   Daily rollup of crashes by:
--     - crash_date
--     - borough
--     - primary contributing factor
--
-- METRICS:
--   - crash_count
--   - total_persons_injured
--   - total_persons_killed
--
-- WHY THIS EXISTS:
--   Makes it easy to analyze trends like:
--     "Driver Inattention crashes over time"
--     "Top causes by borough"
-- ==========================================================

{{ config(materialized='table') }}

WITH

-- ----------------------------------------------------------
-- STEP 1: Read enriched Gold crash events
-- ----------------------------------------------------------
gold_crash_events AS (

    SELECT *
    FROM {{ ref('crash_events') }}

),

-- ----------------------------------------------------------
-- STEP 2: Normalize contributing factors
--   Remove UNKNOWN / UNSPECIFIED so they don't pollute insights
-- ----------------------------------------------------------
crashes_with_clean_cause AS (

    SELECT
        "crash_date",
        "borough",

        CASE
            WHEN "contributing_factor_vehicle_1" IS NULL THEN NULL
            WHEN "contributing_factor_vehicle_1" IN ('UNSPECIFIED', 'UNKNOWN', '') THEN NULL
            ELSE "contributing_factor_vehicle_1"
        END AS "primary_contributing_factor",

        "number_of_persons_injured",
        "number_of_persons_killed"

    FROM gold_crash_events
    WHERE "crash_date" IS NOT NULL
),

-- ----------------------------------------------------------
-- STEP 3: Aggregate into daily metrics
-- ----------------------------------------------------------
daily_metrics AS (

    SELECT
        "crash_date",
        "borough",
        "primary_contributing_factor",

        COUNT(*) AS "crash_count",
        SUM(COALESCE("number_of_persons_injured", 0)) AS "total_persons_injured",
        SUM(COALESCE("number_of_persons_killed", 0))  AS "total_persons_killed",

        CURRENT_TIMESTAMP() AS "gold_loaded_at"

    FROM crashes_with_clean_cause
    GROUP BY
        "crash_date",
        "borough",
        "primary_contributing_factor"
)

SELECT *
FROM daily_metrics
