-- ==========================================================
-- MODEL: crash_events
--
-- OUTPUT:
--   GOLD.CRASH_EVENTS
--
-- PURPOSE:
--   This is the main analytics-ready crash table.
--
--   Each row represents ONE crash event, enriched with:
--     - proper crash_date (DATE)
--     - crash_hour, weekday, weekend flag
--     - injury / fatality flags
--     - severity_score (for ranking serious crashes)
--     - location_key (stable hash for grouping by location)
--
-- SOURCE:
--   SILVER.FACT_CRASHES (dbt model: fact_crashes)
--
-- WHY THIS EXISTS:
--   Silver holds cleaned raw data.
--   Gold adds business meaning so dashboards and analysis are simple.
-- ==========================================================

{{ config(materialized='table') }}

WITH

-- ----------------------------------------------------------
-- STEP 1: Read cleaned Silver crashes
-- ----------------------------------------------------------
silver_crash_rows AS (

    SELECT *
    FROM {{ ref('fact_crashes') }}

),

-- ----------------------------------------------------------
-- STEP 2: Add time-based features for analysis
--   - convert crash_date to DATE
--   - extract crash_hour
--   - derive weekday + weekend flag
-- ----------------------------------------------------------
crashes_with_time_dimensions AS (

    SELECT
        "collision_id",

        TO_DATE(TRY_TO_TIMESTAMP_NTZ("crash_date")) AS "crash_date",

        "crash_time",
        DATE_PART('hour', TRY_TO_TIME("crash_time")) AS "crash_hour",

        DAYOFWEEKISO(TO_DATE(TRY_TO_TIMESTAMP_NTZ("crash_date"))) AS "day_of_week_iso",
        TO_CHAR(TO_DATE(TRY_TO_TIMESTAMP_NTZ("crash_date")), 'DY') AS "day_name",

        IFF(
            DAYOFWEEKISO(TO_DATE(TRY_TO_TIMESTAMP_NTZ("crash_date"))) IN (6, 7),
            TRUE,
            FALSE
        ) AS "is_weekend",

        -- Location fields
        "borough",
        "zip_code",
        "latitude",
        "longitude",
        "location",
        "on_street_name",
        "cross_street_name",
        "off_street_name",

        -- Vehicle + contributing factor fields
        "vehicle_type_code1",
        "vehicle_type_code2",
        "vehicle_type_code_3",
        "vehicle_type_code_4",
        "vehicle_type_code_5",

        "contributing_factor_vehicle_1",
        "contributing_factor_vehicle_2",
        "contributing_factor_vehicle_3",
        "contributing_factor_vehicle_4",
        "contributing_factor_vehicle_5",

        -- Injury / fatality measures
        "number_of_persons_injured",
        "number_of_persons_killed",
        "number_of_pedestrians_injured",
        "number_of_pedestrians_killed",
        "number_of_cyclist_injured",
        "number_of_cyclist_killed",
        "number_of_motorist_injured",
        "number_of_motorist_killed"

    FROM silver_crash_rows
),

-- ----------------------------------------------------------
-- STEP 3: Add severity indicators
--   - flags for injury/fatality
--   - severity_score for ranking worst crashes
-- ----------------------------------------------------------
crashes_with_severity AS (

    SELECT
        *,

        IFF(COALESCE("number_of_persons_injured", 0) > 0, TRUE, FALSE) AS "has_injury",
        IFF(COALESCE("number_of_persons_killed", 0) > 0, TRUE, FALSE)  AS "has_fatality",

        -- Deaths weighted heavier than injuries
        (COALESCE("number_of_persons_killed", 0) * 10)
          + COALESCE("number_of_persons_injured", 0) AS "severity_score"

    FROM crashes_with_time_dimensions
),

-- ----------------------------------------------------------
-- STEP 4: Create a stable location_key
--   This lets us group crashes by physical location consistently
-- ----------------------------------------------------------
final_crash_events AS (

    SELECT
        MD5_HEX(
            COALESCE("borough", '') || '|' ||
            COALESCE(TO_VARCHAR("zip_code"), '') || '|' ||
            COALESCE("on_street_name", '') || '|' ||
            COALESCE("cross_street_name", '') || '|' ||
            COALESCE("off_street_name", '') || '|' ||
            COALESCE(TO_VARCHAR("latitude"), '') || '|' ||
            COALESCE(TO_VARCHAR("longitude"), '')
        ) AS "location_key",

        CURRENT_TIMESTAMP() AS "gold_loaded_at",

        *

    FROM crashes_with_severity
)

SELECT *
FROM final_crash_events
