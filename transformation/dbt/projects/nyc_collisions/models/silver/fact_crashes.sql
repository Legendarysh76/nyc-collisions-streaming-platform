-- models/silver/fact_crashes.sql
--
-- MODEL NAME:
--   fact_crashes  -> Snowflake table will be named: FACT_CRASHES
--
-- PURPOSE (Silver Layer):
--   Create the SILVER version of the BRONZE FACT_CRASHES table.
--
-- CLEANING RULES (ONLY):
--   1) For every STRING column: make it UPPERCASE
--      - TRIM spaces first (so " Queens " becomes "QUEENS")
--      - Convert empty strings to NULL (so "" becomes NULL)
--   2) For every NON-STRING column: leave it unchanged
--   3) Add an audit timestamp column so we know when dbt built this table
--
-- WHY WE DO THIS:
--   Uppercasing strings removes inconsistent casing in analytics
--   (e.g., "Queens" vs "QUEENS" vs "queens"), which prevents messy grouping.

{# -------------------------------------------------------------------------
   1) Identify the BRONZE table we are reading from
      - source('nyc_collisions','FACT_CRASHES') points to your BRONZE schema,
        based on your sources.yml definition.
   ------------------------------------------------------------------------- #}
{% set bronze_fact_crashes_relation = source('nyc_collisions', 'FACT_CRASHES') %}


WITH
/* --------------------------------------------------------------------------
   STEP A: Read the Bronze table as-is
   - This is the "raw input" into Silver.
   - We keep this step separate so the logic reads clearly.
   -------------------------------------------------------------------------- */
bronze_fact_crashes_rows AS (

    SELECT
        *
    FROM {{ bronze_fact_crashes_relation }}

),

/* --------------------------------------------------------------------------
   STEP B: Apply the Silver “standardization” rule
   - Use a macro to:
       * uppercase all string columns
       * keep all non-string columns unchanged
   - This avoids manually writing UPPER(...) for 30+ columns.
   -------------------------------------------------------------------------- */
silver_fact_crashes_standardized AS (

    SELECT

        -- Uppercase ALL string columns automatically, keep the rest unchanged
        {{ uppercase_strings(bronze_fact_crashes_relation) }},

        -- Add an audit column (useful for debugging refresh timing)
        CURRENT_TIMESTAMP() AS dbt_loaded_at

    FROM bronze_fact_crashes_rows

)

-- --------------------------------------------------------------------------
-- STEP C: Final output
-- This SELECT is what dbt materializes into: SILVER.FACT_CRASHES
-- --------------------------------------------------------------------------
SELECT
    *
FROM silver_fact_crashes_standardized
