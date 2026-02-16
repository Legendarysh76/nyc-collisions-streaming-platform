-- macros/uppercase_strings.sql
--
-- PURPOSE:
--   Generate a SELECT list that:
--     - Uppercases ALL string columns (VARCHAR/TEXT/STRING)
--     - Leaves non-string columns unchanged
--
-- HOW IT WORKS:
--   dbt asks Snowflake for the columns on the source relation,
--   then this macro builds a comma-separated SELECT list.

{% macro uppercase_strings(relation) %}

  {%- set cols = adapter.get_columns_in_relation(relation) -%}

  {%- for col in cols -%}

    {%- set col_name = adapter.quote(col.name) -%}
    {%- set dtype = col.data_type | lower -%}

    {# Treat common Snowflake string types as strings #}
    {%- if 'char' in dtype or 'text' in dtype or 'string' in dtype or 'varchar' in dtype -%}
      UPPER(NULLIF(TRIM({{ col_name }}), '')) AS {{ col_name }}
    {%- else -%}
      {{ col_name }}
    {%- endif -%}

    {%- if not loop.last -%},{{ "\n" }}{%- endif -%}

  {%- endfor -%}

{% endmacro %}
