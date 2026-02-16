{% macro generate_schema_name(custom_schema_name, node) -%}
  {# 
    Purpose:
    - If a model has +schema configured (silver/gold), use it EXACTLY.
    - Otherwise fall back to the profile target schema.
  #}

  {% if custom_schema_name is none %}
    {{ target.schema }}
  {% else %}
    {{ custom_schema_name | trim }}
  {% endif %}
{%- endmacro %}
