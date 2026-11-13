{% macro generate_schema_name(custom_schema_name, node) -%}
    {#- use the schema named in dbt_project.yml as-is (STAGING, MARTS) instead of dbt's default ANALYTICS_STAGING -#}
    {%- if custom_schema_name is none -%}{{ target.schema }}{%- else -%}{{ custom_schema_name | trim | upper }}{%- endif -%}
{%- endmacro %}
