{% snapshot dim_fuel %}
{{ config(unique_key='fuel', strategy='check', check_cols=['emission_factor_kg_per_mwh']) }}
-- SCD Type 2: when an eGRID factor changes (edit the seed, `dbt seed`, `dbt snapshot`) the old row is closed
-- (dbt_valid_to) and a new row opened.
select fuel, emission_factor_kg_per_mwh, source_note from {{ ref('egrid_factors') }}
{% endsnapshot %}
