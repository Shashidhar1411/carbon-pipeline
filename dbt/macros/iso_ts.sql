{% macro iso_ts(expr) %}to_timestamp_ntz(({{ expr }})::string, 'YYYY-MM-DD"T"HH24:MI:SS"Z"'){% endmacro %}
