{% test unique_interval_fuel(model) %}
select interval_utc, fuel, count(*) n from {{ model }} group by 1, 2 having count(*) > 1
{% endtest %}

{% test plausible_intensity(model, column_name) %}
select * from {{ model }} where {{ column_name }} < 0 or {{ column_name }} > 1200
{% endtest %}
