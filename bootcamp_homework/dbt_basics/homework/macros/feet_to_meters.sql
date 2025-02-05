{% macro feet_to_meters(column_name, scale=2) %}
    ({{ column_name }} * 0.3048)::numeric(16, {{ scale }})
{% endmacro %}