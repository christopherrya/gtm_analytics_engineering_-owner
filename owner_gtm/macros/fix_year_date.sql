{% macro fix_year_date(column_name) %}
-- Fix year bug: 0020-0024 → 2020-2024 (date only)
to_date(
    '2' || substr(to_varchar({{ column_name }}, 'YYYY-MM-DD'), 2),
    'YYYY-MM-DD'
)
{% endmacro %}
