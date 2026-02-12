{% macro fix_year_timestamp(column_name) %}
-- Fix year bug: 0020-0024 → 2020-2024 (preserves time component)
to_timestamp(
    '2' || substr(to_varchar({{ column_name }}, 'YYYY-MM-DD HH24:MI:SS.FF3'), 2),
    'YYYY-MM-DD HH24:MI:SS.FF3'
)
{% endmacro %}
