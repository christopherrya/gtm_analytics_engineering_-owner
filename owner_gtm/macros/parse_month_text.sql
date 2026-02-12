{% macro parse_month_text(column_name) %}
-- Parses month text like 'Jan-24' to DATE (2024-01-01)
to_date({{ column_name }}, 'MON-YY')
{% endmacro %}
