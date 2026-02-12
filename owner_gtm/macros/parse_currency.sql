{% macro parse_currency(column_name) %}
-- Parses currency text like 'US$    55 779,40' to numeric 55779.40
try_to_double(
    replace(
        regexp_replace(
            replace({{ column_name }}, 'US$', ''),
            '[^0-9,]', ''
        ),
        ',', '.'
    )
)
{% endmacro %}
