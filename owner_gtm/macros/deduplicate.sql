{% macro deduplicate(relation, partition_by, order_by=none) %}

{%- set order_clause = order_by if order_by else partition_by -%}

(
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY {{ partition_by }}
                ORDER BY {{ order_clause }}
            ) AS _row_num
        FROM {{ relation }}
    )
    WHERE _row_num = 1
)

{% endmacro %}
