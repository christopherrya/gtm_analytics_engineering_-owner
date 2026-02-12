WITH source AS (

    SELECT
        month,
        outbound_sales_team,
        inbound_sales_team
    FROM {{ ref('raw_expenses_salary_and_commissions') }}

)

select
    {{ parse_month_text('month') }}              as expense_month,
    {{ parse_currency('outbound_sales_team') }}  as outbound_sales_team_cost,
    {{ parse_currency('inbound_sales_team') }}   as inbound_sales_team_cost
from source
