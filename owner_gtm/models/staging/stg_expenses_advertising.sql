WITH source AS (

    SELECT
        month,
        advertising
    FROM {{ ref('raw_expenses_advertising') }}

)

select
    {{ parse_month_text('month') }}     as expense_month,
    {{ parse_currency('advertising') }} as advertising_spend
from source
