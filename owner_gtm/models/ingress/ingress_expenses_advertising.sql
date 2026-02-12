SELECT
    month,
    advertising
FROM {{ source('gtm_case', 'expenses_advertising') }}
