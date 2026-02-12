SELECT DISTINCT
    month,
    advertising
FROM {{ ref('ingress_expenses_advertising') }}
