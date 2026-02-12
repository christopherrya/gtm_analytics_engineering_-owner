SELECT
    month,
    outbound_sales_team,
    inbound_sales_team
FROM {{ source('gtm_case', 'expenses_salary_and_commissions') }}
