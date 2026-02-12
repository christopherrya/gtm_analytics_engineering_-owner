SELECT DISTINCT
    month,
    outbound_sales_team,
    inbound_sales_team
FROM {{ ref('ingress_expenses_salary_and_commissions') }}
