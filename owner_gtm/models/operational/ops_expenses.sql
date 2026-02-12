/*
    MODEL: ops_expenses
    GRAIN: One row per month (enriched with advertising and salary/commission data)

    PURPOSE: Unified monthly GTM expenses combining advertising spend with
             sales team salary/commissions. Advertising allocated to inbound channel,
             outbound salary allocated to outbound channel.

    UPSTREAM: stg_expenses_advertising, stg_expenses_salary_and_commissions
    
    DOWNSTREAM: rpt_monthly_unit_economics
*/

WITH advertising AS (

    SELECT * FROM {{ ref('stg_expenses_advertising') }}

),

salary AS (

    SELECT * FROM {{ ref('stg_expenses_salary_and_commissions') }}

),

unified AS (

    SELECT
        COALESCE(a.expense_month, s.expense_month)   AS expense_month,
        COALESCE(a.advertising_spend, 0)            AS advertising_spend,
        COALESCE(s.outbound_sales_team_cost, 0)     AS outbound_sales_team_cost,
        COALESCE(s.inbound_sales_team_cost, 0)      AS inbound_sales_team_cost,

        -- Totals
        COALESCE(a.advertising_spend, 0)
            + COALESCE(s.outbound_sales_team_cost, 0)
            + COALESCE(s.inbound_sales_team_cost, 0) AS total_gtm_cost,

        -- Channel-allocated costs
        -- Advertising is assumed inbound (demand gen drives form fills)
        -- Outbound salary is outbound, inbound salary is inbound
        COALESCE(a.advertising_spend, 0)
            + COALESCE(s.inbound_sales_team_cost, 0) AS inbound_channel_cost,

        COALESCE(s.outbound_sales_team_cost, 0)     AS outbound_channel_cost

    FROM advertising a
    FULL OUTER JOIN salary s
        ON a.expense_month = s.expense_month

)

SELECT * FROM unified
