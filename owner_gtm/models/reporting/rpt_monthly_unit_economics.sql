/*
    Monthly unit economics by channel — CAC, cost-per-lead, cost-per-win.
    Joins expenses with monthly conversion volumes to show efficiency over time.
    Covers Jan–Jun 2024 (the 6 months with expense data).

    TIMING CAVEAT: This report joins expenses (by month spent) with wins (by month
    closed). A deal that cost marketing dollars in February but closed in April
    attributes the win to April while the cost sits in February. This is standard
    for monthly reporting, but it means any single month's CAC can look distorted
    by cohort timing mismatches. For accurate aggregate CAC, use rpt_cac_ltv_by_channel
    which sums the full 6-month window and avoids this cohort mismatch.
*/

WITH expenses AS (

    SELECT * FROM {{ ref('ops_expenses') }}

),

leads AS (

    SELECT * FROM {{ ref('ops_leads') }}

),

opportunities AS (

    SELECT * FROM {{ ref('ops_opportunities') }}

),

-- Monthly lead volumes by channel
monthly_leads AS (

    SELECT
        DATE_TRUNC('month', form_submission_date)::date AS month,
        channel,
        COUNT(*)                                        AS new_leads,
        SUM(CASE WHEN is_converted THEN 1 ELSE 0 END)   AS converted_leads
    FROM leads
    WHERE form_submission_date IS NOT NULL
    GROUP BY 1, 2

    UNION ALL

    -- Outbound leads don't have form_submission_date, use first contact date
    SELECT
        DATE_TRUNC('month',
            COALESCE(first_sales_call_date, first_text_sent_date, last_sales_activity_date)
        )::date                                         AS month,
        channel,
        COUNT(*)                                        AS new_leads,
        SUM(CASE WHEN is_converted THEN 1 ELSE 0 END)   AS converted_leads
    FROM leads
    WHERE channel = 'Outbound'
        AND COALESCE(first_sales_call_date, first_text_sent_date, last_sales_activity_date) IS NOT NULL
    GROUP BY 1, 2

),

-- Monthly opportunity outcomes by channel
monthly_opps AS (

    SELECT
        DATE_TRUNC('month', close_date)::date           AS month,
        channel,
        COUNT(*)                                        AS closed_deals,
        SUM(CASE WHEN is_closed_won THEN 1 ELSE 0 END)   AS wins,
        SUM(CASE WHEN is_closed_lost THEN 1 ELSE 0 END) AS losses,
        SUM(CASE WHEN is_closed_won THEN predicted_sales_with_owner ELSE 0 END) AS won_predicted_monthly_revenue
    FROM opportunities
    WHERE is_closed
        AND close_date IS NOT NULL
    GROUP BY 1, 2

),

-- Pivot leads into inbound/outbound columns per month
monthly_leads_pivoted AS (

    SELECT
        month,
        SUM(CASE WHEN channel = 'Inbound' THEN new_leads ELSE 0 END)        AS inbound_leads,
        SUM(CASE WHEN channel = 'Outbound' THEN new_leads ELSE 0 END)       AS outbound_leads,
        SUM(CASE WHEN channel = 'Inbound' THEN converted_leads ELSE 0 END)  AS inbound_converted,
        SUM(CASE WHEN channel = 'Outbound' THEN converted_leads ELSE 0 END) AS outbound_converted
    FROM monthly_leads
    GROUP BY 1

),

-- Pivot opps into inbound/outbound columns per month
monthly_opps_pivoted AS (

    SELECT
        month,
        SUM(CASE WHEN channel = 'Inbound' THEN wins ELSE 0 END)            AS inbound_wins,
        SUM(CASE WHEN channel = 'Outbound' THEN wins ELSE 0 END)           AS outbound_wins,
        SUM(CASE WHEN channel = 'Inbound' THEN losses ELSE 0 END)          AS inbound_losses,
        SUM(CASE WHEN channel = 'Outbound' THEN losses ELSE 0 END)         AS outbound_losses,
        SUM(CASE WHEN channel = 'Inbound' THEN won_predicted_monthly_revenue ELSE 0 END)  AS inbound_won_revenue,
        SUM(CASE WHEN channel = 'Outbound' THEN won_predicted_monthly_revenue ELSE 0 END) AS outbound_won_revenue
    FROM monthly_opps
    GROUP BY 1

),

-- Final: join expenses with volumes and calculate unit economics
final AS (

    SELECT
        e.expense_month,

        -- Costs
        e.advertising_spend,
        e.inbound_sales_team_cost,
        e.outbound_sales_team_cost,
        e.inbound_channel_cost,
        e.outbound_channel_cost,
        e.total_gtm_cost,

        -- Volumes
        COALESCE(l.inbound_leads, 0)                AS inbound_leads,
        COALESCE(l.outbound_leads, 0)               AS outbound_leads,
        COALESCE(l.inbound_converted, 0)           AS inbound_converted,
        COALESCE(l.outbound_converted, 0)          AS outbound_converted,
        COALESCE(o.inbound_wins, 0)                 AS inbound_wins,
        COALESCE(o.outbound_wins, 0)                AS outbound_wins,
        COALESCE(o.inbound_losses, 0)              AS inbound_losses,
        COALESCE(o.outbound_losses, 0)             AS outbound_losses,
        COALESCE(o.inbound_won_revenue, 0)         AS inbound_won_predicted_revenue,
        COALESCE(o.outbound_won_revenue, 0)        AS outbound_won_predicted_revenue,

        -- Unit economics: Inbound
        ROUND(e.inbound_channel_cost
            / NULLIF(COALESCE(l.inbound_leads, 0), 0), 2)      AS inbound_cost_per_lead,
        ROUND(e.inbound_channel_cost
            / NULLIF(COALESCE(l.inbound_converted, 0), 0), 2)  AS inbound_cost_per_opportunity,
        ROUND(e.inbound_channel_cost
            / NULLIF(COALESCE(o.inbound_wins, 0), 0), 2)       AS inbound_cac,

        -- Unit economics: Outbound
        ROUND(e.outbound_channel_cost
            / NULLIF(COALESCE(l.outbound_leads, 0), 0), 2)     AS outbound_cost_per_lead,
        ROUND(e.outbound_channel_cost
            / NULLIF(COALESCE(l.outbound_converted, 0), 0), 2) AS outbound_cost_per_opportunity,
        ROUND(e.outbound_channel_cost
            / NULLIF(COALESCE(o.outbound_wins, 0), 0), 2)      AS outbound_cac,

        -- Blended
        ROUND(e.total_gtm_cost
            / NULLIF(COALESCE(o.inbound_wins, 0) + COALESCE(o.outbound_wins, 0), 0), 2) AS blended_cac

    FROM expenses e
    LEFT JOIN monthly_leads_pivoted l
        ON e.expense_month = l.month
    LEFT JOIN monthly_opps_pivoted o
        ON e.expense_month = o.month

)

SELECT * FROM final
ORDER BY expense_month
