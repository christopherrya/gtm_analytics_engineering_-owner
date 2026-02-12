/*
    CAC and LTV by Channel

    LTV = (monthly subscription + 5% take rate on predicted monthly sales) x customer lifetime months
    CAC = total channel cost / deals won

    NOTE: We don't have churn data, so customer lifetime is parameterized.
          LTV is shown at 12, 24, and 36 month horizons to illustrate sensitivity.
          Expense data covers Jan-Jun 2024 only, so costs reflect that 6-month window.
*/

WITH won_deals AS (

    SELECT
        channel,
        COUNT(*)                                    AS deals_won,
        ROUND(AVG(monthly_subscription), 2)         AS avg_monthly_subscription,
        ROUND(AVG(monthly_take_rate), 2)            AS avg_monthly_take_rate,
        ROUND(AVG(monthly_revenue), 2)              AS avg_monthly_revenue
    FROM {{ ref('ops_opportunities') }}
    WHERE is_closed_won
    GROUP BY channel

),

channel_costs AS (

    SELECT 'Inbound' AS channel, SUM(inbound_channel_cost) AS total_cost
    FROM {{ ref('ops_expenses') }}
    UNION ALL
    SELECT 'Outbound', SUM(outbound_channel_cost)
    FROM {{ ref('ops_expenses') }}

),

with_cac AS (

    SELECT
        w.channel,
        w.deals_won,
        c.total_cost                                AS total_cost_6mo,
        ROUND(c.total_cost / w.deals_won, 2)        AS cac,
        w.avg_monthly_subscription,
        w.avg_monthly_take_rate,
        w.avg_monthly_revenue
    FROM won_deals w
    INNER JOIN channel_costs c ON w.channel = c.channel

),

final AS (

    SELECT
        channel,
        deals_won,

        -- Cost breakdown
        total_cost_6mo,
        cac,

        -- Revenue per deal
        avg_monthly_subscription,
        avg_monthly_take_rate,
        avg_monthly_revenue,

        -- LTV at different lifetime horizons
        ROUND(avg_monthly_revenue * 12, 2)          AS avg_ltv_12mo,
        ROUND(avg_monthly_revenue * 24, 2)          AS avg_ltv_24mo,
        ROUND(avg_monthly_revenue * 36, 2)          AS avg_ltv_36mo,

        -- LTV:CAC ratios
        ROUND((avg_monthly_revenue * 12) / NULLIF(cac, 0), 2) AS ltv_to_cac_12mo,
        ROUND((avg_monthly_revenue * 24) / NULLIF(cac, 0), 2) AS ltv_to_cac_24mo,
        ROUND((avg_monthly_revenue * 36) / NULLIF(cac, 0), 2) AS ltv_to_cac_36mo

    FROM with_cac

)

SELECT * FROM final
ORDER BY channel
