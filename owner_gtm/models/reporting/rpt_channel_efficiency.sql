/*
    MODEL: rpt_channel_efficiency
    GRAIN: One row per channel × month
    PURPOSE: Monthly channel efficiency tracking with rolling averages, MoM deltas,
             threshold flags, and channel mix percentages. Powers the Channel Efficiency
             Monitor dashboard frontend.
    UPSTREAM: rpt_monthly_unit_economics
    DOWNSTREAM: Channel Efficiency Monitor Dashboard

    KEY FEATURES:
    - 3-month rolling CAC to smooth single-month noise
    - Month-over-month deltas for CAC, conversion rate, and deal volume
    - Threshold flags for CAC and conversion rate anomalies
    - Channel mix: each channel's share of total spend and wins per month
    - Efficiency score combining multiple signals

    NOTES:
    - Data covers Jan–Jun 2024 (6 months). 3-month rolling windows produce
      4 usable data points (Mar–Jun). This is a tight window but sufficient
      to show directional trends.
    - CAC thresholds are set at $2,000 (warning) and $3,000 (critical) based
      on the aggregate CAC analysis showing Inbound ~$1,227 and Outbound ~$2,114.
    - Conversion rate floor is set at 15% based on the overall ~26% win rate
      with a safety margin for channel-level variance.
    - The timing caveat from rpt_monthly_unit_economics applies here too:
      costs are attributed to the month spent, wins to the month closed.
      The 3-month rolling window partially mitigates this cohort mismatch.
*/

WITH monthly_base AS (

    SELECT * FROM {{ ref('rpt_monthly_unit_economics') }}

),

unpivoted AS (

    SELECT
        expense_month,
        'Inbound' AS channel,
        inbound_leads AS new_leads,
        inbound_converted AS converted_leads,
        inbound_wins AS deals_won,
        inbound_losses AS deals_lost,
        inbound_channel_cost AS total_channel_cost,
        inbound_cac AS monthly_cac,
        inbound_won_predicted_revenue AS total_monthly_revenue_won,
        ROUND(inbound_wins * 100.0 / NULLIF(inbound_converted, 0), 2) AS lead_to_win_rate_pct
    FROM monthly_base

    UNION ALL

    SELECT
        expense_month,
        'Outbound' AS channel,
        outbound_leads AS new_leads,
        outbound_converted AS converted_leads,
        outbound_wins AS deals_won,
        outbound_losses AS deals_lost,
        outbound_channel_cost AS total_channel_cost,
        outbound_cac AS monthly_cac,
        outbound_won_predicted_revenue AS total_monthly_revenue_won,
        ROUND(outbound_wins * 100.0 / NULLIF(outbound_converted, 0), 2) AS lead_to_win_rate_pct
    FROM monthly_base

),

with_rolling AS (

    SELECT
        u.*,

        ROUND(AVG(u.monthly_cac) OVER (
            PARTITION BY u.channel
            ORDER BY u.expense_month
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ), 2) AS rolling_3mo_cac,

        ROUND(AVG(u.lead_to_win_rate_pct) OVER (
            PARTITION BY u.channel
            ORDER BY u.expense_month
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ), 2) AS rolling_3mo_conversion_rate,

        ROUND(AVG(u.deals_won) OVER (
            PARTITION BY u.channel
            ORDER BY u.expense_month
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ), 1) AS rolling_3mo_deals_won,

        COUNT(*) OVER (
            PARTITION BY u.channel
            ORDER BY u.expense_month
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS rolling_window_months,

        LAG(u.monthly_cac, 1) OVER (
            PARTITION BY u.channel
            ORDER BY u.expense_month
        ) AS prev_month_cac,

        LAG(u.lead_to_win_rate_pct, 1) OVER (
            PARTITION BY u.channel
            ORDER BY u.expense_month
        ) AS prev_month_conversion_rate,

        LAG(u.deals_won, 1) OVER (
            PARTITION BY u.channel
            ORDER BY u.expense_month
        ) AS prev_month_deals_won,

        LAG(u.new_leads, 1) OVER (
            PARTITION BY u.channel
            ORDER BY u.expense_month
        ) AS prev_month_new_leads

    FROM unpivoted u

),

with_deltas AS (

    SELECT
        r.*,

        ROUND(r.monthly_cac - r.prev_month_cac, 2) AS cac_mom_delta,
        ROUND(r.lead_to_win_rate_pct - r.prev_month_conversion_rate, 2) AS conversion_rate_mom_delta,
        r.deals_won - r.prev_month_deals_won AS deals_won_mom_delta,
        r.new_leads - r.prev_month_new_leads AS new_leads_mom_delta,

        ROUND((r.monthly_cac - r.prev_month_cac) * 100.0
            / NULLIF(r.prev_month_cac, 0), 1) AS cac_mom_pct_change,
        ROUND((r.lead_to_win_rate_pct - r.prev_month_conversion_rate)
            * 100.0 / NULLIF(r.prev_month_conversion_rate, 0), 1) AS conversion_rate_mom_pct_change

    FROM with_rolling r

),

monthly_totals AS (

    SELECT
        expense_month,
        SUM(total_channel_cost) AS total_spend_all_channels,
        SUM(deals_won) AS total_wins_all_channels,
        SUM(new_leads) AS total_leads_all_channels,
        SUM(total_monthly_revenue_won) AS total_revenue_all_channels
    FROM unpivoted
    GROUP BY 1

),

final AS (

    SELECT
        d.channel,
        d.expense_month,

        d.new_leads,
        d.converted_leads,
        d.deals_won,
        d.deals_lost,
        d.total_channel_cost,
        d.monthly_cac,
        d.lead_to_win_rate_pct,
        d.total_monthly_revenue_won,

        d.rolling_3mo_cac,
        d.rolling_3mo_conversion_rate,
        d.rolling_3mo_deals_won,
        (d.rolling_window_months >= 3) AS is_full_rolling_window,

        d.cac_mom_delta,
        d.cac_mom_pct_change,
        d.conversion_rate_mom_delta,
        d.conversion_rate_mom_pct_change,
        d.deals_won_mom_delta,
        d.new_leads_mom_delta,

        ROUND(d.total_channel_cost * 100.0
            / NULLIF(t.total_spend_all_channels, 0), 1) AS pct_of_total_spend,
        ROUND(d.deals_won * 100.0
            / NULLIF(t.total_wins_all_channels, 0), 1) AS pct_of_total_wins,
        ROUND(d.new_leads * 100.0
            / NULLIF(t.total_leads_all_channels, 0), 1) AS pct_of_total_leads,
        ROUND(d.total_monthly_revenue_won * 100.0
            / NULLIF(t.total_revenue_all_channels, 0), 1) AS pct_of_total_revenue,

        CASE
            WHEN d.monthly_cac > 3000 THEN 'Critical'
            WHEN d.monthly_cac > 2000 THEN 'Warning'
            ELSE 'Healthy'
        END AS cac_threshold_flag,

        CASE
            WHEN d.lead_to_win_rate_pct < 10 THEN 'Critical'
            WHEN d.lead_to_win_rate_pct < 15 THEN 'Warning'
            ELSE 'Healthy'
        END AS conversion_threshold_flag,

        CASE
            WHEN d.rolling_window_months < 3 THEN 'Insufficient Data'
            WHEN d.monthly_cac > d.rolling_3mo_cac * 1.2 THEN 'Deteriorating'
            WHEN d.monthly_cac < d.rolling_3mo_cac * 0.8 THEN 'Improving'
            ELSE 'Stable'
        END AS cac_trend,

        CASE
            WHEN d.deals_won = 0 THEN NULL
            ELSE ROUND(
                d.monthly_cac / NULLIF(d.lead_to_win_rate_pct, 0) * 100.0, 2
            )
        END AS efficiency_score

    FROM with_deltas d
    LEFT JOIN monthly_totals t
        ON d.expense_month = t.expense_month

)

SELECT * FROM final
ORDER BY channel, expense_month
