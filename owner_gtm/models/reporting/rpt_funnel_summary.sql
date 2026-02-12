/*
    Funnel summary by channel — the core view for GTM decision-making.
    One row per channel (Inbound / Outbound) with full funnel metrics.
*/

WITH leads AS (

    SELECT * FROM {{ ref('ops_leads') }}

),

opportunities AS (

    SELECT * FROM {{ ref('ops_opportunities') }}

),

-- Lead-level funnel by channel
lead_metrics AS (

    SELECT
        channel,
        COUNT(*)                                                          AS total_leads,
        SUM(CASE WHEN is_converted THEN 1 ELSE 0 END)                    AS converted_leads,
        SUM(CASE WHEN status = 'Working' THEN 1 ELSE 0 END)               AS working_leads,
        SUM(CASE WHEN status = 'New' THEN 1 ELSE 0 END)                  AS new_leads,
        SUM(CASE WHEN status = 'Disqualified' THEN 1 ELSE 0 END)          AS disqualified_leads,
        SUM(CASE WHEN status = 'Incorrect Contact Data' THEN 1 ELSE 0 END) AS bad_contact_leads,
        SUM(CASE WHEN connected_with_decision_maker THEN 1 ELSE 0 END)   AS dm_connected_leads,

        -- Speed to lead (inbound only)
        ROUND(AVG(speed_to_lead_minutes), 1)                             AS avg_speed_to_lead_min,
        ROUND(MEDIAN(speed_to_lead_minutes), 1)                          AS median_speed_to_lead_min,

        -- Prospect profile
        ROUND(AVG(predicted_sales_with_owner), 0)                        AS avg_predicted_sales,
        ROUND(AVG(total_touch_count), 1)                                 AS avg_touches_per_lead,
        ROUND(AVG(location_count), 1)                                    AS avg_locations

    FROM leads
    GROUP BY channel

),

-- Opportunity-level metrics by channel
opp_metrics AS (

    SELECT
        channel,
        COUNT(*)                                                          AS total_opportunities,
        SUM(CASE WHEN is_closed_won THEN 1 ELSE 0 END)                   AS closed_won,
        SUM(CASE WHEN is_closed_lost THEN 1 ELSE 0 END)                  AS closed_lost,
        SUM(CASE WHEN NOT is_closed THEN 1 ELSE 0 END)                   AS open_pipeline,
        SUM(CASE WHEN demo_held THEN 1 ELSE 0 END)                       AS demos_held,
        SUM(CASE WHEN loss_category = 'Pre-Demo Loss' THEN 1 ELSE 0 END) AS pre_demo_losses,
        SUM(CASE WHEN loss_category = 'Process/Follow-Up Loss' THEN 1 ELSE 0 END) AS process_losses,
        SUM(CASE WHEN loss_category = 'Competitive Loss' THEN 1 ELSE 0 END) AS competitive_losses,
        SUM(CASE WHEN loss_category = 'Product Fit Loss' THEN 1 ELSE 0 END) AS product_fit_losses,

        ROUND(AVG(CASE WHEN is_closed_won THEN days_to_close END), 1)     AS avg_days_to_close_won,
        ROUND(AVG(CASE WHEN is_closed_won THEN predicted_sales_with_owner END), 0) AS avg_won_predicted_sales,
        ROUND(AVG(CASE WHEN is_closed_won THEN total_touch_count END), 1) AS avg_touches_to_win

    FROM opportunities
    GROUP BY channel

),

-- Final join with calculated rates
funnel AS (

    SELECT
        l.channel,

        -- Lead funnel
        l.total_leads,
        l.converted_leads,
        ROUND(l.converted_leads * 100.0 / NULLIF(l.total_leads, 0), 2)   AS lead_to_opp_rate_pct,

        -- Opportunity funnel
        o.total_opportunities,
        o.demos_held,
        ROUND(o.demos_held * 100.0 / NULLIF(o.total_opportunities, 0), 2) AS demo_rate_pct,
        o.closed_won,
        o.closed_lost,
        o.open_pipeline,
        ROUND(o.closed_won * 100.0
            / NULLIF(o.closed_won + o.closed_lost, 0), 2)                 AS win_rate_pct,
        ROUND(o.closed_won * 100.0 / NULLIF(o.demos_held, 0), 2)         AS demo_to_win_rate_pct,

        -- End-to-end
        ROUND(o.closed_won * 100.0 / NULLIF(l.total_leads, 0), 2)        AS lead_to_win_rate_pct,

        -- Loss breakdown
        o.pre_demo_losses,
        o.process_losses,
        o.competitive_losses,
        o.product_fit_losses,

        -- Lead health
        l.working_leads,
        l.new_leads,
        l.disqualified_leads,
        l.bad_contact_leads,
        l.dm_connected_leads,
        ROUND(l.dm_connected_leads * 100.0 / NULLIF(l.total_leads, 0), 2) AS dm_connection_rate_pct,

        -- Velocity & profile
        l.avg_speed_to_lead_min,
        l.median_speed_to_lead_min,
        l.avg_predicted_sales,
        l.avg_touches_per_lead,
        l.avg_locations,
        o.avg_days_to_close_won,
        o.avg_won_predicted_sales,
        o.avg_touches_to_win

    FROM lead_metrics l
    LEFT JOIN opp_metrics o
        ON l.channel = o.channel

)

SELECT * FROM funnel
