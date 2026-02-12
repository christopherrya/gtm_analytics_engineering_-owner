/*
    MODEL: rpt_pipeline_health
    GRAIN: One row per channel × record_type × pipeline_segment × demo_status × health_status combination
    PURPOSE: Pre-aggregated summary of pipeline health for dashboard performance.
             Powers the Pipeline Health Dashboard frontend.
    UPSTREAM: ops_pipeline_health
    DOWNSTREAM: Pipeline Health Dashboard

    KEY METRICS:
    - Record counts and forecasted revenue by segment
    - Average aging and risk distribution
    - Channel-level pipeline composition
    - Actionable flags for pipeline management

    NOTES:
    - Open opportunities and working leads are unified via ops_pipeline_health
    - risk_distribution columns show what % of each segment is High/Medium/Low risk

    REVENUE COLUMNS (side by side):
    - forecasted_*: Raw revenue assuming all deals close (unweighted)
    - potential_*: Probability-weighted revenue (if converted & won)
      * Opportunities: win rate only (Inbound 24.1%, Outbound 17.1%)
      * Leads: conversion × win rate (Inbound 4.4%, Outbound 0.8%)
*/

WITH pipeline AS (

    SELECT * FROM {{ ref('ops_pipeline_health') }}

),

-- Segment-level aggregation
segment_summary AS (

    SELECT
        channel,
        record_type,
        pipeline_segment,
        demo_status,
        health_status,

        -- Counts
        COUNT(*)                                                    AS record_count,

        -- Raw revenue (assuming all deals close)
        ROUND(SUM(forecasted_monthly_revenue), 2)                   AS total_forecasted_monthly_revenue,
        ROUND(SUM(forecasted_annual_revenue), 2)                    AS total_forecasted_annual_revenue,
        ROUND(AVG(forecasted_monthly_revenue), 2)                   AS avg_forecasted_monthly_revenue,

        -- Probability-weighted revenue (potential if converted & won)
        ROUND(SUM(potential_monthly_revenue), 2)                    AS total_potential_monthly_revenue,
        ROUND(SUM(potential_annual_revenue), 2)                     AS total_potential_annual_revenue,
        ROUND(AVG(potential_monthly_revenue), 2)                    AS avg_potential_monthly_revenue,

        -- Aging
        ROUND(AVG(days_in_pipeline), 1)                             AS avg_days_in_pipeline,
        ROUND(AVG(days_in_current_stage), 1)                        AS avg_days_in_current_stage,
        MIN(days_in_pipeline)                                       AS min_days_in_pipeline,
        MAX(days_in_pipeline)                                       AS max_days_in_pipeline,

        -- Value profile
        ROUND(AVG(predicted_sales_with_owner), 2)                   AS avg_predicted_sales,
        ROUND(AVG(total_touch_count), 1)                            AS avg_touch_count,
        ROUND(AVG(CASE WHEN speed_to_lead_minutes IS NOT NULL
            THEN speed_to_lead_minutes END), 1)                     AS avg_speed_to_lead_minutes,

        -- Decision maker rate
        ROUND(SUM(CASE WHEN connected_with_decision_maker
            THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1)              AS decision_maker_pct,

        -- Demo status (for opportunities only)
        SUM(CASE WHEN demo_held THEN 1 ELSE 0 END)                 AS demos_held,
        SUM(CASE WHEN demo_set_date IS NOT NULL
            AND demo_held = FALSE THEN 1 ELSE 0 END)               AS demos_pending,

        -- Risk distribution
        SUM(CASE WHEN risk_level = 'High'
            THEN 1 ELSE 0 END)                                     AS high_risk_count,
        SUM(CASE WHEN risk_level = 'Medium'
            THEN 1 ELSE 0 END)                                     AS medium_risk_count,
        SUM(CASE WHEN risk_level = 'Low'
            THEN 1 ELSE 0 END)                                     AS low_risk_count,
        ROUND(SUM(CASE WHEN risk_level = 'High'
            THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1)             AS high_risk_pct,
        ROUND(SUM(CASE WHEN risk_level = 'Medium'
            THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1)              AS medium_risk_pct,
        ROUND(SUM(CASE WHEN risk_level = 'Low'
            THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1)             AS low_risk_pct,

        -- Speed to lead buckets (for inbound leads)
        SUM(CASE WHEN speed_to_lead_minutes <= 5
            THEN 1 ELSE 0 END)                                     AS responded_under_5min,
        SUM(CASE WHEN speed_to_lead_minutes <= 30
            THEN 1 ELSE 0 END)                                     AS responded_under_30min,
        SUM(CASE WHEN speed_to_lead_minutes > 60
            THEN 1 ELSE 0 END)                                     AS responded_over_1hr

    FROM pipeline
    GROUP BY 1, 2, 3, 4, 5

),

-- Channel-level totals for composition percentages
channel_totals AS (

    SELECT
        channel,
        SUM(record_count)                                           AS channel_total_records,
        SUM(total_forecasted_annual_revenue)                        AS channel_total_forecasted_revenue,
        SUM(total_potential_annual_revenue)                         AS channel_total_potential_revenue
    FROM segment_summary
    GROUP BY 1

),

final AS (

    SELECT
        s.channel,
        s.record_type,
        s.pipeline_segment,
        s.demo_status,
        s.health_status,

        -- Counts & composition
        s.record_count,
        ROUND(s.record_count * 100.0
            / ct.channel_total_records, 1)                          AS pct_of_channel,

        -- Raw revenue (unweighted)
        s.total_forecasted_monthly_revenue,
        s.total_forecasted_annual_revenue,
        s.avg_forecasted_monthly_revenue,
        ROUND(s.total_forecasted_annual_revenue * 100.0
            / NULLIF(ct.channel_total_forecasted_revenue, 0), 1)    AS pct_of_channel_forecasted_revenue,

        -- Probability-weighted revenue (potential if converted & won)
        s.total_potential_monthly_revenue,
        s.total_potential_annual_revenue,
        s.avg_potential_monthly_revenue,
        ROUND(s.total_potential_annual_revenue * 100.0
            / NULLIF(ct.channel_total_potential_revenue, 0), 1)     AS pct_of_channel_potential_revenue,

        -- Aging
        s.avg_days_in_pipeline,
        s.avg_days_in_current_stage,
        s.min_days_in_pipeline,
        s.max_days_in_pipeline,

        -- Profile
        s.avg_predicted_sales,
        s.avg_touch_count,
        s.avg_speed_to_lead_minutes,
        s.decision_maker_pct,

        -- Demo status
        s.demos_held,
        s.demos_pending,

        -- Risk
        s.high_risk_count,
        s.medium_risk_count,
        s.low_risk_count,
        s.high_risk_pct,
        s.medium_risk_pct,
        s.low_risk_pct,

        -- Speed to lead
        s.responded_under_5min,
        s.responded_under_30min,
        s.responded_over_1hr

    FROM segment_summary s
    LEFT JOIN channel_totals ct
        ON s.channel = ct.channel

)

SELECT * FROM final
ORDER BY channel, record_type, pipeline_segment, demo_status, health_status
