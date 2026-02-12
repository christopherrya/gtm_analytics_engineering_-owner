/*
    MODEL: ops_pipeline_health
    GRAIN: One row per active pipeline record (open opportunity OR unconverted working lead)
    PURPOSE: Row-level operational view of everything still in play.
             Unions open opportunities with unconverted working leads into a single
             pipeline view, enabling flexible slicing by channel, health, stage, and value.
    UPSTREAM: ops_opportunities, ops_leads
    DOWNSTREAM: rpt_pipeline_health

    NOTES:
    - record_type distinguishes 'Opportunity' vs 'Lead' for filtering
    - pipeline_segment provides a more granular classification for dashboard grouping
    - demo_status classifies each record's demo progress for no-show monitoring
    - Reference date '2024-07-10' is the dataset end date (max close_date from opportunities)
    - Days in current state uses the most recent activity date available

    REVENUE COLUMNS:
    - forecasted_monthly/annual_revenue: Raw revenue assuming deal closes
    - potential_monthly/annual_revenue: Probability-weighted (if converted & won)
      * Opportunities: Uses historical win rate by channel
        - Inbound: 24.1% win rate
        - Outbound: 17.1% win rate
      * Leads: Uses conversion rate × win rate
        - Inbound: 18.2% conversion × 24.1% win = 4.4%
        - Outbound: 4.7% conversion × 17.1% win = 0.8%
*/

WITH open_opportunities AS (

    SELECT
        opportunity_id                                          AS record_id,
        'Opportunity'                                           AS record_type,
        channel,
        stage_name,

        -- Pipeline segment based on opportunity stage
        CASE
            WHEN stage_name = 'Interest'            THEN 'Early Pipeline'
            WHEN stage_name = 'Demo Set'            THEN 'Demo Scheduled'
            WHEN stage_name = 'In Progress'         THEN 'Active Deal'
            WHEN stage_name = 'Verbal Commitment'   THEN 'Near Close'
            WHEN stage_name = 'On Hold'             THEN 'Stalled'
            ELSE 'Other Open'
        END                                                     AS pipeline_segment,

        -- Demo status classification for no-show monitoring
        CASE
            WHEN demo_held = TRUE                               THEN 'Demo Held'
            WHEN demo_set_date IS NOT NULL
                AND demo_held = FALSE                           THEN 'Demo Scheduled - Not Held'
            WHEN demo_set_date IS NULL                          THEN 'No Demo Scheduled'
            ELSE 'Unknown'
        END                                                     AS demo_status,

        -- Timing metrics
        created_date,
        demo_set_date,
        demo_held,
        DATEDIFF('day', created_date,
            '2024-07-10'::DATE)                                 AS days_in_pipeline,
        DATEDIFF('day',
            COALESCE(demo_set_date, created_date::DATE),
            '2024-07-10'::DATE)                                 AS days_in_current_stage,

        -- Value metrics
        predicted_sales_with_owner,
        predicted_sales_tier,

        -- Raw revenue (assuming deal closes)
        monthly_revenue                                         AS forecasted_monthly_revenue,
        monthly_revenue * 12                                    AS forecasted_annual_revenue,

        -- Probability-weighted revenue (potential if won)
        ROUND(monthly_revenue
            * CASE channel
                WHEN 'Inbound' THEN 0.241
                WHEN 'Outbound' THEN 0.171
                ELSE 0.20
              END, 2)                                           AS potential_monthly_revenue,
        ROUND(monthly_revenue * 12
            * CASE channel
                WHEN 'Inbound' THEN 0.241
                WHEN 'Outbound' THEN 0.171
                ELSE 0.20
              END, 2)                                           AS potential_annual_revenue,

        -- Lead context (from the joined lead)
        lead_id,
        total_touch_count,
        speed_to_lead_minutes,
        connected_with_decision_maker,

        -- Health classification for opportunities
        CASE
            WHEN stage_name = 'On Hold'                         THEN 'Stalled'
            WHEN DATEDIFF('day', created_date,
                '2024-07-10'::DATE) > 90                        THEN 'Aging'
            WHEN DATEDIFF('day', created_date,
                '2024-07-10'::DATE) > 30                        THEN 'Maturing'
            ELSE 'Fresh'
        END                                                     AS health_status,

        -- Loss risk indicator
        CASE
            WHEN stage_name = 'On Hold'                         THEN 'High'
            WHEN stage_name = 'Interest'
                AND DATEDIFF('day', created_date,
                    '2024-07-10'::DATE) > 30                    THEN 'High'
            WHEN demo_held = FALSE
                AND demo_set_date IS NOT NULL
                AND DATEDIFF('day', demo_set_date,
                    '2024-07-10'::DATE) > 14                    THEN 'High'
            WHEN DATEDIFF('day', created_date,
                '2024-07-10'::DATE) > 60                        THEN 'Medium'
            ELSE 'Low'
        END                                                     AS risk_level

    FROM {{ ref('ops_opportunities') }}
    WHERE is_closed = FALSE

),

unconverted_working_leads AS (

    SELECT
        lead_id                                                 AS record_id,
        'Lead'                                                  AS record_type,
        channel,
        status                                                  AS stage_name,

        -- Pipeline segment based on working lead health
        CASE
            WHEN working_lead_health = 'Active'     THEN 'Active Lead'
            WHEN working_lead_health = 'Aging'      THEN 'Aging Lead'
            WHEN working_lead_health = 'Stale'      THEN 'Stale Lead'
            ELSE 'Working Lead'
        END                                                     AS pipeline_segment,

        -- Demo status: working leads have not reached demo stage
        'No Demo Scheduled'                                     AS demo_status,

        -- Timing metrics
        COALESCE(form_submission_date,
            first_sales_call_date)                              AS created_date,
        NULL::DATE                                              AS demo_set_date,
        FALSE                                                   AS demo_held,
        DATEDIFF('day',
            COALESCE(form_submission_date, first_sales_call_date),
            '2024-07-10'::DATE)                                 AS days_in_pipeline,
        DATEDIFF('day',
            COALESCE(last_sales_activity_date,
                last_sales_call_date,
                first_sales_call_date,
                form_submission_date),
            '2024-07-10'::DATE)                                 AS days_in_current_stage,

        -- Value metrics (inherit tier from ops_leads)
        predicted_sales_with_owner,
        predicted_sales_tier,

        -- Raw revenue (assuming lead converts and closes)
        500.00 + COALESCE(predicted_sales_with_owner * 0.05, 0) AS forecasted_monthly_revenue,
        (500.00 + COALESCE(predicted_sales_with_owner * 0.05, 0)) * 12
                                                                AS forecasted_annual_revenue,

        -- Probability-weighted revenue (potential if converted & won)
        ROUND((500.00 + COALESCE(predicted_sales_with_owner * 0.05, 0))
            * CASE channel
                WHEN 'Inbound' THEN 0.182 * 0.241   -- 4.4%
                WHEN 'Outbound' THEN 0.047 * 0.171  -- 0.8%
                ELSE 0.05
              END, 2)                                           AS potential_monthly_revenue,
        ROUND((500.00 + COALESCE(predicted_sales_with_owner * 0.05, 0)) * 12
            * CASE channel
                WHEN 'Inbound' THEN 0.182 * 0.241
                WHEN 'Outbound' THEN 0.047 * 0.171
                ELSE 0.05
              END, 2)                                           AS potential_annual_revenue,

        -- Lead context
        lead_id,
        total_touch_count,
        speed_to_lead_minutes,
        connected_with_decision_maker,

        -- Health classification (reuse working_lead_health from ops_leads)
        working_lead_health                                     AS health_status,

        -- Loss risk for leads
        CASE
            WHEN working_lead_health = 'Stale'                  THEN 'High'
            WHEN working_lead_health = 'Aging'                  THEN 'Medium'
            ELSE 'Low'
        END                                                     AS risk_level

    FROM {{ ref('ops_leads') }}
    WHERE status = 'Working'
      AND is_converted = FALSE

),

final AS (

    SELECT * FROM open_opportunities
    UNION ALL
    SELECT * FROM unconverted_working_leads

)

SELECT * FROM final
