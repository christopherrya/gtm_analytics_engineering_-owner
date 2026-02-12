/*
    MODEL: rpt_lead_outreach_priority
    GRAIN: One row per unconverted working lead requiring outreach action
    PURPOSE: Prioritizes leads for immediate sales outreach based on speed-to-lead timing.
             Separates actionable leads from expired leads (72+ hours without contact)
             and flags slow-contacted leads that may need re-engagement.

    SECTIONS:
    1. No Outreach Yet     — Leads with zero sales contact, tiered by urgency
    2. Slow First Contact  — Leads contacted, but response was delayed (bucketed by delay)
    3. Expired             — Leads past the 72-hour window with no contact (separate bucket)

    UPSTREAM: ops_leads
    DOWNSTREAM: Dashboard — Lead Outreach Priority Queue

    NOTES:
    - 72-hour (4,320 minute) cutoff based on conversion rate analysis:
      leads contacted after 3+ days show near-zero conversion lift
    - Reference date '2024-07-10' is the dataset end date (max close_date from opportunities)
    - Speed-to-lead is only meaningful for inbound leads (outbound has no form submission)
    - Outbound leads without outreach are flagged separately since speed-to-lead doesn't apply
    - Priority tiers for no-outreach leads:
        P0 Immediate   — less than 1 hour waiting
        P1 Urgent      — 1-24 hours waiting
        P2 At Risk     — 1-3 days waiting
        P3 Last Chance — approaching 72-hour expiry (but still under)
*/

WITH lead_outreach_base AS (

    SELECT
        lead_id,
        channel,
        status,
        working_lead_health,
        predicted_sales_with_owner,
        predicted_sales_tier,

        -- Core timestamps
        form_submission_date,
        first_sales_call_date,
        first_text_sent_date,
        first_meeting_booked_date,
        last_sales_activity_date,
        last_sales_call_date,

        -- Activity metrics
        total_touch_count,
        speed_to_lead_minutes,
        connected_with_decision_maker,

        -- Derived: first outreach timestamp (call or text, whichever came first)
        LEAST(
            COALESCE(first_sales_call_date, first_text_sent_date),
            COALESCE(first_text_sent_date, first_sales_call_date)
        )                                                       AS first_outreach_at,

        -- Derived: has any outreach occurred?
        (first_sales_call_date IS NOT NULL
            OR first_text_sent_date IS NOT NULL)                AS has_been_contacted,

        -- Derived: minutes waiting since lead entered the system
        -- For inbound: time since form submission
        -- For outbound: time since first sales call (proxy for list assignment)
        DATEDIFF('minute',
            COALESCE(form_submission_date, first_sales_call_date),
            '2024-07-10'::TIMESTAMP)                            AS minutes_waiting,

        DATEDIFF('hour',
            COALESCE(form_submission_date, first_sales_call_date),
            '2024-07-10'::TIMESTAMP)                            AS hours_waiting,

        DATEDIFF('day',
            COALESCE(form_submission_date, first_sales_call_date),
            '2024-07-10'::DATE)                                 AS days_waiting

    FROM {{ ref('ops_leads') }}
    WHERE status = 'Working'
      AND is_converted = FALSE

),

-- ============================================================================
-- SECTION 1: Leads with NO outreach — prioritized by wait time
-- ============================================================================
no_outreach AS (

    SELECT
        lead_id,
        'No Outreach'                                           AS outreach_section,
        channel,
        working_lead_health,
        predicted_sales_with_owner,
        predicted_sales_tier,

        -- Timestamps
        form_submission_date,
        first_outreach_at,
        last_sales_activity_date,

        -- Wait time
        minutes_waiting,
        hours_waiting,
        days_waiting,
        speed_to_lead_minutes,
        total_touch_count,
        connected_with_decision_maker,

        -- Priority assignment
        CASE
            -- Outbound leads with no anchor date (no form submission, no first call)
            WHEN minutes_waiting IS NULL            THEN 'Outbound - No Anchor Date'
            -- Expired: past 72-hour window
            WHEN minutes_waiting >= 4320            THEN 'Expired'
            -- P0: still within the golden hour
            WHEN minutes_waiting < 60               THEN 'P0 - Immediate'
            -- P1: under 24 hours — still very recoverable
            WHEN minutes_waiting < 1440             THEN 'P1 - Urgent'
            -- P2: 1-3 days — conversion rate dropping fast
            WHEN minutes_waiting < 4320             THEN 'P2 - At Risk'
            ELSE 'Expired'
        END                                                     AS outreach_priority,

        -- Numeric sort key for dashboard ordering (lower = more urgent)
        CASE
            WHEN minutes_waiting IS NULL            THEN 50  -- No anchor date: separate bucket
            WHEN minutes_waiting < 60               THEN 1
            WHEN minutes_waiting < 1440             THEN 2
            WHEN minutes_waiting < 4320             THEN 3
            ELSE 99
        END                                                     AS priority_sort_key,

        -- Actionability flag (NULL anchor = data quality issue, not actionable)
        (minutes_waiting IS NOT NULL AND minutes_waiting < 4320) AS is_actionable,

        -- Recommended action
        CASE
            WHEN minutes_waiting IS NULL
                THEN 'Verify lead source — no entry timestamp available'
            WHEN minutes_waiting < 60
                THEN 'Call immediately — within golden hour window'
            WHEN minutes_waiting < 1440
                THEN 'Call today — lead is still warm'
            WHEN minutes_waiting < 4320
                THEN 'Attempt contact ASAP — approaching expiry'
            ELSE 'Move to nurture campaign — past 72hr window'
        END                                                     AS recommended_action

    FROM lead_outreach_base
    WHERE has_been_contacted = FALSE

),

-- ============================================================================
-- SECTION 2: Leads with SLOW first contact — contacted but late
-- ============================================================================
slow_outreach AS (

    SELECT
        lead_id,
        'Slow Outreach'                                         AS outreach_section,
        channel,
        working_lead_health,
        predicted_sales_with_owner,
        predicted_sales_tier,

        -- Timestamps
        form_submission_date,
        first_outreach_at,
        last_sales_activity_date,

        -- Wait time (how long they waited before first contact)
        speed_to_lead_minutes                                   AS minutes_waiting,
        ROUND(speed_to_lead_minutes / 60.0, 1)                 AS hours_waiting,
        ROUND(speed_to_lead_minutes / 1440.0, 1)               AS days_waiting,
        speed_to_lead_minutes,
        total_touch_count,
        connected_with_decision_maker,

        -- Response delay bucket
        CASE
            WHEN speed_to_lead_minutes <= 5             THEN 'On Time (<=5 min)'
            WHEN speed_to_lead_minutes <= 60            THEN 'Slightly Late (5-60 min)'
            WHEN speed_to_lead_minutes <= 1440          THEN 'Late (1-24 hours)'
            WHEN speed_to_lead_minutes <= 4320          THEN 'Very Late (1-3 days)'
            ELSE 'Critically Late (3+ days)'
        END                                                     AS outreach_priority,

        -- Sort key: critically late contacts are highest priority for re-engagement
        CASE
            WHEN speed_to_lead_minutes <= 5             THEN 10
            WHEN speed_to_lead_minutes <= 60            THEN 8
            WHEN speed_to_lead_minutes <= 1440          THEN 6
            WHEN speed_to_lead_minutes <= 4320          THEN 4
            ELSE 3
        END                                                     AS priority_sort_key,

        -- Actionability: still working, so yes — but re-engagement may be needed
        TRUE                                                    AS is_actionable,

        -- Recommended action based on delay severity
        CASE
            WHEN speed_to_lead_minutes <= 5
                THEN 'Good response time — continue normal cadence'
            WHEN speed_to_lead_minutes <= 60
                THEN 'Acceptable — ensure consistent follow-up'
            WHEN speed_to_lead_minutes <= 1440
                THEN 'Re-engage with value proposition — trust may have cooled'
            WHEN speed_to_lead_minutes <= 4320
                THEN 'High-touch re-engagement needed — acknowledge the delay'
            ELSE 'Recovery outreach — lead likely explored competitors'
        END                                                     AS recommended_action

    FROM lead_outreach_base
    WHERE has_been_contacted = TRUE
      -- Only include inbound leads for slow-outreach analysis
      -- (speed_to_lead is meaningless for outbound)
      AND channel = 'Inbound'
      AND speed_to_lead_minutes IS NOT NULL

),

-- ============================================================================
-- UNION + final enrichment
-- ============================================================================
combined AS (

    SELECT * FROM no_outreach
    UNION ALL
    SELECT * FROM slow_outreach

),

final AS (

    SELECT
        lead_id,
        outreach_section,
        channel,
        working_lead_health,
        predicted_sales_with_owner,
        predicted_sales_tier,

        -- Timestamps
        form_submission_date,
        first_outreach_at,
        last_sales_activity_date,

        -- Timing
        minutes_waiting,
        hours_waiting,
        days_waiting,
        speed_to_lead_minutes,

        -- Activity
        total_touch_count,
        connected_with_decision_maker,

        -- Priority & action
        outreach_priority,
        priority_sort_key,
        is_actionable,
        recommended_action,

        -- Dashboard summary flags
        CASE
            WHEN outreach_priority = 'Outbound - No Anchor Date'
                THEN 'DATA QUALITY — NO ANCHOR'
            WHEN outreach_section = 'No Outreach' AND is_actionable = TRUE
                THEN 'ACTION REQUIRED'
            WHEN outreach_section = 'No Outreach' AND is_actionable = FALSE
                THEN 'EXPIRED — NURTURE'
            WHEN outreach_section = 'Slow Outreach'
                AND outreach_priority IN ('Very Late (1-3 days)', 'Critically Late (3+ days)')
                THEN 'RE-ENGAGE'
            WHEN outreach_section = 'Slow Outreach'
                THEN 'MONITOR'
            ELSE 'REVIEW'
        END                                                     AS dashboard_status,

        -- Value-weighted urgency: high-value leads that haven't been contacted
        -- should float to the top of the queue
        CASE
            WHEN is_actionable = TRUE
                AND predicted_sales_with_owner >= 5000
                AND outreach_section = 'No Outreach'
                THEN TRUE
            ELSE FALSE
        END                                                     AS is_high_value_urgent

    FROM combined

)

SELECT * FROM final
ORDER BY
    -- Actionable no-outreach leads first, then slow outreach, then expired
    is_actionable DESC,
    outreach_section ASC,
    priority_sort_key ASC,
    -- Within same priority, high-value leads first
    predicted_sales_with_owner DESC NULLS LAST
