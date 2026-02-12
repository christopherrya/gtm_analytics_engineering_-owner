-- Opportunity entity: deal outcome, cycle-time metrics (days to close, days to demo,
-- days demo-to-close), and closed/open flags for reporting and ops joins.
--
-- Guards:
--   1. days_to_close / days_to_demo / days_demo_to_close are only computed when both
--      required dates are non-null. Otherwise NULL so downstream aggregations don't
--      misattribute zero-day cycles.
--   2. deal_outcome and is_closed_* derive from stage_name only; no separate
--      "closed" timestamp, so pipeline health and loss analysis use stage + close_date.
--
-- Downstream: ops_opportunities (lead enrichment, revenue model, loss_category),
-- ops_pipeline_health (open-opp branch), and reporting marts.

WITH staging AS (

    SELECT
        opportunity_id,
        stage_name,
        lost_reason_c,
        closed_lost_notes_c,
        business_issue_c,
        how_did_you_hear_about_us_c,
        created_date,
        demo_held,
        demo_set_date,
        demo_time,
        close_date,
        last_sales_call_date_time,
        account_id
    FROM {{ ref('stg_opportunities') }}

),

entity AS (

    SELECT
        -- Pass through all staged columns
        opportunity_id,
        stage_name,
        lost_reason_c,
        closed_lost_notes_c,
        business_issue_c,
        how_did_you_hear_about_us_c,
        created_date,
        demo_held,
        demo_set_date,
        demo_time,
        close_date,
        last_sales_call_date_time,
        account_id,

        -- Derived fields
        CASE
            WHEN stage_name = 'Closed Won'  THEN 'Won'
            WHEN stage_name = 'Closed Lost' THEN 'Lost'
            ELSE 'Open'
        END AS deal_outcome,

        -- Days from opportunity creation to close
        CASE
            WHEN close_date IS NOT NULL AND created_date IS NOT NULL
            THEN DATEDIFF('day', created_date, close_date)
            ELSE NULL
        END AS days_to_close,

        -- Days from demo set to close
        CASE
            WHEN close_date IS NOT NULL AND demo_set_date IS NOT NULL
            THEN DATEDIFF('day', demo_set_date, close_date)
            ELSE NULL
        END AS days_demo_to_close,

        -- Days from opportunity creation to demo
        CASE
            WHEN demo_set_date IS NOT NULL AND created_date IS NOT NULL
            THEN DATEDIFF('day', created_date, demo_set_date)
            ELSE NULL
        END AS days_to_demo,

        -- Boolean flags
        (stage_name = 'Closed Won') AS is_closed_won,
        (stage_name = 'Closed Lost') AS is_closed_lost,
        (stage_name IN ('Closed Won', 'Closed Lost')) AS is_closed

    FROM staging

)

SELECT
    -- Primary key
    opportunity_id,

    -- Stage info
    stage_name,
    deal_outcome,

    -- Loss details
    lost_reason_c,
    closed_lost_notes_c,
    business_issue_c,

    -- Attribution
    how_did_you_hear_about_us_c,

    -- Dates
    created_date,
    demo_held,
    demo_set_date,
    demo_time,
    close_date,
    last_sales_call_date_time,

    -- Cycle metrics
    days_to_close,
    days_to_demo,
    days_demo_to_close,

    -- Boolean flags
    is_closed_won,
    is_closed_lost,
    is_closed,

    -- Account reference
    account_id

FROM entity
