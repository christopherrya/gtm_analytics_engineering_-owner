-- stg_opportunities.sql
-- Purpose: 1:1 with source - rename, cast, clean only
-- Derived fields (deal_outcome, days_to_close, etc.) live in entity layer

WITH source AS (

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
    FROM {{ ref('raw_opportunities') }}

),

cleaned AS (

    SELECT
        opportunity_id,
        stage_name,
        lost_reason_c,
        closed_lost_notes_c,
        business_issue_c,
        how_did_you_hear_about_us_c,

        -- Fix year bug on all date/timestamp columns
        {{ fix_year_timestamp('created_date') }} AS created_date,

        demo_held,

        {{ fix_year_date('demo_set_date') }} AS demo_set_date,
        {{ fix_year_timestamp('demo_time') }} AS demo_time,
        {{ fix_year_timestamp('last_sales_call_date_time') }} AS last_sales_call_date_time,
        {{ fix_year_date('close_date') }} AS close_date,

        account_id

    FROM source

)

SELECT
    -- Primary key
    opportunity_id,

    -- Stage info
    stage_name,

    -- Loss details
    lost_reason_c,
    closed_lost_notes_c,
    business_issue_c,

    -- Attribution
    how_did_you_hear_about_us_c,

    -- Dates (year-corrected)
    created_date,
    demo_held,
    demo_set_date,
    demo_time,
    close_date,
    last_sales_call_date_time,

    -- Account reference
    account_id

FROM cleaned
