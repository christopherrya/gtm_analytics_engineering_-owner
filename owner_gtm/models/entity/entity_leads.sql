-- Speed to lead: minutes from form submission to first sales contact (inbound only)
--
-- Guards:
--   1. form_submission_date >= '2024-01-01' excludes records with residual year-bug
--      artifacts from the 0024→2024 fix in staging
--   2. LEAST(first_sales_call, first_text) >= form_submission_date ensures the first
--      sales contact happened AFTER the form submission. Without this, hybrid leads
--      (outbound-sourced prospects who later submit an inbound form) produce negative
--      speed-to-lead values. ~60 converted leads in the dataset follow this pattern.
--
-- Known limitation: these hybrid leads are classified as Inbound by the channel
-- derivation logic (form_submission_date IS NOT NULL), but were originally engaged
-- via outbound. Their speed_to_lead_minutes is correctly set to NULL here, but they
-- slightly inflate the Inbound "missing speed-to-lead" population. A production
-- fix would derive an original_source_channel from whichever date came first.

WITH staging AS (

    SELECT
        lead_id,
        form_submission_date,
        first_sales_call_date,
        first_text_sent_date,
        first_meeting_booked_date,
        last_sales_call_date,
        last_sales_activity_date,
        last_sales_email_date,
        sales_call_count,
        sales_text_count,
        sales_email_count,
        predicted_sales_with_owner,
        marketplace_count,
        marketplaces_used_raw,
        online_ordering_count,
        online_ordering_used_raw,
        cuisine_type_count,
        cuisine_types_raw,
        location_count,
        connected_with_decision_maker,
        status,
        converted_opportunity_id
    FROM {{ ref('stg_leads') }}

),

entity AS (

    SELECT
        -- Pass through all staged columns
        lead_id,
        form_submission_date,
        first_sales_call_date,
        first_text_sent_date,
        first_meeting_booked_date,
        last_sales_call_date,
        last_sales_activity_date,
        last_sales_email_date,
        sales_call_count,
        sales_text_count,
        sales_email_count,
        predicted_sales_with_owner,
        marketplace_count,
        marketplaces_used_raw,
        online_ordering_count,
        online_ordering_used_raw,
        cuisine_type_count,
        cuisine_types_raw,
        location_count,
        connected_with_decision_maker,
        status,
        converted_opportunity_id,

        -- Derive channel: inbound if form was submitted, outbound otherwise
        CASE
            WHEN form_submission_date IS NOT NULL THEN 'Inbound'
            ELSE 'Outbound'
        END AS channel,

        -- Total sales touches
        sales_call_count + sales_text_count + sales_email_count AS total_touch_count,

        -- Speed to lead: minutes from form submission to first sales contact (inbound only)
        -- Filter to 2024+ to exclude bad historical dates causing unrealistic values
        -- Only include positive values (sales contact must be after form submission)
        CASE
            WHEN form_submission_date IS NOT NULL
                AND form_submission_date >= '2024-01-01'
                AND (first_sales_call_date IS NOT NULL OR first_text_sent_date IS NOT NULL)
                AND LEAST(
                        COALESCE(first_sales_call_date, first_text_sent_date),
                        COALESCE(first_text_sent_date, first_sales_call_date)
                    ) >= form_submission_date
            THEN DATEDIFF('minute',
                    form_submission_date,
                    LEAST(
                        COALESCE(first_sales_call_date, first_text_sent_date),
                        COALESCE(first_text_sent_date, first_sales_call_date)
                    )
                 )
            ELSE NULL
        END AS speed_to_lead_minutes,

        -- Boolean flags
        (converted_opportunity_id IS NOT NULL) AS is_converted

    FROM staging

)

SELECT
    -- Primary key
    lead_id,

    -- Timestamps
    form_submission_date,
    first_sales_call_date,
    first_text_sent_date,
    first_meeting_booked_date,
    last_sales_call_date,
    last_sales_activity_date,
    last_sales_email_date,

    -- Activity counts
    sales_call_count,
    sales_text_count,
    sales_email_count,
    total_touch_count,

    -- Predicted revenue
    predicted_sales_with_owner,

    -- Parsed list counts and raw values
    marketplace_count,
    marketplaces_used_raw,
    online_ordering_count,
    online_ordering_used_raw,
    cuisine_type_count,
    cuisine_types_raw,

    -- Business attributes
    location_count,
    connected_with_decision_maker,
    status,
    converted_opportunity_id,

    -- Derived fields
    channel,
    speed_to_lead_minutes,
    is_converted

FROM entity
