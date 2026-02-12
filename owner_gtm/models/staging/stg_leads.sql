-- stg_leads.sql
-- Purpose: 1:1 with source - rename, cast, clean only
-- Derived fields (channel, speed_to_lead, is_converted) live in entity layer

WITH source AS (

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
        marketplaces_used,
        online_ordering_used,
        cuisine_types,
        location_count,
        connected_with_decision_maker,
        status,
        converted_opportunity_id
    FROM {{ ref('raw_leads') }}

),

cleaned AS (

    SELECT
        lead_id,

        -- Fix year bug: dates stored as 0020-0024 instead of 2020-2024
        {{ fix_year_timestamp('form_submission_date') }}      AS form_submission_date,
        {{ fix_year_timestamp('first_sales_call_date') }}     AS first_sales_call_date,
        {{ fix_year_timestamp('first_text_sent_date') }}      AS first_text_sent_date,
        {{ fix_year_timestamp('first_meeting_booked_date') }} AS first_meeting_booked_date,
        {{ fix_year_timestamp('last_sales_call_date') }}      AS last_sales_call_date,
        {{ fix_year_timestamp('last_sales_activity_date') }}  AS last_sales_activity_date,
        {{ fix_year_timestamp('last_sales_email_date') }}     AS last_sales_email_date,

        -- Null coalesce activity counts
        COALESCE(sales_call_count, 0) AS sales_call_count,
        COALESCE(sales_text_count, 0) AS sales_text_count,
        COALESCE(sales_email_count, 0) AS sales_email_count,

        -- Cast predicted sales: comma decimal text → numeric
        TRY_TO_DOUBLE(REPLACE(predicted_sales_with_owner, ',', '.')) AS predicted_sales_with_owner,

        -- Parse stringified Python lists → count of items
        CASE
            WHEN marketplaces_used IS NULL OR TRIM(marketplaces_used) IN ('[]', '') THEN 0
            ELSE COALESCE(ARRAY_SIZE(TRY_PARSE_JSON(REPLACE(REPLACE(marketplaces_used, '''', '"'), 'None', 'null'))), 0)
        END AS marketplace_count,
        marketplaces_used AS marketplaces_used_raw,

        CASE
            WHEN online_ordering_used IS NULL OR TRIM(online_ordering_used) IN ('[]', '') THEN 0
            ELSE COALESCE(ARRAY_SIZE(TRY_PARSE_JSON(REPLACE(REPLACE(online_ordering_used, '''', '"'), 'None', 'null'))), 0)
        END AS online_ordering_count,
        online_ordering_used AS online_ordering_used_raw,

        CASE
            WHEN cuisine_types IS NULL OR TRIM(cuisine_types) IN ('[]', '') THEN 0
            ELSE COALESCE(ARRAY_SIZE(TRY_PARSE_JSON(REPLACE(REPLACE(cuisine_types, '''', '"'), 'None', 'null'))), 0)
        END AS cuisine_type_count,
        cuisine_types AS cuisine_types_raw,

        location_count,
        connected_with_decision_maker,

        -- Standardize status: fix underscore variant
        REPLACE(status, '_', ' ') AS status,

        converted_opportunity_id

    FROM source

)

SELECT
    -- Primary key
    lead_id,

    -- Timestamps (year-corrected)
    form_submission_date,
    first_sales_call_date,
    first_text_sent_date,
    first_meeting_booked_date,
    last_sales_call_date,
    last_sales_activity_date,
    last_sales_email_date,

    -- Activity counts (null-coalesced to 0)
    sales_call_count,
    sales_text_count,
    sales_email_count,

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
    converted_opportunity_id

FROM cleaned
