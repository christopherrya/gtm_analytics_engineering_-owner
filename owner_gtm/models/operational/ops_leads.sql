/*

    MODEL: ops_leads
    GRAIN: One row per lead (enriched with opportunity context where available)

    
    IMPORTANT DERIVED FIELDS:
    - lead_id: Primary key
    - converted_opportunity_id: Opportunity ID if the lead converted to an opportunity
    - channel: Inbound/Outbound (from original lead source)
    - status: Current lead status (standardized)
    - is_converted: Whether this lead converted to an opportunity
    - form_submission_date: Date the lead submitted a form (year-corrected)
    - first_sales_call_date: Timestamp of first sales call (year-corrected)
    - first_text_sent_date: Timestamp of first text sent (year-corrected)
    - first_meeting_booked_date: Timestamp of first meeting booked (year-corrected)
    - last_sales_call_date: Timestamp of most recent sales call (year-corrected)
    - last_sales_activity_date: Timestamp of most recent sales activity (year-corrected)
    - last_sales_email_date: Timestamp of most recent sales email (year-corrected)
    - sales_call_count: Number of sales calls made (0 if null)
    - sales_text_count: Number of sales texts sent (0 if null)
    - sales_email_count: Number of sales emails sent (0 if null)
    - total_touch_count: Derived: Sum of sales calls, texts, and emails
    - speed_to_lead_minutes: Minutes from form submission to first sales contact (inbound only)
    - predicted_sales_with_owner: Predicted monthly sales if using Owner.com (cast to numeric in staging)
    - location_count: Number of locations the lead is interested in
    - connected_with_decision_maker: Whether the lead is connected with a decision maker
    - marketplace_count: Number of marketplace platforms used (parsed from list)
    - marketplaces_used_raw: Raw marketplaces_used string from source
    - online_ordering_count: Number of online ordering platforms used (parsed from list)
    - online_ordering_used_raw: Raw online_ordering_used string from source
    - cuisine_type_count: Number of cuisine types the lead is interested in
    - cuisine_types_raw: Raw cuisine_types string from source
    - days_since_last_activity: Days between last activity and dataset end date (2024-07-10)
    - working_lead_health: For Working leads only: Active (<30d), Aging (30-90d), or Stale (>90d)

    NOTE: Only one opportunity is joined per lead (first by opportunity_id), to ensure 1:1 enrichment.
*/


    SELECT
        lead_id,
        converted_opportunity_id,
        channel,
        status,
        is_converted,
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
        total_touch_count,
        speed_to_lead_minutes,
        predicted_sales_with_owner,
        location_count,
        connected_with_decision_maker,
        marketplace_count,
        marketplaces_used_raw,
        online_ordering_count,
        online_ordering_used_raw,
        cuisine_type_count,
        cuisine_types_raw
    FROM {{ ref('entity_leads') }}

)

SELECT
    -- Keys
    lead_id,
    converted_opportunity_id,

    -- Channel & status
    channel,
    status,
    is_converted,

    -- Dates (all year-corrected upstream)
    form_submission_date,
    first_sales_call_date,
    first_text_sent_date,
    first_meeting_booked_date,
    last_sales_call_date,
    last_sales_activity_date,
    last_sales_email_date,

    -- Activity metrics
    sales_call_count,
    sales_text_count,
    sales_email_count,
    total_touch_count,

    -- Speed to lead
    speed_to_lead_minutes,
    CASE
        WHEN speed_to_lead_minutes IS NULL        THEN 'Not Applicable'
        WHEN speed_to_lead_minutes <= 5           THEN '0-5 min'
        WHEN speed_to_lead_minutes <= 15          THEN '5-15 min'
        WHEN speed_to_lead_minutes <= 60          THEN '15-60 min'
        WHEN speed_to_lead_minutes <= 1440        THEN '1-24 hours'
        WHEN speed_to_lead_minutes <= 4320        THEN '1-3 days'
        ELSE '3+ days'
    END                                           AS speed_to_lead_bucket,

    -- Prospect profile
    predicted_sales_with_owner,
    CASE
        WHEN predicted_sales_with_owner IS NULL   THEN 'Unknown'
        WHEN predicted_sales_with_owner >= 10000  THEN 'Tier 1 - $10,000+'
        WHEN predicted_sales_with_owner >= 5000   THEN 'Tier 2 - $5,000-10,000'
        WHEN predicted_sales_with_owner >= 2000   THEN 'Tier 3 - $2,000-5,000'
        WHEN predicted_sales_with_owner >= 1000   THEN 'Tier 4 - $1,000-2,000'
        WHEN predicted_sales_with_owner >= 500    THEN 'Tier 5 - $500-1,000'
        ELSE 'Tier 6 - Under $500'
    END                                           AS predicted_sales_tier,

    location_count,
    connected_with_decision_maker,
    marketplace_count,
    marketplaces_used_raw,
    online_ordering_count,
    online_ordering_used_raw,
    cuisine_type_count,
    cuisine_types_raw,

    -- Lead age: days since last activity (relative to max date in dataset for consistency)
    DATEDIFF('day',
        COALESCE(last_sales_activity_date, last_sales_call_date, last_sales_email_date, form_submission_date),
        '2024-07-10'::DATE
    )                                             AS days_since_last_activity,

    CASE
        WHEN status = 'Working' THEN
            CASE
                WHEN DATEDIFF('day',
                    COALESCE(last_sales_activity_date, last_sales_call_date, last_sales_email_date, form_submission_date),
                    '2024-07-10'::DATE
                ) > 90 THEN 'Stale'
                WHEN DATEDIFF('day',
                    COALESCE(last_sales_activity_date, last_sales_call_date, last_sales_email_date, form_submission_date),
                    '2024-07-10'::DATE
                ) > 30 THEN 'Aging'
                ELSE 'Active'
            END
        ELSE NULL
    END                                           AS working_lead_health

FROM entity
