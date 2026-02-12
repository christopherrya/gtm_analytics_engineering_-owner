WITH deduplicated AS (
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
        converted_opportunity_id,
        ROW_NUMBER() OVER (PARTITION BY lead_id ORDER BY lead_id) AS _row_num -- ordering is non-determinstic. in production would want tiebreaker like ORDER BY last_sales_Activity_date DESC. But since we have confirmed that lead_id = PK. All good for now
    FROM {{ ref('ingress_leads') }}
)

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
FROM deduplicated
WHERE _row_num = 1
