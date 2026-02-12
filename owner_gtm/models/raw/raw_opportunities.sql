WITH deduplicated AS (
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
        account_id,
        ROW_NUMBER() OVER (PARTITION BY opportunity_id ORDER BY created_date DESC) AS _row_num
    FROM {{ ref('ingress_opportunities') }}
)

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
FROM deduplicated
WHERE _row_num = 1
