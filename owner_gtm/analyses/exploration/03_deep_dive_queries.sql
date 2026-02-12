/*
================================================================================
  DEEP DIVE QUERIES — Follow-up Analysis
  Owner.com GTM Analytics Case Study
  
  Run against: demo_db.gtm_case (Snowflake)
  Purpose: Dig into the open questions surfaced by discovery queries
================================================================================
*/


-- ============================================================================
-- Q7: Lost reasons by CHANNEL (inbound vs outbound)
-- Question: Do inbound and outbound leads lose for different reasons?
-- ============================================================================
SELECT
    CASE WHEN l.form_submission_date IS NOT NULL THEN 'Inbound' ELSE 'Outbound' END AS channel,
    o.lost_reason_c,
    o.demo_held,
    COUNT(*) AS cnt,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (
        PARTITION BY CASE WHEN l.form_submission_date IS NOT NULL THEN 'Inbound' ELSE 'Outbound' END
    ), 2) AS pct_within_channel
FROM demo_db.gtm_case.opportunities o
JOIN demo_db.gtm_case.leads l
    ON l.converted_opportunity_id = o.opportunity_id
WHERE o.stage_name = 'Closed Lost'
GROUP BY 1, 2, 3
ORDER BY 1, 4 DESC;


-- ============================================================================
-- Q8: Speed to lead — time from form submission to first sales contact (inbound only)
-- Question: How fast are SDRs reaching out to inbound leads?
-- ============================================================================
SELECT
    CASE
        WHEN mins <= 5 THEN 'a. 0-5 min'
        WHEN mins <= 15 THEN 'b. 5-15 min'
        WHEN mins <= 60 THEN 'c. 15-60 min'
        WHEN mins <= 1440 THEN 'd. 1-24 hours'
        WHEN mins <= 4320 THEN 'e. 1-3 days'
        ELSE 'f. 3+ days'
    END AS speed_bucket,
    COUNT(*) AS leads,
    SUM(CASE WHEN converted_opportunity_id IS NOT NULL THEN 1 ELSE 0 END) AS converted,
    ROUND(SUM(CASE WHEN converted_opportunity_id IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS conversion_rate_pct
FROM (
    SELECT
        DATEDIFF('minute', form_submission_date,
            LEAST(
                COALESCE(first_sales_call_date, first_text_sent_date),
                COALESCE(first_text_sent_date, first_sales_call_date)
            )
        ) AS mins,
        converted_opportunity_id
    FROM demo_db.gtm_case.leads
    WHERE form_submission_date IS NOT NULL
        AND (first_sales_call_date IS NOT NULL OR first_text_sent_date IS NOT NULL)
) sub
GROUP BY 1
ORDER BY 1;


-- ============================================================================
-- Q9: Decision maker connection vs conversion
-- Question: Does reaching the decision maker predict conversion?
-- ============================================================================
SELECT
    CASE WHEN form_submission_date IS NOT NULL THEN 'Inbound' ELSE 'Outbound' END AS channel,
    connected_with_decision_maker,
    COUNT(*) AS leads,
    SUM(CASE WHEN converted_opportunity_id IS NOT NULL THEN 1 ELSE 0 END) AS converted,
    ROUND(SUM(CASE WHEN converted_opportunity_id IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS conversion_rate_pct
FROM demo_db.gtm_case.leads
GROUP BY 1, 2
ORDER BY 1, 2;


-- ============================================================================
-- Q10: No-demo-held trend by week — getting better or worse?
-- Question: Is the demo no-show problem improving over time?
-- ============================================================================
SELECT
    DATE_TRUNC('week', o.close_date) AS close_week,
    COUNT(*) AS total_closed_lost,
    SUM(CASE WHEN o.lost_reason_c = 'No Demo Held' THEN 1 ELSE 0 END) AS no_demo_held,
    ROUND(SUM(CASE WHEN o.lost_reason_c = 'No Demo Held' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS no_demo_pct
FROM demo_db.gtm_case.opportunities o
WHERE o.stage_name = 'Closed Lost'
    AND o.close_date IS NOT NULL
GROUP BY 1
ORDER BY 1;


-- ============================================================================
-- Q11: "Working" lead age — how stale are these leads?
-- Question: How long have leads been sitting in "Working" status?
-- ============================================================================
SELECT
    CASE
        WHEN days_since <= 30 THEN 'a. 0-30 days'
        WHEN days_since <= 60 THEN 'b. 30-60 days'
        WHEN days_since <= 90 THEN 'c. 60-90 days'
        WHEN days_since <= 180 THEN 'd. 90-180 days'
        ELSE 'e. 180+ days'
    END AS age_bucket,
    COUNT(*) AS leads,
    ROUND(AVG(total_touches), 1) AS avg_touches,
    ROUND(AVG(predicted_sales), 0) AS avg_predicted_sales
FROM (
    SELECT
        DATEDIFF('day',
            COALESCE(last_sales_activity_date, first_sales_call_date, first_text_sent_date, form_submission_date),
            CURRENT_DATE()
        ) AS days_since,
        (COALESCE(sales_call_count, 0) + COALESCE(sales_text_count, 0) + COALESCE(sales_email_count, 0)) AS total_touches,
        TRY_TO_DOUBLE(REPLACE(predicted_sales_with_owner, ',', '.')) AS predicted_sales
    FROM demo_db.gtm_case.leads
    WHERE status = 'Working'
) sub
GROUP BY 1
ORDER BY 1;


-- ============================================================================
-- Q12: Won deal profile — what does the ideal customer look like?
-- Question: What are the attributes of leads that became Closed Won?
-- ============================================================================
SELECT
    CASE WHEN l.form_submission_date IS NOT NULL THEN 'Inbound' ELSE 'Outbound' END AS channel,
    COUNT(*) AS won_deals,
    ROUND(AVG(TRY_TO_DOUBLE(REPLACE(l.predicted_sales_with_owner, ',', '.'))), 0) AS avg_predicted_sales,
    ROUND(AVG(l.location_count), 1) AS avg_locations,
    ROUND(AVG(CASE WHEN l.connected_with_decision_maker THEN 1.0 ELSE 0.0 END) * 100, 1) AS dm_rate_pct,
    ROUND(AVG(COALESCE(l.sales_call_count,0) + COALESCE(l.sales_text_count,0) + COALESCE(l.sales_email_count,0)), 1) AS avg_touches,
    ROUND(AVG(o.days_to_close), 1) AS avg_days_to_close
FROM demo_db.gtm_case.opportunities o
JOIN demo_db.gtm_case.leads l ON l.converted_opportunity_id = o.opportunity_id
WHERE o.stage_name = 'Closed Won'
    AND o.close_date IS NOT NULL
GROUP BY 1;


-- ============================================================================
-- Q13: Predicted sales distribution — won vs lost vs unconverted
-- Question: Do higher-value prospects convert and win more?
-- ============================================================================
SELECT
    CASE
        WHEN predicted_sales >= 5000 THEN 'a. $5,000+'
        WHEN predicted_sales >= 2000 THEN 'b. $2,000-5,000'
        WHEN predicted_sales >= 1000 THEN 'c. $1,000-2,000'
        WHEN predicted_sales >= 500  THEN 'd. $500-1,000'
        WHEN predicted_sales > 0     THEN 'e. $1-500'
        ELSE 'f. Unknown/Zero'
    END AS predicted_sales_tier,
    COUNT(*) AS total_leads,
    SUM(CASE WHEN converted_opportunity_id IS NOT NULL THEN 1 ELSE 0 END) AS converted,
    ROUND(SUM(CASE WHEN converted_opportunity_id IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS conversion_rate_pct
FROM (
    SELECT
        TRY_TO_DOUBLE(REPLACE(predicted_sales_with_owner, ',', '.')) AS predicted_sales,
        converted_opportunity_id
    FROM demo_db.gtm_case.leads
) sub
GROUP BY 1
ORDER BY 1;
