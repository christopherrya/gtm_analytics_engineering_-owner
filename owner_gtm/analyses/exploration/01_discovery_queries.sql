/*
================================================================================
  DISCOVERY QUERIES — Current State Analysis
  Owner.com GTM Analytics Case Study
  
  Run against: demo_db.gtm_case (Snowflake)
  Purpose: Understand the full data landscape before building any models
================================================================================
*/


-- ============================================================================
-- Q1: Table volumes & date ranges
-- Result: leads=27,056 (dates 0020-07-29 to 0024-07-10), opportunities=2,794 (2024-01-02 to 2024-07-10)
-- Finding: LEADS table has year bug — years stored as 0020-0024 instead of 2020-2024
-- ============================================================================
SELECT 'leads' AS table_name, COUNT(*) AS row_count,
    MIN(form_submission_date) AS min_date, MAX(form_submission_date) AS max_date
FROM demo_db.gtm_case.leads
UNION ALL
SELECT 'opportunities', COUNT(*),
    MIN(created_date::date), MAX(created_date::date)
FROM demo_db.gtm_case.opportunities;


-- ============================================================================
-- Q2: Lead status distribution
-- Result: 54% Working, 11% Not Interested, 10.5% Converted, 8.8% Disqualified,
--         6.9% Incorrect Contact Data, 4.8% New
-- Finding: Massive pool of 14,703 "Working" leads — are these truly being worked?
--          1,867 (6.9%) incorrect contact data is a significant data quality issue
-- ============================================================================
SELECT status, COUNT(*) AS cnt,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct
FROM demo_db.gtm_case.leads
GROUP BY 1 ORDER BY 2 DESC;


-- ============================================================================
-- Q3: Opportunity stage distribution
-- Result: 63% Closed Lost (1,762), 22.3% Closed Won (623), 15% open pipeline
-- Finding: Overall win rate ~26% on closed deals. 409 opportunities still open.
-- ============================================================================
SELECT stage_name, COUNT(*) AS cnt,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct
FROM demo_db.gtm_case.opportunities
GROUP BY 1 ORDER BY 2 DESC;


-- ============================================================================
-- Q4: Inbound vs Outbound channel split
-- Result: Outbound 15,771 leads → 744 converted (4.72%)
--         Inbound  11,285 leads → 2,050 converted (18.17%)
-- Finding: Inbound converts at nearly 4x the rate of outbound.
--          Outbound is 58% of lead volume but only 27% of conversions.
-- ============================================================================
SELECT
    CASE WHEN form_submission_date IS NOT NULL THEN 'Inbound' ELSE 'Outbound' END AS channel,
    COUNT(*) AS leads,
    SUM(CASE WHEN converted_opportunity_id IS NOT NULL THEN 1 ELSE 0 END) AS converted,
    ROUND(SUM(CASE WHEN converted_opportunity_id IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS conversion_rate_pct
FROM demo_db.gtm_case.leads
GROUP BY 1;


-- ============================================================================
-- Q5: Lost reason breakdown for closed lost opportunities
-- Result: Top reasons:
--   33.3% — No Demo Held (586, demo_held=false) ← BIGGEST SINGLE LEAK
--   19.1% — Non-Responsive post-demo (337)
--    9.8% — Lack of Urgency (172)
--    8.8% — Price (155)
--    5.3% — POS Integration (93)
--    3.8% — Lost to Competitor (67)
--    5.2% — Bad Fit (92, mixed demo status)
--    3.6% — Not a Decision Maker (63, mixed)
-- Finding: 1/3 of all losses never saw a demo. Another ~30% are follow-up/process
--          issues (non-responsive + urgency). Only ~15% are product/pricing objections.
-- ============================================================================
SELECT
    lost_reason_c,
    demo_held,
    COUNT(*) AS cnt,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct
FROM demo_db.gtm_case.opportunities
WHERE stage_name = 'Closed Lost'
GROUP BY 1, 2
ORDER BY 3 DESC;


-- ============================================================================
-- Q6: Weekly closed deals — won vs lost with win rate
-- Result: 28 weeks (Jan–Jul 2024), 623 won, 1,762 lost, 26.1% overall win rate
-- Finding: Win rate started high in Jan (~50-60%) at low volume, compressed to
--          ~20% as volume scaled in Feb-May, slight recovery in late Jun/Jul.
--          Year bug confirmed: dates show as 0024- instead of 2024-.
-- ============================================================================
SELECT
    DATE_TRUNC('week', close_date) AS close_week,
    SUM(CASE WHEN stage_name = 'Closed Won' THEN 1 ELSE 0 END) AS won,
    SUM(CASE WHEN stage_name = 'Closed Lost' THEN 1 ELSE 0 END) AS lost,
    COUNT(*) AS total_closed,
    ROUND(SUM(CASE WHEN stage_name = 'Closed Won' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS win_rate_pct
FROM demo_db.gtm_case.opportunities
WHERE stage_name IN ('Closed Won', 'Closed Lost')
    AND close_date IS NOT NULL
GROUP BY 1
ORDER BY 1;
