/*
    MODEL: ops_opportunities
    GRAIN: One row per opportunity (enriched with lead context where available)

    IMPORTANT DERIVED FIELDS:

    - deal_outcome: Classifies opportunities as 'Won', 'Lost', or 'Open' based on stage_name.
    - days_to_close: Number of days from opportunity creation to close (null if missing key dates).
    - days_demo_to_close: Number of days from demo to close (null if missing key dates).
    - is_closed_won / is_closed_lost / is_closed: Boolean flags for closed outcome types.
    - lead context columns: Each opportunity is enriched with lead-level info where a matching lead exists (via converted_opportunity_id).
        * channel: Inbound/Outbound (from original lead source)
        * predicted_sales_with_owner: Sales amount estimated at lead intake
        * predicted_sales_tier: Sales tier bucket based on prediction
        * location_count, connected_with_decision_maker, marketplaces, speed_to_lead_minutes, etc.
    - lead_form_submission_date: When the associated lead (if any) originally submitted their form.
    - Additional lead metrics: total_touch_count, speed_to_lead_minutes, marketplace_count.
    - These enrichments support pipeline analysis, channel attribution, and opportunity-level reporting downstream.

    NOTE: Only one lead is joined per opportunity (first by lead_id), to ensure 1:1 enrichment.
*/

WITH opportunities AS (

    SELECT * FROM {{ ref('entity_opportunities') }}

),

-- One lead per opportunity (first by lead_id) for 1:1 enrichment
leads AS (
    SELECT *
    FROM {{ deduplicate(
        relation=ref('ops_leads'),
        partition_by='converted_opportunity_id',
        order_by='lead_id'
    ) }}
    WHERE converted_opportunity_id IS NOT NULL
),

joined AS (

    SELECT
        -- Opportunity fields
        o.opportunity_id,
        o.stage_name,
        o.deal_outcome,
        o.lost_reason_c,
        o.closed_lost_notes_c,
        o.business_issue_c,
        o.how_did_you_hear_about_us_c,
        o.created_date,
        o.demo_held,
        o.demo_set_date,
        o.demo_time,
        o.close_date,
        o.last_sales_call_date_time,
        o.account_id,
        o.days_to_close,
        o.days_demo_to_close,
        o.is_closed_won,
        o.is_closed_lost,
        o.is_closed,

        -- Lead-sourced fields (enriching the opportunity with lead context)
        l.lead_id,
        l.channel,
        l.predicted_sales_with_owner,
        l.predicted_sales_tier,
        l.location_count,
        l.connected_with_decision_maker,
        l.total_touch_count,
        l.speed_to_lead_minutes,
        l.marketplace_count,
        l.form_submission_date                    AS lead_form_submission_date,

        -- Revenue model per deal
        -- LTV = (monthly subscription + 5% take rate on predicted sales) x lifetime months
        500.00                                                    AS monthly_subscription,
        COALESCE(l.predicted_sales_with_owner * 0.05, 0)          AS monthly_take_rate,
        500.00 + COALESCE(l.predicted_sales_with_owner * 0.05, 0) AS monthly_revenue,

        -- Loss categorization
        CASE
            WHEN o.is_closed_lost AND o.demo_held = FALSE
                THEN 'Pre-Demo Loss'
            WHEN o.is_closed_lost AND o.lost_reason_c IN ('Non-Responsive', 'Lack of Urgency')
                THEN 'Process/Follow-Up Loss'
            WHEN o.is_closed_lost AND o.lost_reason_c IN ('Price', 'Lost to Competitor')
                THEN 'Competitive Loss'
            WHEN o.is_closed_lost AND o.lost_reason_c IN ('POS Integration', 'Bad Fit')
                THEN 'Product Fit Loss'
            WHEN o.is_closed_lost
                THEN 'Other Loss'
            ELSE NULL
        END                                       AS loss_category

    FROM opportunities o
    LEFT JOIN leads l
        ON l.converted_opportunity_id = o.opportunity_id

)

SELECT * FROM joined
