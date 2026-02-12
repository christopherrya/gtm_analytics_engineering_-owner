# Owner.com GTM Analytics Engineering Case Study

## Scaling 2–3x While Improving CAC:LTV

---

## Section 1: Channel Economics

Owner.com operates two distinct acquisition channels — Inbound (advertising + inbound sales team) and Outbound (outbound sales team). Both channels exceed the 3:1 LTV:CAC benchmark, but with dramatically different efficiency profiles.

| Metric | Inbound | Outbound |
|--------|---------|----------|
| Deals Won | 494 | 127 |
| Total Cost (6mo) | $612,405 | $270,538 |
| CAC | $1,240 | $2,130 |
| Avg Monthly Subscription | $500 | $500 |
| Avg Monthly Take Rate | $172 | $256 |
| Avg Monthly Revenue | $672 | $756 |
| LTV (12mo) | $8,061 | $9,074 |
| LTV (24mo) | $16,121 | $18,148 |
| LTV (36mo) | $24,182 | $27,221 |
| **LTV:CAC (12mo)** | **6.5x** | **4.3x** |

**Validation Query** (`rpt_cac_ltv_by_channel`):

```sql
SELECT
    *
FROM demo_db.gtm_case_reporting.rpt_cac_ltv_by_channel  -- {{ ref('rpt_cac_ltv_by_channel') }}
ORDER BY channel;
```

---

## Section 2: Funnel Performance

The funnel summary reveals the most macro view of the pipeline. Inbound converts at nearly 4x the rate of outbound, driving 73% of all opportunities from just 42% of leads.

| Metric | Inbound | Outbound |
|--------|---------|----------|
| Total Leads | 11,285 | 15,771 |
| Converted to Opportunity | 2,050 | 744 |
| Lead→Opp Rate | 18.2% | 4.7% |
| Closed Won | 494 | 127 |
| Closed Lost | 1,272 | 488 |
| Win Rate (of closed) | 28.0% | 20.7% |
| Working Leads (stuck) | 5,644 | 9,059 |
| Bad Contact Data | 343 | 1,525 |

**Validation Query** (`rpt_funnel_summary`):

```sql
SELECT 
    *
FROM demo_db.gtm_case_reporting.rpt_funnel_summary; -- {{ ref('rpt_funnel_summary') }}
```

---

## Section 3: The Three Biggest Leaks

### Leak #1: Demo No-Shows (33% of all losses)

586 opportunities were lost because the demo never happened. At the current 28% demo-to-win rate, recovering even 25% of these no-shows would yield ~40 additional wins per 6-month period at zero incremental CAC.

| Lost Reason | Count | % of Losses |
|-------------|-------|-------------|
| No Demo Held | 605 | 34.4% |
| No Decision / Non-Responsive | 354 | 20.1% |
| Lack of Urgency | 185 | 10.5% |
| Price | 159 | 9.0% |
| POS Integration | 100 | 5.7% |
| Bad Fit | 91 | 5.2% |
| Lost to Competitor | 74 | 4.2% |
| Not a Decision Maker | 63 | 3.6% |

**Validation Query** (`ops_opportunities` — loss reason breakdown):

```sql
SELECT
    lost_reason_c,
    COUNT(*)                                               AS lost_deals,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2)     AS pct_of_losses,
    SUM(CASE WHEN channel = 'Inbound'  THEN 1 ELSE 0 END)  AS inbound,
    SUM(CASE WHEN channel = 'Outbound' THEN 1 ELSE 0 END)  AS outbound
FROM demo_db.gtm_case_operational.ops_opportunities
WHERE is_closed_lost
GROUP BY lost_reason_c
ORDER BY lost_deals DESC;
```

### Leak #2: Process & Follow-Up Failures (29% of losses)

Non-Responsive (337) and Lack of Urgency (172) together account for 509 lost deals. These prospects saw a demo and didn't say no — they went quiet. Better follow-up cadence and urgency-building could convert a meaningful percentage.

### Leak #3: The Working Lead Graveyard (14,703 leads, 54% of all leads)

More than half of all leads are stuck in Working status with no health classification. Without distinguishing a lead that went cold 6 months ago from one with activity last week, reps waste time on dead prospects while hot leads cool off.

| Status | Count | % of Total |
|--------|-------|------------|
| Working | 14,703 | 54.3% |
| Not Interested | 3,040 | 11.2% |
| Converted | 2,839 | 10.5% |
| Disqualified | 2,372 | 8.8% |
| Incorrect Contact Data | 1,867 | 6.9% |
| New | 1,294 | 4.8% |
| Demo Set | 528 | 2.0% |
| Sales Nurture | 334 | 1.2% |

**Validation Query** (`ops_leads` — status distribution):

```sql
SELECT
    status,
    COUNT(*)                                                    AS lead_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2)         AS pct_of_total,

    SUM(CASE WHEN channel = 'Inbound'  THEN 1 ELSE 0 END)      AS inbound,
    SUM(CASE WHEN channel = 'Outbound' THEN 1 ELSE 0 END)      AS outbound

FROM demo_db.gtm_case_operational.ops_leads --{{ ref('ops_leads') }}
GROUP BY status
ORDER BY lead_count DESC;
```

```sql
SELECT
    working_lead_health,
    COUNT(*)                                                   AS lead_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2)         AS pct_of_working,

    SUM(CASE WHEN channel = 'Inbound'  THEN 1 ELSE 0 END)      AS inbound,
    SUM(CASE WHEN channel = 'Outbound' THEN 1 ELSE 0 END)      AS outbound,

    ROUND(AVG(days_since_last_activity), 0)                    AS avg_days_inactive,
    ROUND(AVG(total_touch_count), 1)                           AS avg_touches

FROM demo_db.gtm_case_operational.ops_leads --{{ ref('ops_leads') }}
WHERE status = 'Working'
GROUP BY working_lead_health
ORDER BY lead_count DESC;
```


---

## Section 4: Monthly Efficiency Trends

While deal volume ramped from January through June, efficiency did as well, until April, then tapered off through June. 

### Inbound Channel — Monthly Performance

| Month | Leads | Wins | Cost/Lead | CAC |
|-------|-------|------|-----------|-----|
| Jan | 1,388 | 75 | $68 | $1,283 |
| Feb | 1,325 | 78 | $78 | $1,318 |
| Mar | 1,147 | 75 | $94 | $1,438 |
| **Apr** | **1,300** | **95** | **$79** | **$1,099** |
| May | 1,343 | 80 | $74 | $1,239 |
| Jun | 1,320 | 61 | $79 | $1,709 |

### Outbound Channel — Monthly Performance

| Month | Leads | Wins | Cost/Lead | CAC |
|-------|-------|------|-----------|-----|
| Jan | 2,697 | 8 | $14 | $4,759 |
| Feb | 2,572 | 21 | $17 | $2,050 |
| Mar | 1,819 | 21 | $24 | $2,108 |
| **Apr** | **2,263** | **25** | **$21** | **$1,881** |
| May | 2,807 | 25 | $17 | $1,924 |
| Jun | 2,307 | 20 | $22 | $2,500 |

**Key Insight:** April and May were quite efficient.  By June, CAC increased by 57% for Inbound and 33% for Outbound although lead volume stayed steady. This was the early signal that scaling would only be worthwhile with operational fixes.

**Validation Query** (`rpt_monthly_unit_economics` — pivoted by channel):

```sql
SELECT
    expense_month,

    -- Volume
    inbound_leads,
    outbound_leads,

    inbound_converted AS inbound_opps,
    outbound_converted AS outbound_opps,

    inbound_wins,
    outbound_wins,

    -- Unit costs
    inbound_cost_per_lead,
    outbound_cost_per_lead,

    inbound_cost_per_opportunity AS inbound_cost_per_opp,
    outbound_cost_per_opportunity AS outbound_cost_per_opp,

    inbound_cac,
    outbound_cac,
    
    (inbound_cac * inbound_wins + outbound_cac * outbound_wins) / NULLIF(inbound_wins + outbound_wins, 0) AS blended_cac

FROM demo_db.gtm_case_reporting.rpt_monthly_unit_economics --{{ ref('rpt_monthly_unit_economics') }}
ORDER BY expense_month;
```

---

## Section 5: Data Products — Operationalizing the Recommendations

The analysis identified five recommendations for scaling Owner.com 2–3x. (Aside from marginal marketing spend increase.) Rather than leaving these as one-time findings, **two data products** were built to make them operationally actionable, organized around  **effective capital capture**: first, knowing WHERE to deploy budget for maximum return; second, making sure that investment ACTUALLY converts into revenue.


| Metric | Value |
|--------|-------|
| **Open Pipeline at Risk** | ~$2.37M across 262 opportunities (152 demo no-shows + 110 no demo scheduled) |
| **Post-Demo Danger Zone** | ~$400K across 46 demo-held opps past the 21-day loss threshold |
| **Working Leads Unscored** | 14,703 leads now segmented: 5,246 Active / 4,586 Aging / 4,871 Stale |
| **Leads Contacted in 5 Min** | 0 (vs. 21× conversion benchmark) |
| **Expected Pipeline Value** | $3.23M probability-weighted ($792K opps + $2.44M leads) |

---

### Data Product 1: Pipeline Health Dashboard

*Models:* `rpt_pipeline_health` (aggregated summaries), `ops_pipeline_health` (row-level operational)
*Dashboard:* Pipeline Health Monitor with Overview, Opportunities, and Working Leads views — filterable by channel

The Pipeline Health Dashboard answers a very important question: **how much revenue is sitting in the pipeline right now that is actively being lost?** Not revenue that was failed to win — revenue that's failing to *work*. Demos scheduled but never held, opportunities aging without next steps, leads going stale without a lost reason. 

The dashboard reads from `rpt_pipeline_health` (27 pre-aggregated rows across channel × record type × pipeline segment × demo status × health status). The row-level data in `ops_pipeline_health` gives the record-by-record detail for operational fixes and CRM integration/Reverse ETL flexibility. 

![Pipeline Health Dashboard — Overview](images/pipeline_health1/Screenshot%202026-02-11%20at%208.53.43%20PM.png)
*Pipeline Health Dashboard: Overview with pipeline summary, demo status breakdown, and key metrics by channel.*

#### Open Pipeline: 409 Opportunities, $3.67M Forecasted Revenue/$792k Potential Accnual Revenue (Weighted Probability)

The pipeline splits into three groups by demo status — and the split reveals where value is leaking:

| Demo Status | Records | Forecasted Annual Revenue | Share of Pipeline | Potential Annual Revenue
|---|---|---|---|---|
| Demo Scheduled — Not Held | 152 | $1,351,891 | 36.9% | $293,443 |
| Demo Held | 147 | $1,297,404 | 35.4% | $290,242 |
| No Demo Scheduled | 110 | $1,017,607 | 27.8% | $208,362 |

**$1.35M in forecasted annual revenue/$293k in projected revenue is attached to demos that were booked but never happened**. Cases like this should be differentiated against as they are not cold leads — someone scheduled a demo and  the demo fell through. Without visibility into this status, these records age until they become the next batch of "No Demo Held" closed-lost outcomes.

> **Total addressable pipeline leakage: 262 opportunities, $2.37M forecasted annual revenue.** Luckily, action can be tabken on these opportunities with a demo booked in time to come. 

**Validation Query** (`rpt_pipeline_health` — demo status breakdown):

```sql
SELECT
    channel,
    demo_status,
    SUM(record_count)                                   AS records,
    ROUND(SUM(total_forecasted_annual_revenue), 0)      AS at_risk_annual_revenue,
    ROUND(SUM(record_count * avg_days_in_pipeline)
        / NULLIF(SUM(record_count), 0), 0)              AS wtd_avg_days_in_pipeline
FROM rpt_pipeline_health
WHERE record_type = 'Opportunity'
GROUP BY channel, demo_status
ORDER BY records DESC;
```

#### Post-Demo Follow-Up Risk: The 21-Day Danger Zone

Demo no-shows are the most visible leak, but there is a second pattern hiding in the data. Historically, 539 deals were lost to "No Decision / Non-Responsive" (354) and "Lack of Urgency" (185) — prospects who completed a demo, went quiet/weren't convinced by the sales process, and were marked closed-lost. These deals died pretty fast:

| Lost Reason | Deals Lost | Avg Days to Close | Annual Revenue Lost | Potential Revenue Lost |
|---|---|---|---|---|
| No Decision / Non-Responsive | 354 | 25 days | $16,688,734 | $656,654 |
| Lack of Urgency | 185 | 21 days | $8,793,917 | $337,241 |

**These deals averaged just 21–25 days from creation to closed-lost.** A demo was held, the prospect went quiet, and within 3–4 weeks the deal was dead. Combined, these two categories represent $25.5M in lost annual value — **more than any other loss category.**

The Pipeline Health Dashboard catches this pattern in the current open pipeline. Of the 147 demo-held opportunities, the 'health status segmentation' identifies which ones are entering the same danger zone:

| Health Status | Records | Avg Days in Pipeline | Annual Revenue | Potential Revenue | Risk Level |
|---|---|---|---|---|---|
| Fresh | 101 | 15 days | $882,144 | $200,887 |Low — still healthy |
| Maturing | 18 | 41–72 days | $179,537 | $39,561 |Low/Medium — **past the 21-day threshold** |
| Aging | 2 | 104–145 days | $14,440 | $3,480 |Medium — likely unrecoverable |
| Stalled | 25 | 49 days | $221,284 | $46,303 |High — 100% high risk |

**46 demo-held opportunities (Forecasted AR $415,261/Potential AR $89,344) are currently Maturing, Aging, or Stalled.** These are the active version of Leak #2. Without the health scoring, they would be weighted/categorized similarly to the 101 Fresh records. Now it is easier to track which opportunities held a demo and maturing or worse to see which deals are not converting post demo. 

539 losses over 6 months ≈ 90 per month, with a 3–4 week lifecycle, means ~45–90 records are in the active danger zone at any given snapshot. The current 46 fits this pattern.

![Pipeline Health Dashboard — Opportunities](images/pipeline_health1/Screenshot%202026-02-11%20at%208.53.56%20PM.png)
*Pipeline Health Dashboard: Opportunities view — demo status, health segmentation, and post-demo danger zone.*

**Validation Queries:**

Historical loss pattern (`ops_opportunities`):

```sql
SELECT
    lost_reason_c,
    COUNT(*)                                                AS lost_deals,
    ROUND(AVG(DATEDIFF('day', created_date, close_date)), 0) AS avg_days_to_close,
    ROUND(SUM(predicted_sales_with_owner * 12), 0)          AS total_lost_annual_value
FROM ops_opportunities
WHERE is_closed_lost
  AND lost_reason_c IN ('No Decision / Non-Responsive', 'Lack of Urgency')
GROUP BY lost_reason_c
ORDER BY lost_deals DESC;
```

Current danger zone (`ops_pipeline_health`):

```sql
SELECT
    health_status,
    pipeline_segment,
    risk_level,
    COUNT(*)                                            AS records,
    ROUND(AVG(days_in_pipeline), 0)                     AS avg_days,
    ROUND(AVG(days_in_current_stage), 0)                AS avg_days_in_stage,
    ROUND(SUM(forecasted_annual_revenue), 0)            AS annual_rev
FROM ops_pipeline_health
WHERE record_type = 'Opportunity'
  AND demo_held = TRUE
GROUP BY health_status, pipeline_segment, risk_level
ORDER BY health_status, pipeline_segment;
```

#### Working Lead Health: 14,703 Leads, $125M Forecasted AR, $2.44M Potential AR

The opportunity layer holds a larger pool of unconverted working leads. These are records that entered the funnel, received some level of outreach, but never converted to an opportunity. The 'health segmentation' classifies them by recency of engagement:

| Health Status | Leads | Forecasted AR | Potential AR | Avg Days in Pipeline | Avg Touches | DM Connected |
|---|---|---|---|---|---|---|
| Active | 5,246 | $48,673,812 | $845,788 | 130 days | 11.0 | 27% |
| Aging | 4,586 | $39,154,672 | $708,760 | 122 days | 7.9 | 21% |
| Stale | 4,871 | $37,453,640 | $882,251 | 189 days | 6.2 | 15% |

Active leads have the highest touch count (11.0) and the best decision-maker connection rate (27%) — these are the leads most likely to convert with sustained effort. Aging leads show decreasing engagement across every metric. Stale leads have been inactive for 90+ days, average only 6.2 touches, and have a 15% DM connection rate — the probability of conversion is low enough that continued rep time on these records might have negative ROI.

The risk distribution across the entire pipeline (opportunities + leads) shows this:

- **High risk:** 5,001 records (33%) — all Stale leads and Stalled/Aging opportunities
- **Medium risk:** 4,591 records (30%) — Aging leads and Maturing opportunities
- **Low risk:** 5,520 records (37%) — Active leads and Fresh opportunities

![Pipeline Health Dashboard — Working Leads](images/pipeline_health1/Screenshot%202026-02-11%20at%208.54.11%20PM.png)
*Pipeline Health Dashboard: Working Leads view — health segmentation (Active / Aging / Stale), forecasted value, and engagement metrics.*

**Validation Query** (`rpt_pipeline_health` — lead health segmentation):

```sql
SELECT
    health_status,
    SUM(record_count)                                   AS leads,
    ROUND(SUM(total_forecasted_annual_revenue), 0)      AS forecasted_annual_revenue,
    ROUND(SUM(record_count * avg_days_in_pipeline)
        / NULLIF(SUM(record_count), 0), 0)              AS wtd_avg_days,
    ROUND(SUM(record_count * avg_touch_count)
        / NULLIF(SUM(record_count), 0), 1)              AS wtd_avg_touches,
    ROUND(SUM(record_count * decision_maker_pct)
        / NULLIF(SUM(record_count), 0), 1)              AS wtd_dm_pct
FROM rpt_pipeline_health
WHERE record_type = 'Lead'
GROUP BY health_status
ORDER BY
    CASE health_status
        WHEN 'Active' THEN 1
        WHEN 'Aging' THEN 2
        WHEN 'Stale' THEN 3
    END;
```

#### Beyond the Dashboard: Reverse ETL to CRM

For this case study, the data product is delivered as a React dashboard (screenshot in images/)that reads from the pre-aggregated reporting layer. In a production environment, a more impactful delivery mechanism would be **reverse ETL directly into a CRM**.

The value-add from both data products isn't that the sales org can look at a dashboard showing 152 demo no-shows; it is that those 152 records get flagged *inside the tool reps already live in*. The models are already structured to support this:

- **`ops_pipeline_health`** is row-level with `record_id` — dervied every field (health status, risk level, demo status, days in pipeline, days in current stage) can be written back to its according CRM record as custom fields
- **`ops_leads`** holds the same structure for working leads — predicted sales tier, outreach status, actionability flags, and priority scores are all record-level and CRM-writeable. 

A reverse ETL layer would push these derived fields back to the CRM on a scheduled cadence through a tool such as Census or Hightouch. The result is that health scoring, risk classification, and triage recommendations appear natively in the rep's workflow — as derived fields on the Opportunity or Lead record, as filtered list views, or as trigger conditions for automated sequences.

**Trigger-Based Actions on Health Status Transitions**

The most valuable application of reverse ETL would be **event-driven triggers when a record's health status changes**. The 21-day danger zone analysis proves that post-demo deals die within 3–4 weeks. That means the moment a demo-held opportunity transitions from Fresh to Maturing, the system should fire an intervention:

- **Fresh → Maturing on a demo-held opportunity:** Auto-create a high-priority CRM task for the assigned rep: *"This deal had a demo and has gone quiet for 30+ days. Historical data shows deals in this state close lost at a rate of 90/month. Re-engage now or escalate."* Simultaneously push a Slack notification to the sales manager with the deal value and days since last activity.
- **Maturing → Stalled on any opportunity:** Trigger an escalation workflow — the deal is now 100% high risk. Flag it for manager review, pause any automated sequences that assume active engagement, and surface it in the daily stand-up queue.
- **Active → Aging on a working lead:** Queue the lead into a targeted re-engagement/nurture sequence before it crosses into Stale. 

None of these triggers require new models. The health status, risk level, and days-in-stage fields already exist in `ops_pipeline_health` and `ops_leads`. The reverse ETL layer watches for state changes between syncs and fires the appropriate action. A rep doesn't need to open a dashboard to know that an opportunity has gone quiet post-demo — the CRM record itself carries that signal, and the transition itself triggers the response.

The dashboard built for this case study demonstrates the analytical layer. The reverse ETL path demonstrates the operational one. Both functions read from the same dbt models — the only difference is how the information is delivered and then acted upon. 

---

### Data Product 2: Lead Outreach Priority Queue

*Model:* `rpt_lead_outreach_priority` (sources from `ops_leads`)
*Dashboard:* Speed-to-Lead Dashboard with action queue, slow outreach analysis, and priority triage

The Lead Outreach Priority Queue answers the conversion efficiency question: **is the capital we deploy actually being captured?** It focuses on every unconverted working lead, classifies them by outreach urgency, and separates actionable leads from expired ones — giving the sales ord a prioritized action list every morning.

| Status | Count | Description |
|--------|-------|-------------|
| **ACTION REQUIRED** | 60 | No contact, within 72hr window |
| **RE-ENGAGE** | 742 | Contacted late, needs recovery |
| **MONITOR** | 524 | Contacted 1–24hrs, follow up |
| **EXPIRED** | 3,093 | Past 72hr cutoff → nurture |
| **DATA QUALITY** | 942 | Outbound, no anchor timestamp |

![Speed-to-Lead Dashboard — Overview](images/speed_to_lead2/Screenshot%202026-02-11%20at%202.43.34%20PM.png)
*Speed-to-Lead Dashboard: Overview tab with pipeline status distribution, actionable leads by priority and value tier, and key metrics.*

#### Improve Speed to Lead for Inbound

Speed to lead is the highest-impact, lowest-cost lever. The data shows a huge operational failure: **100% of inbound leads (1,266 total) took over 1 hour to receive first contact**. Not a single lead was contacted within 5 or even 30 minutes.

**Slow Outreach Distribution** (from `rpt_lead_outreach_priority` — Inbound only):

| Response Delay | Leads | % of Contacted | Recommended Action |
|----------------|-------|----------------|-------------------|
| Late (1–24 hours) | 524 | 41.4% | Ensure consistent follow-up cadence |
| Very Late (1–3 days) | 492 | 38.9% | High-touch re-engagement needed |
| Critically Late (3+ days) | 250 | 19.7% | Recovery outreach — competitors explored |

> **Critical Finding: Zero Leads Contacted Within 5 Minutes.** According to the MIT Lead Response Management Study, leads contacted within 5 minutes are 21× more likely to convert than those contacted after 30 minutes. Owner.com's current median response time of ~2,300 minutes (38+ hours) means the majority of inbound leads have less chance of converting by the time a rep makes first contact. This is a high-impact, operationally fixable problem at zero incremental CAC.

![Speed-to-Lead Dashboard — Slow Outreach](images/speed_to_lead2/Screenshot%202026-02-11%20at%202.43.41%20PM.png)
*Slow Outreach tab: Response delay distribution (Late / Very Late / Critical) with operational recommendation for automated acknowledgment and lead routing.*

**No-Outreach Action Queue** (from `rpt_lead_outreach_priority`):

60 inbound leads have submitted a form but received zero sales contact. The model prioritizes them by urgency:

| Priority | Leads | Window | Action |
|----------|-------|--------|--------|
| P0 — Immediate | 16 | < 1 hour | Call/text immediately — golden hour window |
| P2 — At Risk | 44 | 1–3 days | Contact ASAP — approaching 72hr expiry |
| Expired | 3,093 | > 72 hours | Move to nurture campaign |

![Speed-to-Lead Dashboard — Action Queue](images/speed_to_lead2/Screenshot%202026-02-11%20at%202.43.49%20PM.png)
*Action Queue tab: Immediate Action Queue table — leads with no contact yet, sorted by value tier and wait time, with P0/P2/Expired priority breakdown.*

**Validation Query** (`rpt_lead_outreach_priority` — outreach priority distribution):

```sql
SELECT
    outreach_section,
    outreach_priority,
    dashboard_status,
    COUNT(*) AS leads,
    ROUND(SUM(predicted_sales_with_owner), 0) AS total_predicted_value,
    SUM(CASE WHEN is_actionable THEN 1 ELSE 0 END) AS actionable,
    SUM(CASE WHEN is_high_value_urgent THEN 1 ELSE 0 END) AS high_value_urgent
FROM rpt_lead_outreach_priority
GROUP BY 1, 2, 3
ORDER BY
    outreach_section,
    CASE outreach_priority
        WHEN 'P0 - Immediate' THEN 1
        WHEN 'P2 - At Risk' THEN 2
        WHEN 'Expired' THEN 3
        ELSE 4
    END;
```

#### Working Lead Health Scoring

With the introduction of 'Health Scoring' 14,703 working leads are no longer undifferentiated. Every lead now has a health classification (Active/Aging/Stale), a predicted sales tier using a standardized hybrid naming convention (Tier 1 — $10,000+ through Tier 6 — Under $500), and engagement metrics. The Sales org can filter to Active + Tier 1 first — the highest-value, most-engaged prospects.

**Working Lead Health Summary** (from `ops_pipeline_health`):

| Health | Total Leads | Inbound | Outbound | Recommended Action |
|--------|-------------|---------|----------|--------------------|
| Active | 5,246 | 1,620 | 3,626 | Immediate attention — recent engagement |
| Aging | 4,586 | 1,596 | 2,990 | Targeted re-engagement campaigns |
| Stale | 4,871 | 2,428 | 2,443 | Disqualify or move to nurture drip |

> **Prioritization Impact:** The 60 Inbound Tier 1 Active leads average $1,196/month in forecasted revenue with 15.8 touches. The 348 Outbound Tier 1 Aging leads at $1,142/month represent prospects at risk of going cold. Health scoring provides a solution for both for immediate action.

**Validation Query** (`ops_pipeline_health` — working lead health):

```sql
SELECT
    channel,
    health_status,
    predicted_sales_tier,
    COUNT(*) AS leads,
    ROUND(AVG(days_in_pipeline), 0) AS avg_days_inactive,
    ROUND(AVG(total_touch_count), 1) AS avg_touches,
    ROUND(AVG(forecasted_monthly_revenue), 2) AS avg_monthly_rev,
    SUM(CASE WHEN connected_with_decision_maker
        THEN 1 ELSE 0 END) AS dm_connected
FROM ops_pipeline_health
WHERE record_type = 'Lead'
GROUP BY channel, health_status, predicted_sales_tier
ORDER BY channel, health_status, leads DESC;
```

---

### Foundation: The dbt Pipeline

Both data products are powered by a complete single-source-of-truth pipeline: source data flows through **ingress → raw → staging → entity → operational → reporting** without row drops or duplication. Every model is connected via `ref()`, so dbt manages dependencies and any upstream schema change propagates cleanly. Business logic like the standardized predicted sales tier (Tier 1 — $10,000+ through Tier 6 — Under $500) is defined once in `ops_leads` and inherited by all downstream models.

**Infrastructure Validation** (row counts across all models):

```sql
SELECT 'ops_leads' AS model, COUNT(*) AS rows FROM ops_leads
UNION ALL
SELECT 'ops_opportunities', COUNT(*) FROM ops_opportunities
UNION ALL
SELECT 'ops_pipeline_health', COUNT(*) FROM ops_pipeline_health
UNION ALL
SELECT 'rpt_lead_outreach_priority', COUNT(*) FROM rpt_lead_outreach_priority
UNION ALL
SELECT 'rpt_channel_efficiency', COUNT(*) FROM rpt_channel_efficiency
UNION ALL
SELECT 'rpt_pipeline_health', COUNT(*) FROM rpt_pipeline_health
UNION ALL
SELECT 'rpt_monthly_unit_economics', COUNT(*) FROM rpt_monthly_unit_economics
ORDER BY model;
```

| Model | Expected Rows | Purpose |
|-------|---------------|---------|
| `ops_leads` | ~27,056 | All leads with derived metrics |
| `ops_opportunities` | ~2,794 | All opportunities with lead context |
| `ops_pipeline_health` | ~15,100+ | Open opps + working leads unified |
| `rpt_lead_outreach_priority` | ~5,361 | Outreach action queue |
| `rpt_channel_efficiency` | 12 + ~261 | Monthly summaries + pipeline detail |
| `rpt_pipeline_health` | Varies | Aggregated pipeline segments |
| `rpt_monthly_unit_economics` | 12 | 6 months × 2 channels |

---

## Section 6: Data Product Architecture

The analysis and data products are built on a production-grade data pipeline designed as a single source of truth for GTM metrics. All models were run in a Snowflake workspace under a dbt project.

| Layer | Purpose | What Happens |
|-------|---------|--------------|
| Ingress | Source reference | Explicit column selection from source tables. No transformations. |
| Raw | Deduplication | ROW_NUMBER() on primary keys; SELECT DISTINCT on reference tables. |
| Staging | Clean & cast | Year bug fix (0024→2024), currency parsing, Python list parsing, status standardization. |
| Entity | Business logic | Channel derivation, speed-to-lead, deal outcome flags, cycle time metrics. |
| Operational | Cross-entity joins | Lead→Opportunity enrichment, revenue model, loss categorization, cost allocation, lead health. |
| Reporting | Materialized outputs | CAC/LTV, funnel summary, monthly unit economics, pipeline health, channel efficiency. |

**Five reporting tables serve as the analytical foundation:**

- **rpt_cac_ltv_by_channel:**  GTM unit economics with LTV aggregates at 12/24/36 month horizons.
- **rpt_funnel_summary:** Full-funnel metrics by channel.
- **rpt_monthly_unit_economics:** Monthly trend data with per-channel cost-per-lead, cost-per-opportunity, and CAC.
- **rpt_pipeline_health:** Aggregated pipeline health by channel, record type, and segment with risk scoring.
- **rpt_channel_efficiency:** Rolling averages, MoM deltas, threshold alerts, and composite efficiency scoring.
- **rpt_lead_outreach_priority:** Prioritized queue of unconverted working leads for sales outreach. 

---

## Conclusion

Owner.com's GTM engine IS strong. The unit economics are healthy, the product-market fit is apparent in current win rates, and the present funnel generates consistent deal flow across Inbound and Outbound channels. The challenge ahead is not necessarily in finding new channels to grow into. 

Rather accumulating best practices to carry the load of increased amount of leads and opportunities with given increases to budget as input. 

The data tells a clear story: the biggest returns come from operational improvement, NOT ONLY from further investment into either channel. Fixing demo no-shows, improving speed to lead, and cleaning up the working lead pool are all high-impact, low-cost interventions that can be implemented into the Sales Pipeline quickly. Channel rebalancing toward inbound provides the volume scaling path with proven unit economics.

The two data products built here — Pipeline Health Dashboard and Channel Efficiency Monitor — transform these one-time findings into infrastructure that can be used by the Sales team. Every recommendation now has a corresponding model that makes it measurable, monitorable, and actionable on a daily basis. 

Feel free to use the validation queries throughout this document to read through outputs, piece by piece. Just make sure to replace demo_db/gtm_case* with your database and schema when running in your environment!

-- Christopher Silva
