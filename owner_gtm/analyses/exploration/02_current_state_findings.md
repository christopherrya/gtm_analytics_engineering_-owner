# Current State Findings — Discovery Phase

## Data Overview

| Metric | Value |
|--------|-------|
| Total leads | 27,056 |
| Total opportunities | 2,794 |
| Expense data coverage | Jan–Jun 2024 (6 months) |
| Opportunity date range | Jan 2 – Jul 10, 2024 |
| Lead date range | ~Jul 2020 – Jul 2024 (years need correction) |

---

## Funnel Summary

```
Leads (27,056)
  ├── Inbound:  11,285 (42%)  → 2,050 converted (18.2% rate)
  └── Outbound: 15,771 (58%)  →   744 converted (4.7% rate)
                                ─────
                    Total converted: 2,794 opportunities
                                        │
                              ┌──────────┴──────────┐
                              │                     │
                         Closed Won: 623       Closed Lost: 1,762
                           (22.3%)               (63.1%)
                                              Still Open: 409 (14.6%)
```

**Overall lead-to-win rate: ~2.3%** (623 won / 27,056 leads)

---

## Key Findings

### 1. Channel Economics — Inbound Converts 4x Better

| Channel | Leads | Converted | Conv Rate | Share of Leads | Share of Conversions |
|---------|-------|-----------|-----------|----------------|---------------------|
| Inbound | 11,285 | 2,050 | 18.2% | 42% | 73% |
| Outbound | 15,771 | 744 | 4.7% | 58% | 27% |

- Inbound produces nearly **3x more opportunities** despite being a smaller lead pool.
- Outbound consumes the majority of lead volume but converts at less than 1/4 the rate.
- **Open question:** What does CAC look like per channel when we factor in ad spend vs salary costs?

### 2. Biggest Funnel Leak — No Demo Held (33% of All Losses)

| Lost Reason | Count | % of Losses | Demo Held? |
|-------------|-------|-------------|------------|
| No Demo Held | 586 | 33.3% | No |
| Non-Responsive (post-demo) | 337 | 19.1% | Yes |
| Lack of Urgency | 172 | 9.8% | Yes |
| Price | 155 | 8.8% | Yes |
| POS Integration | 93 | 5.3% | Yes |
| Lost to Competitor | 67 | 3.8% | Yes |
| Bad Fit | 92 | 5.2% | Mixed |
| Not a Decision Maker | 63 | 3.6% | Mixed |

**586 opportunities were lost because the demo never happened.** These are leads that were qualified enough to become opportunities, had demos scheduled, but never showed up or the demo was never conducted.

The top 3 loss reasons — No Demo Held, Non-Responsive, Lack of Urgency — account for **62% of all losses** and are all operational/process problems, not product or pricing issues.

### 3. Lead Status Pool — 54% Still "Working"

| Status | Count | % |
|--------|-------|---|
| Working | 14,703 | 54.3% |
| Not Interested | 3,040 | 11.2% |
| Converted | 2,839 | 10.5% |
| Disqualified | 2,372 | 8.8% |
| Incorrect Contact Data | 1,867 | 6.9% |
| New | 1,294 | 4.8% |
| Demo Set | 528 | 2.0% |
| Sales Nurture | 334 | 1.2% |

- 14,703 leads sitting in "Working" status — are they actually being worked, or is this a catch-all?
- 1,867 leads (6.9%) have incorrect contact data — upstream data quality issue.
- 1,294 leads still marked "New" — untouched?

### 4. Win Rate Trend — Volume vs Quality Tradeoff

- January: High win rates (50-60%) but very low volume (ramp-up).
- Feb–May: Volume scaled to 80-130 deals closing per week, win rate compressed to ~20-25%.
- Mid-June: Win rate bottomed at 11.5%.
- Late Jun–Jul: Win rate recovered to 37-45% with moderate volume.

**This suggests scaling volume diluted deal quality — a key tension for the 2-3x growth goal.**

---

## Data Quality Issues Identified

1. **Year bug in LEADS dates** — `form_submission_date` stores years as `0020`–`0024` instead of `2020`–`2024`. Must add 2000 years in staging.
2. **Year bug in OPPORTUNITIES dates** — `close_date` shows `0024-` prefix. Need to verify all timestamp columns.
3. **Currency values stored as TEXT** — Expense tables use formatted strings like `'US$    55 779,40'`. Must parse to numeric.
4. **Comma-decimal numbers** — `PREDICTED_SALES_WITH_OWNER` uses comma as decimal separator (`'426,83'`). Must convert.
5. **Stringified Python lists** — `MARKETPLACES_USED`, `ONLINE_ORDERING_USED`, `CUISINE_TYPES` stored as text like `"['Grubhub', 'Uber Eats']"`.
6. **Month as text** — Expense tables use `'Jan-24'` format instead of proper dates.
7. **Sparse attribution** — `HOW_DID_YOU_HEAR_ABOUT_US_C` on opportunities is mostly empty.

---

## Open Questions for Deep Dive

- [ ] Lost reasons broken out by channel (inbound vs outbound) — do they lose differently?
- [ ] Speed to lead — how fast are SDRs/BDRs contacting inbound leads after form submission?
- [ ] Decision maker connection rate vs conversion rate — does DM access predict success?
- [ ] No-demo-held trend over time — getting better or worse?
- [ ] "Working" lead age — how long have these been sitting? Are they stale?
- [ ] Predicted sales distribution — what does the ICP look like for won vs lost deals?
- [ ] Expense-weighted CAC by channel
