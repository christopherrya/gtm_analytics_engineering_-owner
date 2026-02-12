# Owner.com GTM Analytics — Data Product

## Objective
Build a scalable data product (SSOT) that supports recommendations to:
1. Scale GTM 2-3x within the next year
2. Improve CAC:LTV ratio

## Approach
1. **Current State Analysis** — Exploratory queries against the full dataset to understand the real funnel shape, conversion rates, loss patterns, and channel economics before making any assumptions.
2. **Theorize Solutions** — Based on findings, identify the highest-leverage opportunities.
3. **Architecture & Data Model** — Design the layered model (ingress → raw → staging → operational → reporting).
4. **Output Tables** — Build the SSOT data product in `demo_db.gtm_case`.

## Project Structure
```
owner-gtm-analytics/
├── README.md
├── analysis/
│   └── exploration/
│       ├── 01_discovery_queries.sql      -- Foundational exploratory queries
│       ├── 02_current_state_findings.md  -- Documented findings from exploration
│       └── 03_deep_dive_queries.sql      -- Follow-up queries based on findings
├── models/
│   ├── staging/                          -- (TBD after analysis complete)
│   ├── intermediate/
│   └── marts/
└── docs/
    └── data_quality_issues.md            -- Known issues with raw tables
```

## Source Data
- **Database:** `demo_db`
- **Schema:** `demo_db.gtm_case`
- **Warehouse:** `case_wh`

| Table | Rows | Date Range | Description |
|-------|------|------------|-------------|
| `LEADS` | 27,056 | Jul 2020* – Jul 2024 | Lead records from Salesforce |
| `OPPORTUNITIES` | 2,794 | Jan 2024 – Jul 2024 | Sales opportunities (post-demo booking) |
| `EXPENSES_ADVERTISING` | 6 | Jan–Jun 2024 | Monthly ad spend |
| `EXPENSES_SALARY_AND_COMMISSIONS` | 6 | Jan–Jun 2024 | Monthly sales team costs by channel |

*Dates in LEADS have a known year bug — stored as `0020`–`0024` instead of `2020`–`2024`.

## Status
- [x] Current state exploration (in progress)
- [ ] Deep-dive analysis
- [ ] Solution design
- [ ] Data model architecture
- [ ] Output tables built
