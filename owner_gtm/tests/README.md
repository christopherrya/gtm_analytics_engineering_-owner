# Singular tests

These are **singular tests**: SQL that returns rows that violate a business or data-quality rule. If the query returns **0 rows**, the test passes; if it returns **any rows**, the test fails.

| Test | Rule | Model(s) |
|------|------|----------|
| `referential_integrity_leads_to_opportunities.sql` | Every non-null `converted_opportunity_id` in leads must exist in opportunities. | entity_leads, entity_opportunities |
| `closed_deals_have_close_date.sql` | Every closed opportunity (Won or Lost) must have a non-null `close_date`. | entity_opportunities |
| `closed_flags_mutually_exclusive.sql` | No row may have both `is_closed_won` and `is_closed_lost` true. | entity_opportunities |
| `no_negative_cac_or_cost.sql` | CAC and total cost must not be negative. | rpt_cac_ltv_by_channel |

Run all tests (schema + singular): `dbt test`
