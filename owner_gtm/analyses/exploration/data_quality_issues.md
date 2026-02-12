# Data Quality Issues — Raw Snowflake Tables

## Critical Issues

### 1. Year Bug in Date Columns
- **Tables affected:** `LEADS`, `OPPORTUNITIES`
- **Columns:** `FORM_SUBMISSION_DATE`, `CLOSE_DATE`, and likely all date/timestamp columns
- **Problem:** Years stored as `0020`–`0024` instead of `2020`–`2024`
- **Evidence:** `MIN(form_submission_date)` returns `0020-07-29`, `CLOSE_DATE` shows `0024-xx-xx`
- **Impact:** Any date arithmetic, cohort analysis, or time-series reporting will be incorrect
- **Fix in staging:** `DATEADD('year', 2000, column_name)`

### 2. Monetary Values Stored as Formatted Text
- **Tables affected:** `EXPENSES_ADVERTISING`, `EXPENSES_SALARY_AND_COMMISSIONS`
- **Columns:** `ADVERTISING`, `OUTBOUND_SALES_TEAM`, `INBOUND_SALES_TEAM`
- **Problem:** Values like `'US$    55 779,40'` — currency symbol, spaces as thousands separator, comma as decimal
- **Impact:** Cannot perform any math operations without parsing
- **Fix in staging:** Strip `'US$'`, remove spaces, replace `,` with `.`, cast to `DOUBLE`

### 3. Numeric Values Stored as Text with Comma Decimals
- **Table:** `LEADS`
- **Column:** `PREDICTED_SALES_WITH_OWNER`
- **Problem:** Values like `'426,83'` instead of `426.83`
- **Impact:** Cannot aggregate, compare, or use in calculations
- **Fix in staging:** `TRY_TO_DOUBLE(REPLACE(predicted_sales_with_owner, ',', '.'))`

### 4. Month Column as Text
- **Tables:** `EXPENSES_ADVERTISING`, `EXPENSES_SALARY_AND_COMMISSIONS`
- **Column:** `MONTH`
- **Problem:** Text format `'Jan-24'` instead of a `DATE` type
- **Impact:** Cannot join to other tables by date, cannot sort chronologically without parsing
- **Fix in staging:** Parse with `TO_DATE()` using month abbreviation + year mapping

## Moderate Issues

### 5. Stringified Python Lists in Text Columns
- **Table:** `LEADS`
- **Columns:** `MARKETPLACES_USED`, `ONLINE_ORDERING_USED`, `CUISINE_TYPES`
- **Problem:** Values like `"['Grubhub', 'Uber Eats', 'DoorDash']"` — a Python list serialized as string
- **Impact:** Cannot query individual values, cannot count items cleanly, cannot join/filter by marketplace
- **Recommendation:** Normalize into junction/bridge tables, or at minimum use Snowflake `ARRAY` / `VARIANT` types

### 6. No Explicit Channel Field on Leads
- **Table:** `LEADS`
- **Problem:** No column indicating whether a lead is Inbound or Outbound
- **Current workaround:** Infer from `FORM_SUBMISSION_DATE IS NOT NULL` → Inbound, otherwise Outbound
- **Risk:** This proxy may not be 100% accurate
- **Recommendation:** Add `CHANNEL` as a first-class field at data ingestion

### 7. Sparse Source Attribution on Opportunities
- **Table:** `OPPORTUNITIES`
- **Column:** `HOW_DID_YOU_HEAR_ABOUT_US_C`
- **Problem:** Mostly NULL/empty — only populated on a small fraction of records
- **Impact:** Cannot reliably attribute deals to specific ad channels, campaigns, or sources
- **Recommendation:** Improve CRM data capture discipline; consider UTM parameter tracking from form submissions

## Minor Issues

### 8. Inconsistent Status Values
- **Table:** `LEADS`
- **Column:** `STATUS`
- **Problem:** Near-duplicate values — `'Incorrect Contact Data'` and `'Incorrect_Contact_Data'` (1 record with underscores)
- **Fix in staging:** Standardize with `REPLACE` or mapping logic

### 9. No Direct Lead FK on Opportunities
- **Tables:** `LEADS` ↔ `OPPORTUNITIES`
- **Problem:** Relationship is via `LEADS.CONVERTED_OPPORTUNITY_ID → OPPORTUNITIES.OPPORTUNITY_ID`, which means the FK lives on the lead side. There is no `LEAD_ID` on the opportunities table.
- **Impact:** To get lead attributes for an opportunity, you must reverse-join. Only converted leads link to opportunities — unconverted leads have no opportunity context.
- **Recommendation:** Add `LEAD_ID` as a column on `OPPORTUNITIES` for cleaner joins
