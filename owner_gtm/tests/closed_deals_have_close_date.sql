-- Singular test: every closed opportunity (Won or Lost) must have a non-null close_date.
SELECT
    opportunity_id,
    stage_name,
    close_date
FROM {{ ref('entity_opportunities') }}
WHERE is_closed = TRUE
  AND close_date IS NULL
