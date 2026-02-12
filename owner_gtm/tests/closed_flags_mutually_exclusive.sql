-- Singular test: no row may have both is_closed_won and is_closed_lost true.
SELECT
    opportunity_id,
    is_closed_won,
    is_closed_lost
FROM {{ ref('entity_opportunities') }}
WHERE is_closed_won = TRUE
  AND is_closed_lost = TRUE
