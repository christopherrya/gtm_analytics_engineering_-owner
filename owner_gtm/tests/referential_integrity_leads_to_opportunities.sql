-- Singular test: every non-null converted_opportunity_id in entity_leads
-- must exist in entity_opportunities. Fails if any lead points to a missing opportunity.
SELECT
    l.lead_id,
    l.converted_opportunity_id
FROM {{ ref('entity_leads') }} l
LEFT JOIN {{ ref('entity_opportunities') }} o
    ON l.converted_opportunity_id = o.opportunity_id
WHERE l.converted_opportunity_id IS NOT NULL
  AND o.opportunity_id IS NULL
