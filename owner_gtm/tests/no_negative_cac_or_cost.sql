-- Singular test: CAC and cost columns must not be negative.
SELECT
    channel,
    cac,
    total_cost_6mo
FROM {{ ref('rpt_cac_ltv_by_channel') }}
WHERE (cac IS NOT NULL AND cac < 0)
   OR (total_cost_6mo IS NOT NULL AND total_cost_6mo < 0)
