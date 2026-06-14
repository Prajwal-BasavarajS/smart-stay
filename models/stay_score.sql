WITH connectivity AS (SELECT * FROM {{ ref('stg_connectivity') }}),
     value        AS (SELECT * FROM {{ ref('stg_value') }}),
     events       AS (SELECT * FROM {{ ref('stg_events') }}),

combined AS (
    SELECT
        v.neighbourhood,
        v.value_score,
        c.connectivity_score,
        COALESCE(e.events_score, 0) AS events_score
    FROM value v
    LEFT JOIN connectivity c USING (neighbourhood)
    LEFT JOIN events e       USING (neighbourhood)
    WHERE c.connectivity_score IS NOT NULL
),
scored AS (
    SELECT *,
        ROUND(0.50*value_score + 0.50*connectivity_score, 1) AS stay_score
    FROM combined
),
analyzed AS (
    SELECT *,
        RANK() OVER (ORDER BY stay_score DESC) AS overall_rank,
        ROUND(PERCENT_RANK() OVER (ORDER BY value_score), 2) AS value_percentile,
        ROUND(PERCENT_RANK() OVER (ORDER BY connectivity_score), 2) AS connectivity_percentile,
        NTILE(4) OVER (ORDER BY stay_score DESC) AS stay_quartile,
        ROUND(connectivity_score - AVG(connectivity_score) OVER (), 1) AS connectivity_vs_avg,
        ROUND(value_score - AVG(value_score) OVER (), 1) AS value_vs_avg
    FROM scored
)
SELECT *,
    CASE
        WHEN value_percentile >= 0.6 AND connectivity_percentile >= 0.6 THEN 'Hidden Gem'
        WHEN connectivity_percentile >= 0.6 AND value_percentile < 0.4  THEN 'Premium / Central'
        WHEN value_percentile >= 0.6 AND connectivity_percentile < 0.4  THEN 'Budget / Remote'
        ELSE 'Balanced'
    END AS neighbourhood_type
FROM analyzed
ORDER BY stay_score DESC