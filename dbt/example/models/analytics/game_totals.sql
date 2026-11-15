SELECT game_id, count(*) AS n, sum(amount) AS revenue, min(amount) AS lo, max(amount) AS hi
FROM {{ ref('clicks') }}
GROUP BY ALL
