-- A view over a view: DuckWake reads big_spend's table through its change feed.
SELECT game_id, count(*) AS n, sum(amount) AS total FROM {{ ref('big_spend') }} GROUP BY ALL
