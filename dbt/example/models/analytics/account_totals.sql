-- A view over a lake table that DuckWake does not maintain: it backfills the rows the table holds, then
-- follows its inserts, updates and deletes.
{{ config(max_rows=5000) }}
SELECT game_id, count(*) AS n, sum(amount) AS total FROM {{ source('shop', 'accounts') }} GROUP BY ALL
