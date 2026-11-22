-- The table name of analytics.game_totals in another schema. A view is named schema.table, so this is a view of
-- its own, with its own state and checkpoints. dbt model names are unique, so the table name is the alias.
{{ config(alias='game_totals') }}
SELECT game_id, count(*) AS n
FROM {{ ref('clicks') }}
WHERE amount >= 50
GROUP BY ALL
