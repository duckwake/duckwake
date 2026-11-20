-- An upsert view: one row per user, its latest click by event time. Its table is a plain DuckLake table that
-- other views can read.
{{ config(upsert_key='user_id', upsert_version='ts') }}
SELECT user_id, ts, game_id, amount FROM {{ ref('clicks') }}
