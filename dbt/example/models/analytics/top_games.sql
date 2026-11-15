-- Added to a running deployment with --vars '{with_top_games: true}': a new view over a running one fills from
-- game_totals' current rows, then follows its changes.
{{ config(enabled=var('with_top_games', false)) }}
SELECT game_id, n, revenue FROM {{ ref('game_totals') }} WHERE n >= 10
