SELECT ts, game_id, user_id, amount FROM {{ ref('clicks') }} WHERE amount >= {{ var('big_spend_min', 90) }}
