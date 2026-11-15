SELECT window_start, game_id, count(*) AS n, sum(amount) AS revenue, max(amount) AS max_amount
FROM TUMBLE({{ ref('clicks') }}, ts, INTERVAL 10 SECOND)
GROUP BY ALL
