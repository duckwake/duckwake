-- Events that arrive after their window closed go to the late-data table, which duckwake_verified needs too.
{{ config(late_data='lake.analytics.clicks_late') }}
SELECT window_start, game_id, count(*) AS n, sum(amount) AS revenue, max(amount) AS max_amount
FROM TUMBLE({{ ref('clicks') }}, ts, INTERVAL 10 SECOND)
GROUP BY ALL
