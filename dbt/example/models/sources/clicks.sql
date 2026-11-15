{#- The test topic's events. `--vars '{with_country: true}'` adds a column, as a producer that started sending
    one would need. -#}
{%- set columns = {'ts': 'TIMESTAMP', 'game_id': 'VARCHAR', 'user_id': 'VARCHAR', 'amount': 'DECIMAL(18,2)'} -%}
{%- if var('with_country', false) -%}{%- do columns.update({'country': 'VARCHAR'}) -%}{%- endif -%}
{{ config(
    brokers=env_var('DUCKWAKE_TEST_BROKERS', 'localhost:19092'),
    topic=var('topic', 'clicks'),
    event_time='ts',
    watermark_delay='5 seconds',
    idle_timeout='30 seconds',
    columns=columns
) }}
