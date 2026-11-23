# Read a Kafka topic

A Kafka source tells views how to read a topic: where the brokers are, which columns each JSON message holds, and
which column is the event's time. A source holds no data. Every view that reads it keeps its own position in the
topic, so views can start, stop and be reset on their own.

## Register the topic

```sql
CALL duckwake_create_source('clicks', 'kafka', '{
  "brokers": "localhost:19092", "topic": "clicks",
  "event_time": "ts", "watermark_delay": "5 seconds",
  "columns": {"ts": "TIMESTAMP", "game_id": "VARCHAR", "user_id": "VARCHAR", "amount": "DECIMAL(18,2)"}}');
```

Each message is one JSON object, and each column is read from the field of the same name. A field the message
lacks is NULL. Fields the source does not list are ignored.

| Option | Default | Use it to |
| --- | --- | --- |
| `columns` | required | Name the fields to read and their DuckDB types. |
| `event_time` | required | Name the column that holds each event's time. Windows and the watermark use it. |
| `watermark_delay` | `30 seconds` | Wait for late events: a window closes once the newest event time read, minus this delay, passes its end. |
| `idle_timeout` | `60 seconds` | Let windows close when a partition goes quiet: after this long, the partition no longer holds the watermark back. |
| `start` | `earliest` | Start new views at the beginning of the topic, or at its end (`latest`). |

Every option is in the [SQL reference](../sql.md#sources).

## Look at the topic before writing a view

`duckwake sql` reads the newest 100,000 messages of a topic into a temporary table when a query names its
source, with the source's columns plus where each message came from and why it could not be decoded:

```sh
duckwake sql "SELECT _partition, _offset, ts, game_id, amount, _error FROM clicks ORDER BY ts DESC LIMIT 10"
```

The playground's SQL console does the same. Use it to check the columns and types before creating a view.

## Event time and the watermark

A view reads each partition in order and tracks the newest event time it has read. The watermark is the lowest of
those times across partitions, minus `watermark_delay`. Windows close by the watermark, so a view's results
depend on the events' times, not on when the view read them: replaying the topic gives the same rows.

- **Choosing the delay.** Set `watermark_delay` to how late your producers' events usually arrive. A longer
  delay catches more stragglers, and every window is written that much later.
- **Quiet partitions.** A partition with no new events would hold the watermark back. After `idle_timeout` it no
  longer counts, and windows close on the other partitions' times.
- **Late events.** An event for a window that has already closed is late. A view with a `late_data` table writes
  it there; without one it is counted (`rows_late`) and dropped. See
  [windows and aggregates](windows.md#late-events).

## Malformed messages

A message that is not valid JSON, has a value that does not fit its column's type, or lacks its event time, is a
dead letter. The view skips it and records it with its payload and the error:

```sql
SELECT error, count(*) FROM lake.__duckwake.dead_letter
WHERE job_id = 'analytics.clicks_per_min' GROUP BY ALL;
```

`FROM duckwake_jobs()` shows each view's dead letters in `dead`, and the `DuckWakeDeadLetters` alert fires when
more than 1% of a view's events are dead letters ([alerts](../alerts.md#duckwakedeadletters)).

## Topics whose messages are updates

When each message is the latest version of a record, such as an order or an account, read the topic as upserts:
the view keeps one row per key. See [changelogs and the latest row](changelogs.md), which also covers topics a
Debezium connector writes.

## Authentication and TLS

Options with the names Kafka clients use reach authenticated brokers. A password is never stored in the lake:
it is `env:NAME` or `file:/path`, read by each worker when it connects.

```sql
CALL duckwake_create_source('clicks', 'kafka', '{
  "brokers": "kafka-1:9094", "topic": "clicks", "event_time": "ts",
  "security_protocol": "sasl_ssl", "sasl_mechanism": "SCRAM-SHA-512",
  "sasl_username": "duckwake", "sasl_password": "env:KAFKA_PASSWORD",
  "columns": {"ts": "TIMESTAMP", "game_id": "VARCHAR"}}');
```

Every worker that may run the view needs that variable or file. The protocols, mechanisms and certificate
options are in the [CLI guide](../cli.md#views-and-sources).

## When the topic or the brokers are not there

A view whose topic does not exist yet, or whose brokers do not answer, waits instead of failing. `waiting_for`
in `FROM duckwake_jobs()` says what it waits for, and the view starts on its own once the topic or a broker is
there. Brokers that answer but refuse the connection, such as a wrong password, mark the view `failed` with the
reason. Fix it and start the view again.

## Add a column

A producer that starts sending a new field needs a new source column:

```sql
CALL duckwake_add_source_column('clicks', 'country', 'VARCHAR');
```

Views that already read the source keep their columns. Replace a view to use the new one
([change a view or a source](change-views.md)).

## Related

- [Recipe: a live per-minute dashboard](../recipes/live-dashboard.md)
- [Windows and aggregates](windows.md)
