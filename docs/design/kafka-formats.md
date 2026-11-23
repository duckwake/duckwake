# Kafka message formats

Date: Nov 23, 2026. Status: proposed, not built. Roadmap issue
[#49](https://github.com/duckwake/duckwake/issues/49).

## The problem

A Kafka source reads one format: each message value is a JSON object whose fields are cast to the source's
`columns`. `"format": "debezium"` reads Debezium's JSON change events. Most Kafka estates also carry:

- **Avro and Protobuf** values, written by Confluent's serializers with the schema in a schema registry. Kafka
  Connect, Debezium included, commonly writes Avro this way.
- **Plain text** (log lines, one value per message), **CSV** records, and **raw bytes**.
- **JSON written by a registry serializer**, which puts five bytes before the object; the JSON format refuses
  every such message today.
- **Metadata** a view needs: the message's timestamp (the only event time a text message has) and its headers.
- **Avro or Protobuf keys.** Upsert and Debezium views read the message key as a JSON object or plain text
  today; a key written by a registry serializer is neither.

None of these values can be read today: a topic in any of them sends every message to the dead letters.

## The decision in one table

A source's format splits in two, as RisingWave (`FORMAT ... ENCODE ...`) and Materialize (`FORMAT ... ENVELOPE
...`) split it: the **encoding** of the bytes, and the **envelope** that says what a message means.

| Option | Values | Default |
| --- | --- | --- |
| `format` (the encoding) | `json`, `avro`, `protobuf`, `text`, `csv`, `bytes` | `json` |
| `envelope` | `none` (each message a row), `debezium` (change events, read as upserts by key) | `none` |
| `key_format` | `auto` (a JSON object or plain text, as today), `avro`, `protobuf` | `auto` |
| `schema_registry` | the registry's URL, with `schema_registry_username` and `schema_registry_password` | none |
| `subject`, `key_subject` | the registry subjects of the values and keys | `<topic>-value`, `<topic>-key` |

`"format": "debezium"` stays, meaning `json` with the `debezium` envelope, so existing sources do not change.
Upsert views (`upsert_key`) work over any encoding, since they key on columns.

## Encodings

### `text` and `bytes`

The value is one column: `text` gives `VARCHAR` (a value that is not valid UTF-8 is a dead letter, `invalid
utf-8`, not replaced characters), and `bytes` gives `BLOB`. The column is named by `columns` with a single entry,
or `value` without one. A view parses it with SQL:

```sql
CALL duckwake_create_source('app_logs', 'kafka', '{
  "brokers": "localhost:19092", "topic": "app_logs", "format": "text", "event_time": "_timestamp"}');
CALL duckwake_create_view('ops.errors_per_min',
  'SELECT window_start, regexp_extract(value, ''service=(\w+)'', 1) AS service, count(*) AS errors
   FROM TUMBLE(app_logs, _timestamp, INTERVAL 1 MINUTE)
   WHERE value LIKE ''%level=error%'' GROUP BY ALL');
```

### `csv`

Each message is one CSV record, with fields in the order of `columns`. Options: `csv_delimiter` (`,`),
`csv_quote` (`"`), `csv_null` (the text that means NULL; empty by default). No header row: a message that is the
header, or has the wrong number of fields, is a dead letter. The reader splits each record into its fields in
Rust (the `csv` crate, a new dependency: DuckDB parses CSV only from files, not one string at a time), and each
field is cast to its column's type as a JSON string value is, on the Arrow fast path when its form is on the fast
list and in SQL otherwise, so CSV and JSON agree on every value.

### `json` with a registry

A JSON value written by a registry serializer starts with a five-byte header: magic byte 0 and a 4-byte schema
id. No JSON text starts with byte 0, so the JSON format strips such a header whether or not `schema_registry` is
set, and reads the rest as today. The schema itself is not used: `columns` decide the types, as for plain JSON.

### `avro`

The value is Avro binary in the registry's wire format: magic byte 0, a 4-byte schema id, then the datum,
written with that id's schema (the **writer schema**).

- **Columns from the registry.** `create_source` fetches the subject's latest schema (`<topic>-value`, or
  `subject`) and derives `columns` from its top-level fields, which the user may narrow by listing `columns`.
  That schema, the **reader schema**, is stored with the source and fixed, like `columns` today.
- **Schema evolution by Avro's resolution rules.** Each message is decoded with its writer schema and resolved
  to the reader schema: a field the writer added is skipped, a field the writer dropped takes the reader
  schema's default, a widened type (`int` to `long`) is promoted. The view's columns never change under it.
- **Which compatibility that needs.** Here the reader schema is older than the writers', so producers may evolve
  only as the registry's `FORWARD` (or `FULL`) compatibility allows. Under `BACKWARD`, the registry's default,
  a producer may drop a field that has no default in the reader schema, and its messages cannot be resolved.
  `create_source` reads the subject's compatibility level and says so when it is not `FORWARD` or `FULL`.
- **When a schema cannot be resolved.** That is a property of the writer schema, not of one message: every
  message with that id would fail. So the view fails, naming the schema id and the field, rather than sending
  the rest of the topic to the dead letters. A message whose bytes do not decode under its own schema (a
  corrupt datum) is a dead letter, `avro: <reason>`, as for JSON.
- **Taking a new field.** `duckwake_add_source_column`, as for JSON, adds the field to the reader schema from
  the registry's latest version; the views that should read it are changed as today.
- **Without a registry:** `avro_schema` gives the schema inline, and messages are plain Avro binary with no
  header (one schema for the topic).
- **Types:** `boolean`, `int`, `long`, `float`, `double`, `string`, `bytes` and `fixed` (`BLOB`), `enum` and
  `uuid` (`VARCHAR`), and the logical types `decimal` (`DECIMAL`, refused above DuckDB's precision of 38),
  `date`, `time-*` (`TIME`), and `timestamp-*` and `local-timestamp-*` (`TIMESTAMP`, the type `event_time`
  needs; an instant is written in UTC; nanoseconds are cut to microseconds). A union of `null` and one type is
  that type, nullable; other unions are refused at `create_source`. Records, arrays and maps become JSON text
  in a `VARCHAR` column, read with DuckDB's JSON functions; native `STRUCT`, `LIST` and `MAP` columns wait until the operators are tested on them.

### `protobuf`

The value is a Protobuf message in the registry's wire format: magic byte 0, the 4-byte schema id, the message's
index path within the schema's file (a count and the indexes as zigzag varints, or a single 0 for the file's
first message), then the message.

- `message` names the message type when the file has several; the registry gives the schema as `.proto` text
  with its references, compiled in Rust (`protox`) into a descriptor and decoded dynamically
  (`prost-reflect`). Without a registry, `protobuf_descriptor` is the path of a compiled descriptor set
  (`protoc --descriptor_set_out`).
- **Evolution:** messages are decoded with the descriptor fixed at `create_source`, as the reader schema is for
  Avro. Protobuf decodes by field number, so new fields are skipped and removed ones read as absent. The writer
  schema (by id) is used only to check that the index path names the same message type; a field number reused
  with another type fails to decode, and that message is a dead letter.
- **Types:** scalars map to their DuckDB types, enums to their names, `google.protobuf.Timestamp` to
  `TIMESTAMP` (in UTC), the wrapper types to nullable scalars, `oneof` members to nullable columns, and messages and
  `repeated` fields to JSON text. Proto3 scalars without `optional` read as their default (0, `''`) when absent,
  as Protobuf defines; the docs say so, since it differs from JSON's NULL.

## Metadata columns

Every Kafka source gains columns a view can select by name; `*` leaves them out, so existing views do not
change. Ad hoc queries of a source (`duckwake sql`) show `_key`, `_partition` and `_offset` today, but a view
cannot select them:

| Column | Type | What |
| --- | --- | --- |
| `_key` | `VARCHAR` | The message key as text; a binary key as `\x` and hex digits |
| `_timestamp` | `TIMESTAMP` | The message's timestamp in UTC (create time, or log-append time if the topic is set so), the type `event_time` needs; NULL when the message has none |
| `_headers` | `VARCHAR` | The headers as a JSON object, each value as text, or as `\x` and hex digits when it is not UTF-8. rskafka hands headers over as a map, so of a repeated header name one value is kept. A native `MAP` waits with the other nested types (below). |
| `_partition`, `_offset` | `INTEGER`, `BIGINT` | Where it is in the topic |

`event_time` may name `_timestamp`, which gives text, CSV and bytes topics windows, and any topic whose payload has
no time of its own. Log-append time is set by the broker and is nearly in order within a partition; with create
time, late producers are late data, as with a field.

With `key_format` `avro` or `protobuf`, the key is decoded with its own schema (`key_subject`), and its fields
are matched to the upsert key columns by name, as a JSON key's fields are today. Upsert views and the `debezium`
envelope need it for tombstones, whose value is empty: only the key says which row to delete. (A Debezium delete
event also carries its `before` row, but the tombstone after it does not.)

## How decoding runs

- **In Rust, into Arrow.** Avro, Protobuf, text, bytes and the registry header are decoded in the reader, into
  the same typed Arrow batches the Arrow JSON path stages today (`append_record_batch`). For Avro, `arrow-avro`
  from arrow-rs (released with `arrow`, Apache-2.0; the release matching the `arrow` 60 that DuckDB's crate uses)
  is the first choice: it decodes straight to Arrow and reads the registry's framing, looking each id up in a
  `SchemaStore` that DuckWake fills from the registry (it has no registry client of its own). A spike checks its
  resolution rules and logical types against the reference `apache-avro` crate before it is chosen. There is
  no SQL decode for binary formats: a message the decoder refuses is a dead letter with the decoder's reason.
- **Dead letters of binary messages** keep the payload in the existing `VARCHAR` column as `\x` and hex digits,
  as PostgreSQL shows `bytea`, so `unhex(substr(payload, 3))` gives the bytes back. The dead-letter table does not
  change shape.
- **Schemas are cached in the lake.** A schema fetched by id goes into `__duckwake.schemas` (registry URL, id,
  text) in the epoch's transaction. Registry ids are immutable, so the cache never goes stale, and a restart, a
  reset, a backfill or `verify` replays the topic with the same schemas even if the registry is down or the
  subject was deleted. Only an id never seen needs the registry.
- **A registry that does not answer** is waited out like an unreachable broker
  ([#26](https://github.com/duckwake/duckwake/issues/26)): the view stays `running`, waiting on the registry,
  with the reason in `duckwake_jobs()` and a metric, and does not commit past the message it cannot decode. A
  registry that answers "no such id" is a dead letter, not a wait.
- **Exactly once is unchanged.** A message's rows depend only on its bytes and the schemas its ids name, which
  are fixed, so a reread after a crash gives the same rows. `verify` decodes with the same decoder: what it
  checks is the views, not the decoder, so the decoders get their own tests (below).
- **Shared readers** (`--shared-readers`) decode once per source, as for JSON.

## Operations

- **Registry authentication:** basic auth, the password as a reference (`env:NAME`, `file:/path`), and TLS with
  the same `ssl_ca_location` as the brokers or a `schema_registry_ssl_ca_location` of its own.
- **Metrics:** messages per encoding, dead letters by reason, schema fetches and cache hits, and time spent
  waiting on the registry.
- **`create_source` checks** what it can: the registry answers, the subject exists, its schema maps to columns,
  and the listed `columns` are in it.
- **New dependencies:** an HTTP client for the registry (`ureq` with rustls, blocking, small), `csv`,
  `arrow-avro`, `prost-reflect` and `protox`; each license checked by `cargo deny` and listed in
  `THIRD_PARTY_LICENSES.md`, as today.

## What is left out

- **JSON Schema validation.** A registry's JSON schemas are not used to type columns; `columns` do that.
- **Writing** Avro or Protobuf: DuckWake writes DuckLake tables, not topics.
- **Avro object container files** (a header and blocks per message): not what Kafka serializers write.
- **Native nested columns** (above), and Protobuf `Any` (as JSON text with its type URL, if asked for).

## Plan

Each step its own PR, with its gate in CI. Redpanda in the compose stack has a schema registry built in, so the
tests need no new service: its config gains a `schema_registry` listener on a published port.

1. **K1. The split and the simple encodings.** `format`/`envelope`/`key_format` with `debezium` kept as an
   alias; `text`, `bytes`, `csv`; metadata columns, and `_timestamp` as event time. Gate: the decode suite
   (`scripts/decode_test.sh`) gains a text and a CSV source over hostile payloads (invalid UTF-8, wrong field
   counts) whose rows and dead letters must match the batch recomputation, and the crash suite runs a text source
   windowed on `_timestamp`.
2. **K2. Avro.** The registry client and its cache in the lake, the wire format, resolution, types; JSON with the
   registry header. Spike first: `arrow-avro` against `apache-avro` on generated schemas and data. The producer
   (`duckwake produce`) learns `--format avro` and registers its schema. Gate: `FORMAT=avro` runs of the crash and
   upsert suites, with workers killed; a step that registers a forward-compatible version 2 (a field added, one
   with a default dropped) partway through, with old and new messages interleaved; a corrupt datum that must be
   a dead letter; a version that cannot be resolved, which must fail the view naming its id; a step with the
   registry stopped, the view waiting and then catching up exactly.
3. **K3. Protobuf.** The same, with `FORMAT=protobuf`, nested messages and an index path other than the first
   message.
4. **K4. Keys and Debezium with Avro.** `key_format`, keys matched to upsert key columns, the `debezium`
   envelope over Avro. Gate: the
   Debezium end-to-end suite with the Avro converter (Debezium's Connect images include Apicurio's converters,
   which can write the registry's wire format; to confirm in the spike).

Docs for each step: a page in [Work with data](../guides/kafka.md), the SQL reference's source options, a recipe
for text logs (K1) and for Avro (K2), the changelog.
