//! Arrow decode of JSON messages (Phase A of the Arrow ingest plan): one pass in Rust from Kafka payloads to
//! typed columns, so staging can `append_record_batch` instead of parsing JSON in SQL per column.
//!
//! The result must equal the SQL decode (`decode_sql`) row for row. A value is decoded here only when
//! its JSON form is on a narrow fast list for the column type; any other form flags the message for the SQL
//! fallback, which decides value or dead letter exactly as before. Messages are decoded as one stream and
//! bisected when that stream fails, so a bad message costs extra decodes but never affects its neighbours:
//! every message ends up as exactly one fast row, one fallback message or one dead letter.
//!
//! DuckDB parses JSON with yyjson, which differs from arrow-json at the edges: it accepts NaN and Infinity,
//! rejects leading zeros and raw control characters in strings, and reads numbers as doubles. A message
//! arrow-json cannot decode alone therefore goes to the fallback rather than straight to the dead letters,
//! and forms arrow-json accepts but yyjson may not (anywhere in the object, not only declared fields) are
//! flagged.
use crate::sources::kafka::Msg;
use crate::lake::{ident, lit};
use crate::model::SourceOptions;
use anyhow::{bail, Result};
use arrow_json::reader::{ArrayDecoder, DecoderContext, DecoderFactory, Tape, TapeElement};
use arrow_json::ReaderBuilder;
use duckdb::arrow::array::{Array, ArrayRef, AsArray, BooleanArray, Int32Array, Int64Array, RecordBatch};
use duckdb::arrow::compute::filter_record_batch;
use duckdb::arrow::datatypes::{DataType, Field, FieldRef, Schema, SchemaRef, TimeUnit};
use duckdb::arrow::error::ArrowError;
use std::sync::{Arc, Mutex};

/// Dead-letter reason for a payload that is not a single JSON object (same text as the SQL path).
pub const INVALID_JSON: &str = "invalid json";

pub use duckwake_circuit::sql::TOMBSTONE;

/// What Debezium writes for a column it did not read: an unchanged TOASTed value of a PostgreSQL table without
/// `REPLICA IDENTITY FULL`. Binary columns get it base64-encoded, arrays as their one element.
pub const UNAVAILABLE: &str = "__debezium_unavailable_value";
const UNAVAILABLE_BASE64: &str = "X19kZWJleml1bV91bmF2YWlsYWJsZV92YWx1ZQ==";
/// `_err` of a Debezium event that carries `UNAVAILABLE`: its row cannot be rebuilt, and the view stops.
pub const UNAVAILABLE_ERR: &str = "unavailable value";

#[derive(Debug, Clone, Copy, PartialEq)]
enum Kind {
    Timestamp,
    Varchar,
    Boolean,
    Int { min: i64, max: i64 },
    Double,
    Decimal { precision: u8, scale: i8 },
}

impl Kind {
    fn parse(duck_type: &str) -> Option<(Kind, DataType)> {
        let t = duck_type.trim().to_ascii_uppercase();
        let int = |min, max, dt| Some((Kind::Int { min, max }, dt));
        match t.as_str() {
            "TIMESTAMP" => Some((Kind::Timestamp, DataType::Timestamp(TimeUnit::Microsecond, None))),
            "VARCHAR" | "TEXT" | "STRING" => Some((Kind::Varchar, DataType::Utf8)),
            "BOOLEAN" | "BOOL" => Some((Kind::Boolean, DataType::Boolean)),
            "TINYINT" => int(i8::MIN as i64, i8::MAX as i64, DataType::Int8),
            "SMALLINT" => int(i16::MIN as i64, i16::MAX as i64, DataType::Int16),
            "INTEGER" | "INT" => int(i32::MIN as i64, i32::MAX as i64, DataType::Int32),
            "BIGINT" => int(i64::MIN, i64::MAX, DataType::Int64),
            "DOUBLE" => Some((Kind::Double, DataType::Float64)),
            _ => {
                let inner = t.strip_prefix("DECIMAL(")?.strip_suffix(')')?;
                let (p, s) = inner.split_once(',')?;
                let (precision, scale): (u8, i8) = (p.trim().parse().ok()?, s.trim().parse().ok()?);
                (precision <= 38 && scale >= 0 && scale as u8 <= precision)
                    .then_some((Kind::Decimal { precision, scale }, DataType::Decimal128(precision, scale)))
            }
        }
    }

    /// Whether this JSON value's form decodes here exactly as the SQL path's `TRY_CAST(json_extract_string(..))`.
    fn fast(self, tape: &Tape<'_>, idx: u32) -> bool {
        match (self, tape.get(idx)) {
            (_, TapeElement::Null) => true,
            (Kind::Timestamp, TapeElement::String(s)) => plain_timestamp(tape.get_string(s)),
            (Kind::Varchar, TapeElement::String(_)) => true,
            (Kind::Boolean, TapeElement::True | TapeElement::False) => true,
            (Kind::Int { min, max }, TapeElement::Number(n)) => {
                let s = tape.get_string(n);
                s.bytes().all(|b| b.is_ascii_digit() || b == b'-') && s.parse::<i64>().is_ok_and(|v| v >= min && v <= max)
            }
            (Kind::Double, TapeElement::Number(n)) => plain_decimal(tape.get_string(n)).is_some_and(|(i, f)| i + f <= 15),
            // The SQL path sees the number through a double, so only digits a double holds exactly qualify.
            (Kind::Decimal { precision, scale }, TapeElement::Number(n)) => plain_decimal(tape.get_string(n))
                .is_some_and(|(i, f)| f <= scale as usize && i <= (precision - scale as u8) as usize && i + f <= 15),
            _ => false,
        }
    }
}

/// `YYYY-MM-DD HH:MM:SS[.ffffff]` (or `T` as separator), with no zone offset and at most microseconds.
fn plain_timestamp(s: &str) -> bool {
    let b = s.as_bytes();
    let digits = |r: std::ops::Range<usize>| b.get(r).is_some_and(|d| d.iter().all(u8::is_ascii_digit));
    if b.len() < 19
        || !(digits(0..4) && b[4] == b'-' && digits(5..7) && b[7] == b'-' && digits(8..10))
        || !(b[10] == b' ' || b[10] == b'T')
        || !(digits(11..13) && b[13] == b':' && digits(14..16) && b[16] == b':' && digits(17..19))
    {
        return false;
    }
    match &b[19..] {
        [] => true,
        [b'.', frac @ ..] => (1..=6).contains(&frac.len()) && frac.iter().all(u8::is_ascii_digit),
        _ => false,
    }
}

/// A JSON number without exponent: returns (significant integer digits, fraction digits).
fn plain_decimal(s: &str) -> Option<(usize, usize)> {
    if !json_number(s) || s.contains(['e', 'E']) {
        return None;
    }
    let s = s.strip_prefix('-').unwrap_or(s);
    let (int, frac) = s.split_once('.').unwrap_or((s, ""));
    Some((int.trim_start_matches('0').len(), frac.len()))
}

/// RFC 8259 number grammar (arrow-json's tape also takes forms such as `01` or `1.` that yyjson rejects).
fn json_number(s: &str) -> bool {
    let b = s.strip_prefix('-').unwrap_or(s).as_bytes();
    let digits = |b: &[u8]| b.iter().take_while(|c| c.is_ascii_digit()).count();
    let int = digits(b);
    if int == 0 || (int > 1 && b[0] == b'0') {
        return false;
    }
    let mut rest = &b[int..];
    if let [b'.', r @ ..] = rest {
        let n = digits(r);
        if n == 0 {
            return false;
        }
        rest = &r[n..];
    }
    if let [b'e' | b'E', r @ ..] = rest {
        let r = r.strip_prefix(b"+").or_else(|| r.strip_prefix(b"-")).unwrap_or(r);
        let n = digits(r);
        if n == 0 {
            return false;
        }
        rest = &r[n..];
    }
    rest.is_empty()
}

/// Checks what arrow-json's tokenizer does not: it treats commas as optional separators (`{"a":1 "b":2}`,
/// `[1,,2]`, `{"a":1,}`) and accepts raw control characters inside strings; yyjson rejects both. Strings,
/// escapes, literals, colons and nesting are validated by arrow-json itself, and numbers by `json_number`.
/// Also rejects more than one top-level value, so the stream decode sees one value per message.
fn separators_ok(text: &str) -> bool {
    #[derive(PartialEq, Clone, Copy)]
    enum Prev {
        Start,
        Open,
        Comma,
        Colon,
        Value,
        Done,
    }
    let b = text.as_bytes();
    let (mut prev, mut depth, mut i) = (Prev::Start, 0usize, 0);
    let value_start = |p: Prev| matches!(p, Prev::Start | Prev::Open | Prev::Comma | Prev::Colon);
    while i < b.len() {
        match b[i] {
            b' ' | b'\n' | b'\r' | b'\t' => {}
            b'"' => {
                if !value_start(prev) {
                    return false;
                }
                i += 1;
                while i < b.len() && b[i] != b'"' {
                    match b[i] {
                        b'\\' => i += 1,
                        c if c < 0x20 => return false,
                        _ => {}
                    }
                    i += 1;
                }
                prev = if depth == 0 { Prev::Done } else { Prev::Value };
            }
            b'{' | b'[' => {
                if !value_start(prev) {
                    return false;
                }
                depth += 1;
                prev = Prev::Open;
            }
            b'}' | b']' => {
                if !matches!(prev, Prev::Open | Prev::Value) || depth == 0 {
                    return false;
                }
                depth -= 1;
                prev = if depth == 0 { Prev::Done } else { Prev::Value };
            }
            b',' => {
                if prev != Prev::Value {
                    return false;
                }
                prev = Prev::Comma;
            }
            b':' => {
                if prev != Prev::Value {
                    return false;
                }
                prev = Prev::Colon;
            }
            _ => {
                // A number or literal: one token up to the next separator.
                if !value_start(prev) {
                    return false;
                }
                while i + 1 < b.len() && !matches!(b[i + 1], b' ' | b'\n' | b'\r' | b'\t' | b',' | b':' | b'}' | b']' | b'{' | b'[' | b'"') {
                    i += 1;
                }
                prev = if depth == 0 { Prev::Done } else { Prev::Value };
            }
        }
        i += 1;
    }
    true
}

#[derive(Debug)]
struct Column {
    name: String,
    kind: Kind,
}

/// Wraps the reader's own decoder for the root object. Before decoding it scans each row's keys on the tape
/// and flags rows that hold a value outside the fast list, a duplicate declared key (SQL keeps the first,
/// Arrow the last) or no event time; after decoding it also flags any non-null input that came out NULL.
#[derive(Debug)]
struct Factory {
    columns: Arc<Vec<Column>>,
    event_time: usize,
    flags: Arc<Mutex<Vec<bool>>>,
}

impl DecoderFactory for Factory {
    fn make_default_decoder(
        &self,
        ctx: &DecoderContext,
        field: &FieldRef,
        is_nullable: bool,
    ) -> Result<Option<Box<dyn ArrayDecoder>>, ArrowError> {
        if !field.name().is_empty() {
            return Ok(None);
        }
        Ok(Some(Box::new(RootDecoder {
            inner: ctx.make_builtin_decoder(field, is_nullable)?,
            columns: self.columns.clone(),
            event_time: self.event_time,
            flags: self.flags.clone(),
        })))
    }
}

struct RootDecoder {
    inner: Box<dyn ArrayDecoder>,
    columns: Arc<Vec<Column>>,
    event_time: usize,
    flags: Arc<Mutex<Vec<bool>>>,
}

impl ArrayDecoder for RootDecoder {
    fn decode(&mut self, tape: &Tape<'_>, pos: &[u32]) -> Result<ArrayRef, ArrowError> {
        let n = self.columns.len();
        let mut flags = vec![false; pos.len()];
        let mut values = vec![None; pos.len() * n];
        for (row, &p) in pos.iter().enumerate() {
            let TapeElement::StartObject(end) = tape.get(p) else { continue };
            if (p..end).any(|i| matches!(tape.get(i), TapeElement::Number(s) if !json_number(tape.get_string(s)))) {
                flags[row] = true;
            }
            let seen = &mut values[row * n..(row + 1) * n];
            let mut cur = p + 1;
            while cur < end {
                let TapeElement::String(key) = tape.get(cur) else { break };
                let v = cur + 1;
                let name = tape.get_string(key);
                if let Some(c) = self.columns.iter().position(|c| c.name == name) {
                    if seen[c].is_some() || !self.columns[c].kind.fast(tape, v) {
                        flags[row] = true;
                    }
                    seen[c].get_or_insert(v);
                }
                cur = tape.next(v, "value")?;
            }
            if seen[self.event_time].is_none_or(|v| matches!(tape.get(v), TapeElement::Null)) {
                flags[row] = true;
            }
        }
        let out = self.inner.decode(tape, pos)?;
        let s = out.as_struct();
        for (c, col) in s.columns().iter().enumerate() {
            for (row, flag) in flags.iter_mut().enumerate() {
                if let Some(v) = values[row * n + c] {
                    *flag |= col.is_null(row) && !matches!(tape.get(v), TapeElement::Null);
                }
            }
        }
        *self.flags.lock().unwrap() = flags;
        Ok(out)
    }
}

/// The decode of one run of messages (a fetch). `batch` has `_partition`, `_offset` and the source columns.
#[derive(Debug)]
pub struct Decoded {
    pub batch: RecordBatch,
    /// Indexes of messages the SQL decode must handle.
    pub fallback: Vec<usize>,
    /// Indexes of messages without a payload (Kafka tombstones), staged by the SQL path as `TOMBSTONE` rows.
    pub dead: Vec<usize>,
    /// Extra stream decodes spent isolating bad messages.
    pub bisections: usize,
}

pub struct ArrowDecoder {
    columns: Arc<Vec<Column>>,
    event_time: usize,
    schema: SchemaRef,
    out_schema: SchemaRef,
}

impl ArrowDecoder {
    /// `None` when a column type is not on the fast list; such a source keeps the SQL decode.
    pub fn new(opts: &SourceOptions) -> Result<Option<Self>> {
        let mut columns = Vec::new();
        let mut fields = Vec::new();
        for (name, t) in &opts.columns {
            let Some((kind, dt)) = Kind::parse(t) else { return Ok(None) };
            columns.push(Column { name: name.clone(), kind });
            fields.push(Field::new(name, dt, true));
        }
        // Debezium's envelope nests the row, and a source without an event time has nothing to check here.
        let Some(et) = opts.event_time.as_deref().filter(|_| !opts.is_debezium()) else { return Ok(None) };
        let Some(event_time) = columns.iter().position(|c| c.name == et) else { bail!("event_time {et:?} is not a column") };
        let mut out = vec![
            Field::new("_partition", DataType::Int32, false),
            Field::new("_offset", DataType::Int64, false),
            Field::new("_key", DataType::Utf8, true),
        ];
        out.extend(fields.iter().cloned());
        Ok(Some(ArrowDecoder {
            columns: Arc::new(columns),
            event_time,
            schema: Arc::new(Schema::new(fields)),
            out_schema: Arc::new(Schema::new(out)),
        }))
    }

    pub fn decode(&self, msgs: &[Msg]) -> Result<Decoded> {
        let mut d = Decoded {
            batch: RecordBatch::new_empty(self.out_schema.clone()),
            fallback: Vec::new(),
            dead: Vec::new(),
            bisections: 0,
        };
        // The SQL path reads payloads as lossy UTF-8; a missing payload (tombstone) is not an object.
        let texts: Vec<Option<std::borrow::Cow<str>>> = msgs.iter().map(|m| m.payload.as_deref().map(String::from_utf8_lossy)).collect();
        let mut idx = Vec::with_capacity(msgs.len());
        for (i, t) in texts.iter().enumerate() {
            match t {
                None => d.dead.push(i),
                Some(t) if !separators_ok(t) => d.fallback.push(i),
                Some(_) => idx.push(i),
            }
        }
        let mut parts = Vec::new();
        self.decode_range(&texts, &idx, &mut parts, &mut d)?;
        let batches = parts
            .into_iter()
            .map(|(rows, batch)| self.with_source_columns(msgs, &rows, batch))
            .collect::<Result<Vec<_>>>()?;
        d.batch = duckdb::arrow::compute::concat_batches(&self.out_schema, &batches)?;
        d.dead.sort_unstable();
        d.fallback.sort_unstable();
        Ok(d)
    }

    /// Decodes `idx` as one stream. A clean pass yields one row per message; otherwise the range is split
    /// in half and each half decoded afresh, down to single messages, which go to the SQL fallback.
    fn decode_range(
        &self,
        texts: &[Option<std::borrow::Cow<str>>],
        idx: &[usize],
        parts: &mut Vec<(Vec<usize>, RecordBatch)>,
        d: &mut Decoded,
    ) -> Result<()> {
        if idx.is_empty() {
            return Ok(());
        }
        if let Some((batch, flags)) = self.try_decode(texts, idx)? {
            let keep: Vec<bool> = flags.iter().map(|f| !f).collect();
            d.fallback.extend(idx.iter().zip(&flags).filter(|(_, f)| **f).map(|(i, _)| *i));
            let rows: Vec<usize> = idx.iter().zip(&keep).filter(|(_, k)| **k).map(|(i, _)| *i).collect();
            let batch = if rows.len() == idx.len() { batch } else { filter_record_batch(&batch, &BooleanArray::from(keep))? };
            parts.push((rows, batch));
            return Ok(());
        }
        if idx.len() == 1 {
            d.fallback.push(idx[0]);
            return Ok(());
        }
        d.bisections += 2;
        let (a, b) = idx.split_at(idx.len() / 2);
        self.decode_range(texts, a, parts, d)?;
        self.decode_range(texts, b, parts, d)
    }

    /// One stream decode; `None` unless it ends cleanly with exactly one row per message.
    fn try_decode(&self, texts: &[Option<std::borrow::Cow<str>>], idx: &[usize]) -> Result<Option<(RecordBatch, Vec<bool>)>> {
        let flags = Arc::new(Mutex::new(Vec::new()));
        let factory = Factory { columns: self.columns.clone(), event_time: self.event_time, flags: flags.clone() };
        let mut dec = ReaderBuilder::new(self.schema.clone())
            .with_batch_size(idx.len() + 1)
            .with_coerce_primitive(false)
            .with_ignore_type_conflicts(true)
            .with_decoder_factory(Arc::new(factory))
            .build_decoder()?;
        for &i in idx {
            let text = texts[i].as_deref().unwrap_or_default();
            // A message with no JSON value at all would vanish from the stream instead of failing.
            if text.trim_ascii().is_empty() {
                return Ok(None);
            }
            for buf in [text.as_bytes(), b"\n"] {
                match dec.decode(buf) {
                    Ok(n) if n == buf.len() => {}
                    _ => return Ok(None),
                }
            }
        }
        if dec.has_partial_record() {
            return Ok(None);
        }
        let batch = match dec.flush() {
            Ok(Some(b)) if b.num_rows() == idx.len() => b,
            _ => return Ok(None),
        };
        let flags = std::mem::take(&mut *flags.lock().unwrap());
        if flags.len() != idx.len() {
            return Ok(None);
        }
        Ok(Some((batch, flags)))
    }

    fn with_source_columns(&self, msgs: &[Msg], rows: &[usize], batch: RecordBatch) -> Result<RecordBatch> {
        let part: ArrayRef = Arc::new(Int32Array::from_iter_values(rows.iter().map(|&i| msgs[i].partition)));
        let off: ArrayRef = Arc::new(Int64Array::from_iter_values(rows.iter().map(|&i| msgs[i].offset)));
        let key: ArrayRef = Arc::new(duckdb::arrow::array::StringArray::from_iter(rows.iter().map(|&i| msgs[i].key_text())));
        let mut cols = vec![part, off, key];
        cols.extend(batch.columns().iter().cloned());
        Ok(RecordBatch::try_new(self.out_schema.clone(), cols)?)
    }
}

/// A decoded batch shaped like the staging table `(_partition, _offset, payload, _key, <columns>, _err)`:
/// fast rows keep no payload and have no error.
pub fn staging_batch(batch: &RecordBatch) -> Result<RecordBatch> {
    let null = duckdb::arrow::array::new_null_array(&DataType::Utf8, batch.num_rows());
    let mut fields: Vec<FieldRef> = batch.schema().fields().iter().cloned().collect();
    let mut arrays: Vec<ArrayRef> = batch.columns().to_vec();
    fields.insert(2, Arc::new(Field::new("payload", DataType::Utf8, true)));
    arrays.insert(2, null.clone());
    fields.push(Arc::new(Field::new("_err", DataType::Utf8, true)));
    arrays.push(null);
    Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)?)
}

/// True for payloads that parse as a JSON object. `json_type` raises on malformed input and DuckDB does not
/// short-circuit AND, so the CASE keeps it away from rows that are not valid JSON.
pub const IS_JSON_OBJECT: &str = "CASE WHEN json_valid(payload) THEN json_type(payload) = 'OBJECT' ELSE false END";

/// Builds the decode query for raw Kafka rows `(_partition, _offset, payload, _key)` already known to be JSON
/// objects. Adds the typed source columns and `_err` (NULL when the row is usable).
pub fn decode_sql(src: &crate::model::SourceDef, ok_table: &str) -> String {
    if src.options.is_debezium() {
        return debezium_sql(src, ok_table);
    }
    let o = &src.options;
    let field = |c: &str| format!("json_extract_string(payload, {})", lit(&format!("$.\"{c}\"")));
    let cols: Vec<String> = o
        .columns
        .iter()
        .map(|(c, t)| format!("TRY_CAST({} AS {t}) AS {}", field(c), ident(c)))
        .collect();
    let mut checks = Vec::new();
    if let Some(et) = &o.event_time {
        checks.push(format!("WHEN TRY_CAST({} AS TIMESTAMP) IS NULL THEN 'missing or invalid event time {et}'", field(et)));
    }
    for (c, t) in &o.columns {
        checks.push(format!(
            "WHEN {f} IS NOT NULL AND TRY_CAST({f} AS {t}) IS NULL THEN {}",
            lit(&format!("type conflict: {c} is not {t}")),
            f = field(c)
        ));
    }
    format!(
        "SELECT _partition, _offset, payload, _key, {}, CASE {} ELSE NULL END AS _err FROM {ok_table}",
        cols.join(", "),
        checks.join(" ")
    )
}

/// The message key of a raw row as the source's views read it: `_key`, or for Debezium its payload.
pub fn key_sql(src: &crate::model::SourceDef) -> &'static str {
    if src.options.is_debezium() { debezium_key() } else { "_key" }
}

/// A message key as the upsert reads it: Debezium's key with `schemas.enable` is `{"schema": ..., "payload":
/// {"id": 1}}`, and the key is its payload. `json_type` raises on text that is not JSON, hence the nested CASE.
fn debezium_key() -> &'static str {
    "CASE WHEN json_valid(_key) THEN CASE WHEN json_type(_key) = 'OBJECT' AND json_exists(_key, '$.schema') \
       AND json_type(_key, '$.payload') = 'OBJECT' THEN json_extract(_key, '$.payload')::VARCHAR ELSE _key END \
     ELSE _key END"
}

/// A Debezium field `f` (text) as DuckDB type `t`. Debezium sends `timestamp` columns as numbers
/// (`Timestamp` ms, `MicroTimestamp` µs, `NanoTimestamp` ns by `time.precision.mode`) and `date` ones as days
/// since 1970-01-01; a number is read in the unit its size fits, as RisingWave does: right for 1973 to 5138.
/// Text (`ZonedTimestamp`, or `decimal.handling.mode=string`) casts as usual.
fn debezium_cast(f: &str, t: &str) -> String {
    let n = format!("TRY_CAST({f} AS BIGINT)");
    let upper = t.trim().to_ascii_uppercase();
    if upper == "TIMESTAMP" || upper == "DATETIME" {
        format!(
            "CASE WHEN regexp_full_match({f}, '-?[0-9]+') THEN make_timestamp(CASE WHEN abs({n}) < 100000000000000 \
             THEN {n} * 1000 WHEN abs({n}) < 100000000000000000 THEN {n} ELSE {n} // 1000 END) ELSE TRY_CAST({f} AS {t}) END"
        )
    } else if upper == "DATE" {
        format!("CASE WHEN regexp_full_match({f}, '-?[0-9]+') THEN DATE '1970-01-01' + {n}::INTEGER ELSE TRY_CAST({f} AS {t}) END")
    } else {
        format!("TRY_CAST({f} AS {t})")
    }
}

/// The decode of Debezium change events. The envelope (`schema`/`payload` when `schemas.enable` is on) holds
/// `op` and the row: `c` (insert), `r` (snapshot read) and `u` (update) carry it in `after` and are upserts;
/// `d` (delete) carries the old row in `before`, staged as a `TOMBSTONE` with it so a delete without a message
/// key still finds its key. `before` is never the row to retract: the view's own state is. Anything else (`t`
/// truncate, `m` message, a value that is not a change event) is a dead letter that names it, and an event
/// carrying `UNAVAILABLE` is staged as `UNAVAILABLE_ERR`, which stops its views.
fn debezium_sql(src: &crate::model::SourceDef, ok_table: &str) -> String {
    let o = &src.options;
    let field = |c: &str| format!("json_extract_string(__row, {})", lit(&format!("$.\"{c}\"")));
    let cols: Vec<String> =
        o.columns.iter().map(|(c, t)| format!("{} AS {}", debezium_cast(&field(c), t), ident(c))).collect();
    let unavailable: Vec<String> = o
        .columns
        .iter()
        .map(|(c, _)| format!("contains({f}, {}) OR {f} = {}", lit(UNAVAILABLE), lit(UNAVAILABLE_BASE64), f = field(c)))
        .collect();
    let mut checks = vec![
        "WHEN __op IS NULL THEN 'not a Debezium change event: no op'".to_string(),
        "WHEN __op NOT IN ('c', 'r', 'u', 'd') THEN 'Debezium operation ' || __op || ' is not applied'".to_string(),
        format!("WHEN __op = 'd' THEN {}", lit(TOMBSTONE)),
        "WHEN __row IS NULL OR json_type(__row) <> 'OBJECT' THEN 'Debezium event without an after row'".to_string(),
        format!("WHEN {} THEN {}", unavailable.join(" OR "), lit(UNAVAILABLE_ERR)),
    ];
    if let Some(et) = &o.event_time {
        checks.push(format!(
            "WHEN {} IS NULL THEN 'missing or invalid event time {et}'",
            debezium_cast(&field(et), "TIMESTAMP")
        ));
    }
    for (c, t) in &o.columns {
        checks.push(format!(
            "WHEN {f} IS NOT NULL AND {} IS NULL THEN {}",
            debezium_cast(&field(c), t),
            lit(&format!("type conflict: {c} is not {t}")),
            f = field(c)
        ));
    }
    let envelope = "CASE WHEN json_exists(payload, '$.schema') AND json_type(payload, '$.payload') = 'OBJECT' \
                    THEN json_extract(payload, '$.payload') ELSE payload::JSON END";
    format!(
        "SELECT _partition, _offset, payload, _key, {}, CASE {} ELSE NULL END AS _err FROM (
           SELECT *, CASE WHEN __op = 'd' THEN json_extract(__e, '$.before') ELSE json_extract(__e, '$.after') END AS __row
           FROM (SELECT *, json_extract_string(__e, '$.op') AS __op FROM (
             SELECT _partition, _offset, payload, {} AS _key, {envelope} AS __e FROM {ok_table})))",
        cols.join(", "),
        checks.join(" "),
        debezium_key()
    )
}

/// The raw Kafka rows `(_partition, _offset, payload, _key)` without a value (tombstones), shaped like
/// `decode_sql`'s rows with `_err` = `TOMBSTONE` and the columns NULL.
pub fn tombstones_sql(src: &crate::model::SourceDef, raw_table: &str) -> String {
    let nulls: Vec<String> = src.options.columns.iter().map(|(c, t)| format!("NULL::{t} AS {}", ident(c))).collect();
    let key = key_sql(src);
    format!(
        "SELECT _partition, _offset, payload, {key} AS _key, {}, {} AS _err FROM {raw_table} WHERE payload IS NULL",
        nulls.join(", "),
        lit(TOMBSTONE)
    )
}

/// The raw rows that are dead letters as invalid JSON: a value that is not one JSON object. Tombstones are not
/// among them: they are staged as `TOMBSTONE` rows.
pub fn invalid_json_where() -> String {
    format!("payload IS NOT NULL AND NOT ({IS_JSON_OBJECT})")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A tombstone (a message without a value) is staged as a typed row with its key and `_err` 'tombstone', not
    /// as invalid JSON; a value that is not a JSON object is still a dead letter.
    #[test]
    fn tombstones_are_staged_with_their_keys() {
        let options = crate::model::SourceOptions::from_json(&serde_json::json!({
            "brokers": "x", "topic": "t", "event_time": "ts", "columns": {"ts": "TIMESTAMP", "id": "BIGINT"}
        }))
        .unwrap();
        let src = crate::model::SourceDef { name: "s".into(), kind: "kafka".into(), options };
        let conn = duckdb::Connection::open_in_memory().unwrap();
        conn.execute_batch(&format!(
            "CREATE TABLE raw (_partition INTEGER, _offset BIGINT, payload VARCHAR, _key VARCHAR);
             INSERT INTO raw VALUES (0, 0, '{{\"ts\": \"2026-01-01 00:00:00\", \"id\": 7}}', '{{\"id\": 7}}'),
                                    (0, 1, NULL, '7'), (0, 2, 'not json', NULL), (0, 3, '[1]', 'x');
             CREATE TABLE ok AS SELECT * FROM raw WHERE {IS_JSON_OBJECT};
             CREATE TABLE typed AS {} UNION ALL {};",
            decode_sql(&src, "ok"),
            tombstones_sql(&src, "raw")
        ))
        .unwrap();
        type Row = (i64, Option<i64>, Option<String>, Option<String>);
        let rows = |sql: &str| -> Vec<Row> {
            let mut st = conn.prepare(sql).unwrap();
            st.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?))).unwrap().map(Result::unwrap).collect()
        };
        assert_eq!(
            rows("SELECT _offset, id, _key, _err FROM typed ORDER BY _offset"),
            [(0, Some(7), Some("{\"id\": 7}".into()), None), (1, None, Some("7".into()), Some(TOMBSTONE.into()))]
        );
        let dead = format!("SELECT _offset, NULL::BIGINT, _key, payload FROM raw WHERE {} ORDER BY _offset", invalid_json_where());
        assert_eq!(rows(&dead), [(2, None, None, Some("not json".into())), (3, None, Some("x".into()), Some("[1]".into()))]);
    }

    /// Debezium change events as the connector writes them, with and without `schemas.enable`: inserts, updates
    /// and snapshot reads are rows of `after`; a delete is a tombstone with `before`'s columns, so a delete
    /// without a message key still has its key; other operations, values that are not change events and
    /// events with a value Debezium left out are dead letters that say why. Numbers in timestamp and date
    /// columns are Debezium's epoch encodings.
    #[test]
    fn debezium_events_decode() {
        let options = crate::model::SourceOptions::from_json(&serde_json::json!({
            "brokers": "x", "topic": "t", "format": "debezium",
            "columns": {"id": "BIGINT", "status": "VARCHAR", "amount": "DECIMAL(10,2)", "created": "TIMESTAMP", "day": "DATE"}
        }))
        .unwrap();
        let src = crate::model::SourceDef { name: "orders".into(), kind: "kafka".into(), options };
        let after = r#"{"id": 1, "status": "new", "amount": "19.90", "created": 1763712345123456, "day": 20413}"#;
        let msgs: Vec<(Option<String>, Option<&str>)> = vec![
            // with schemas.enable: the envelope and the key are each a schema and a payload
            (
                Some(format!(r#"{{"schema": {{"type": "struct"}}, "payload": {{"before": null, "after": {after}, "source": {{"ts_ms": 1763712345123}}, "op": "c", "ts_ms": 1763712345200}}}}"#)),
                Some(r#"{"schema": {"type": "struct"}, "payload": {"id": 1}}"#),
            ),
            (Some(r#"{"before": null, "after": {"id": 1, "status": "paid", "amount": "19.90", "created": 1763712345123, "day": "2025-11-21"}, "op": "u"}"#.into()), Some(r#"{"id": 1}"#)),
            (Some(r#"{"before": null, "after": {"id": 2, "status": "new", "created": 1763712345123456789}, "op": "r"}"#.into()), Some(r#"{"id": 2}"#)),
            (Some(r#"{"before": {"id": 1, "status": "paid"}, "after": null, "op": "d"}"#.into()), Some(r#"{"id": 1}"#)),
            (Some(r#"{"before": {"id": 2}, "after": null, "op": "d"}"#.into()), None),
            (Some(r#"{"before": null, "after": null, "op": "t"}"#.into()), None),
            (Some(r#"{"before": null, "after": {"id": 3, "status": "__debezium_unavailable_value"}, "op": "u"}"#.into()), Some(r#"{"id": 3}"#)),
            (Some(r#"{"id": 4, "status": "new"}"#.into()), Some(r#"{"id": 4}"#)),
            (None, Some(r#"{"schema": {"type": "struct"}, "payload": {"id": 1}}"#)),
            (Some(r#"{"after": {"id": 5, "amount": "abc"}, "op": "c"}"#.into()), Some(r#"{"id": 5}"#)),
        ];
        let conn = duckdb::Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE raw (_partition INTEGER, _offset BIGINT, payload VARCHAR, _key VARCHAR)").unwrap();
        for (i, (v, k)) in msgs.iter().enumerate() {
            conn.execute("INSERT INTO raw VALUES (0, ?, ?, ?)", duckdb::params![i as i64, v, k]).unwrap();
        }
        conn.execute_batch(&format!(
            "CREATE TABLE ok AS SELECT * FROM raw WHERE {IS_JSON_OBJECT};
             CREATE TABLE typed AS {} UNION ALL {};",
            decode_sql(&src, "ok"),
            tombstones_sql(&src, "raw")
        ))
        .unwrap();
        let mut st = conn
            .prepare(
                "SELECT _offset, _key, id, status, amount::VARCHAR, created::VARCHAR, day::VARCHAR, _err FROM typed ORDER BY _offset",
            )
            .unwrap();
        type Row = (i64, Option<String>, Option<i64>, Option<String>, Option<String>, Option<String>, Option<String>, Option<String>);
        let rows: Vec<Row> = st
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?, r.get(6)?, r.get(7)?)))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        let s = |x: &str| Some(x.to_string());
        let ts = s("2025-11-21 08:05:45.123456");
        let expected: Vec<Row> = vec![
            (0, s(r#"{"id":1}"#), Some(1), s("new"), s("19.90"), ts.clone(), s("2025-11-21"), None),
            (1, s(r#"{"id": 1}"#), Some(1), s("paid"), s("19.90"), s("2025-11-21 08:05:45.123"), s("2025-11-21"), None),
            (2, s(r#"{"id": 2}"#), Some(2), s("new"), None, ts.clone(), None, None),
            (3, s(r#"{"id": 1}"#), Some(1), s("paid"), None, None, None, s(TOMBSTONE)),
            (4, None, Some(2), None, None, None, None, s(TOMBSTONE)),
            (5, None, None, None, None, None, None, s("Debezium operation t is not applied")),
            (6, s(r#"{"id": 3}"#), Some(3), s(UNAVAILABLE), None, None, None, s(UNAVAILABLE_ERR)),
            (7, s(r#"{"id": 4}"#), None, None, None, None, None, s("not a Debezium change event: no op")),
            (8, s(r#"{"id":1}"#), None, None, None, None, None, s(TOMBSTONE)),
            (9, s(r#"{"id": 5}"#), Some(5), None, None, None, None, s("type conflict: amount is not DECIMAL(10,2)")),
        ];
        assert_eq!(rows, expected);
    }

    #[test]
    fn fast_list_forms() {
        assert!(plain_timestamp("2026-10-29 06:02:50"));
        assert!(plain_timestamp("2026-10-29T06:02:50.123456"));
        assert!(!plain_timestamp("2026-10-29 06:02:50.1234567"));
        assert!(!plain_timestamp("2026-10-29 06:02:50+07:00"));
        assert!(!plain_timestamp("2026-10-29 06:02:50Z"));
        assert!(!plain_timestamp("2026-10-29"));
        assert_eq!(plain_decimal("12.34"), Some((2, 2)));
        assert_eq!(plain_decimal("-0.5"), Some((0, 1)));
        assert_eq!(plain_decimal("1e2"), None);
        assert_eq!(plain_decimal("1."), None);
        assert_eq!(plain_decimal("01"), None);
        assert!(json_number("0") && json_number("-0.5") && json_number("1E+2") && json_number("10"));
        assert!(!json_number("01") && !json_number("-") && !json_number(".5") && !json_number("1e") && !json_number("NaN"));
        for ok in ["{}", "[]", "{\"a\": [1, {\"b\": null}], \"c\": \"x,\\\"}\"}", "{\n\t\"a\":\r\n1\n}", " 5 ", "\"s\""] {
            assert!(separators_ok(ok), "{ok}");
        }
        for bad in ["{\"a\":1 \"b\":2}", "{,\"a\":1}", "{\"a\":1,}", "[1,,2]", "[1 2]", "{\"a\":1,,\"b\":2}", "{}{}", "{} x", "{\"a\": \"x\u{0}\"}", "{\"a\" 1}"] {
            assert!(!separators_ok(bad), "{bad}");
        }
    }
}
