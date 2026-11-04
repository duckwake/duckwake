//! Arrow decode of JSON messages (Phase A of the Arrow ingest plan): one pass in Rust from Kafka payloads to
//! typed columns, so staging can `append_record_batch` instead of parsing JSON in SQL per column.
//!
//! The result must equal the SQL decode (`plan::decode_sql`) row for row. A value is decoded here only when
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
use crate::kafka::Msg;
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
    /// Indexes of messages without a payload (Kafka tombstones): dead letters (`INVALID_JSON`).
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
        let Some(event_time) = columns.iter().position(|c| c.name == opts.event_time) else {
            bail!("event_time {:?} is not a column", opts.event_time)
        };
        let mut out = vec![Field::new("_partition", DataType::Int32, false), Field::new("_offset", DataType::Int64, false)];
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
        let mut cols = vec![part, off];
        cols.extend(batch.columns().iter().cloned());
        Ok(RecordBatch::try_new(self.out_schema.clone(), cols)?)
    }
}

/// A decoded batch shaped like the staging table `(_partition, _offset, payload, <columns>, _err)`:
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

#[cfg(test)]
mod tests {
    use super::*;

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
