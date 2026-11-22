//! PostgreSQL's logical replication messages (`pgoutput`, protocol version 1), as
//! `pg_logical_slot_peek_binary_changes` returns them: one message per row. Also the LSN and `pg_snapshot`
//! text forms the ingest job stores. See "Logical Replication Message Formats" in the PostgreSQL docs.
use crate::sources::database::Value;
use anyhow::{bail, Context, Result};


/// A column of a relation as the `Relation` message describes it.
#[derive(Debug, Clone, PartialEq)]
pub struct Column {
    pub name: String,
    pub type_oid: u32,
    pub typmod: i32,
    /// Part of the replica identity (the primary key, by default).
    pub key: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Message {
    /// `commit_us`: the commit time in microseconds since 1970.
    Begin { final_lsn: u64, commit_us: i64, xid: u32 },
    Commit { commit_lsn: u64, end_lsn: u64 },
    Relation { id: u32, schema: String, name: String, replica_identity: u8, columns: Vec<Column> },
    Insert { rel: u32, new: Vec<Value> },
    /// `old` is the replica identity's columns ('K', when the key changed) or the whole old row ('O', with
    /// `REPLICA IDENTITY FULL`); None when neither was sent.
    Update { rel: u32, old: Option<Vec<Value>>, new: Vec<Value> },
    Delete { rel: u32, old: Vec<Value> },
    Truncate { rels: Vec<u32> },
    /// Origin, Type and logical decoding messages: nothing for a mirror.
    Other(u8),
}

struct Reader<'a> {
    buf: &'a [u8],
    at: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self.at.checked_add(n).filter(|e| *e <= self.buf.len()).context("pgoutput message ends early")?;
        let s = &self.buf[self.at..end];
        self.at = end;
        Ok(s)
    }
    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    fn i16(&mut self) -> Result<i16> {
        Ok(i16::from_be_bytes(self.take(2)?.try_into()?))
    }
    fn i32(&mut self) -> Result<i32> {
        Ok(i32::from_be_bytes(self.take(4)?.try_into()?))
    }
    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into()?))
    }
    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_be_bytes(self.take(8)?.try_into()?))
    }
    fn string(&mut self) -> Result<String> {
        let rest = &self.buf[self.at..];
        let end = rest.iter().position(|b| *b == 0).context("pgoutput string without its terminator")?;
        let s = String::from_utf8(rest[..end].to_vec()).context("pgoutput string is not UTF-8")?;
        self.at += end + 1;
        Ok(s)
    }
    fn tuple(&mut self) -> Result<Vec<Value>> {
        let n = self.i16()?;
        let mut out = Vec::with_capacity(n.max(0) as usize);
        for _ in 0..n {
            out.push(match self.u8()? {
                b'n' => Value::Null,
                b'u' => Value::Unchanged,
                b't' => {
                    let len = self.i32()?;
                    let bytes = self.take(usize::try_from(len).context("negative value length")?)?;
                    Value::Text(String::from_utf8(bytes.to_vec()).context("a value is not UTF-8: the database's encoding must be UTF8")?)
                }
                b'b' => bail!("a value came in binary form; the slot must be read without the binary option"),
                k => bail!("unknown tuple value kind {:?}", k as char),
            });
        }
        Ok(out)
    }
}

pub fn decode(buf: &[u8]) -> Result<Message> {
    let mut r = Reader { buf, at: 0 };
    let kind = r.u8()?;
    let m = match kind {
        b'B' => {
            let final_lsn = r.u64()?;
            // PostgreSQL counts from 2000-01-01.
            let commit_us = r.u64()? as i64 + 946_684_800_000_000;
            Message::Begin { final_lsn, commit_us, xid: r.u32()? }
        }
        b'C' => {
            let _flags = r.u8()?;
            let commit_lsn = r.u64()?;
            let end_lsn = r.u64()?;
            Message::Commit { commit_lsn, end_lsn }
        }
        b'R' => {
            let id = r.u32()?;
            let schema = r.string()?;
            let name = r.string()?;
            let replica_identity = r.u8()?;
            let n = r.i16()?;
            let mut columns = Vec::new();
            for _ in 0..n {
                let flags = r.u8()?;
                let name = r.string()?;
                let type_oid = r.u32()?;
                let typmod = r.i32()?;
                columns.push(Column { name, type_oid, typmod, key: flags & 1 == 1 });
            }
            Message::Relation { id, schema, name, replica_identity, columns }
        }
        b'I' => {
            let rel = r.u32()?;
            match r.u8()? {
                b'N' => Message::Insert { rel, new: r.tuple()? },
                k => bail!("insert: expected a new tuple, got {:?}", k as char),
            }
        }
        b'U' => {
            let rel = r.u32()?;
            let (old, new) = match r.u8()? {
                b'K' | b'O' => {
                    let old = r.tuple()?;
                    match r.u8()? {
                        b'N' => (Some(old), r.tuple()?),
                        k => bail!("update: expected a new tuple, got {:?}", k as char),
                    }
                }
                b'N' => (None, r.tuple()?),
                k => bail!("update: unknown tuple kind {:?}", k as char),
            };
            Message::Update { rel, old, new }
        }
        b'D' => {
            let rel = r.u32()?;
            match r.u8()? {
                b'K' | b'O' => Message::Delete { rel, old: r.tuple()? },
                k => bail!("delete: unknown tuple kind {:?}", k as char),
            }
        }
        b'T' => {
            let n = r.i32()?;
            let _options = r.u8()?;
            let mut rels = Vec::new();
            for _ in 0..n {
                rels.push(r.u32()?);
            }
            Message::Truncate { rels }
        }
        // Stream messages only come with streaming = on, which the ingest job never asks for.
        b'S' | b'E' | b'c' | b'A' => bail!("a streamed transaction message ({:?}); the slot must be read with protocol version 1", kind as char),
        k => Message::Other(k),
    };
    Ok(m)
}

/// An LSN from its text form `X/Y`.
pub fn parse_lsn(s: &str) -> Result<u64> {
    let (hi, lo) = s.trim().split_once('/').with_context(|| format!("LSN {s:?} is not X/Y"))?;
    Ok((u64::from_str_radix(hi, 16)? << 32) | u64::from_str_radix(lo, 16)?)
}

pub fn format_lsn(lsn: u64) -> String {
    format!("{:X}/{:X}", lsn >> 32, lsn & 0xFFFF_FFFF)
}

/// A `pg_snapshot` (`xmin:xmax:xip,...`, 64-bit transaction ids): which transactions a REPEATABLE READ copy saw.
#[derive(Debug, Clone, PartialEq)]
pub struct Snapshot {
    pub xmin: u64,
    pub xmax: u64,
    pub xip: Vec<u64>,
}

impl Snapshot {
    pub fn parse(s: &str) -> Result<Self> {
        let mut parts = s.trim().split(':');
        let (Some(xmin), Some(xmax), Some(xip), None) = (parts.next(), parts.next(), parts.next(), parts.next()) else {
            bail!("snapshot {s:?} is not xmin:xmax:xip");
        };
        let xip = xip.split(',').filter(|x| !x.is_empty()).map(|x| x.parse::<u64>()).collect::<Result<Vec<_>, _>>()?;
        Ok(Snapshot { xmin: xmin.parse()?, xmax: xmax.parse()?, xip })
    }

    /// Whether the copy taken under this snapshot already holds the changes of transaction `xid` (the 32-bit
    /// id a Begin message carries). The full id is taken as the one nearest `xmax`, which is right for every
    /// transaction within two billion of the snapshot; the ingest job forgets the snapshot long before.
    pub fn sees(&self, xid: u32) -> bool {
        let diff = xid.wrapping_sub(self.xmax as u32) as i32 as i64;
        let full = self.xmax as i64 + diff;
        full < self.xmin as i64 || (full < self.xmax as i64 && !self.xip.contains(&(full as u64)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tuple(vals: &[Option<&str>]) -> Vec<u8> {
        let mut b = (vals.len() as i16).to_be_bytes().to_vec();
        for v in vals {
            match v {
                None => b.push(b'n'),
                Some("\u{0}u") => b.push(b'u'),
                Some(s) => {
                    b.push(b't');
                    b.extend((s.len() as i32).to_be_bytes());
                    b.extend(s.as_bytes());
                }
            }
        }
        b
    }

    #[test]
    fn messages_decode() {
        let mut b = vec![b'B'];
        b.extend(0x3C6E_6048u64.to_be_bytes());
        b.extend(1_000_000u64.to_be_bytes());
        b.extend(36442u32.to_be_bytes());
        assert_eq!(decode(&b).unwrap(), Message::Begin { final_lsn: 0x3C6E_6048, commit_us: 946_684_801_000_000, xid: 36442 });

        let mut b = vec![b'C', 0];
        b.extend(10u64.to_be_bytes());
        b.extend(20u64.to_be_bytes());
        b.extend(0u64.to_be_bytes());
        assert_eq!(decode(&b).unwrap(), Message::Commit { commit_lsn: 10, end_lsn: 20 });

        let mut b = vec![b'R'];
        b.extend(7u32.to_be_bytes());
        b.extend(b"public\0orders\0d");
        b.extend(2i16.to_be_bytes());
        b.push(1);
        b.extend(b"id\0");
        b.extend(23u32.to_be_bytes());
        b.extend((-1i32).to_be_bytes());
        b.push(0);
        b.extend(b"amount\0");
        b.extend(1700u32.to_be_bytes());
        b.extend(((18 << 16) + 2 + 4i32).to_be_bytes());
        let Message::Relation { id, schema, name, replica_identity, columns } = decode(&b).unwrap() else { panic!() };
        assert_eq!((id, schema.as_str(), name.as_str(), replica_identity), (7, "public", "orders", b'd'));
        assert!(columns[0].key && !columns[1].key);
        assert_eq!(columns[1].typmod, (18 << 16) + 6);

        let mut b = vec![b'I'];
        b.extend(7u32.to_be_bytes());
        b.push(b'N');
        b.extend(tuple(&[Some("1"), None]));
        assert_eq!(decode(&b).unwrap(), Message::Insert { rel: 7, new: vec![Value::Text("1".into()), Value::Null] });

        // A key change: the old key, then the new row with an unchanged TOAST value.
        let mut b = vec![b'U'];
        b.extend(7u32.to_be_bytes());
        b.push(b'K');
        b.extend(tuple(&[Some("1"), None]));
        b.push(b'N');
        b.extend(tuple(&[Some("2"), Some("\u{0}u")]));
        assert_eq!(
            decode(&b).unwrap(),
            Message::Update {
                rel: 7,
                old: Some(vec![Value::Text("1".into()), Value::Null]),
                new: vec![Value::Text("2".into()), Value::Unchanged]
            }
        );

        let mut b = vec![b'U'];
        b.extend(7u32.to_be_bytes());
        b.push(b'N');
        b.extend(tuple(&[Some("2"), Some("x")]));
        assert!(matches!(decode(&b).unwrap(), Message::Update { old: None, .. }));

        let mut b = vec![b'D'];
        b.extend(7u32.to_be_bytes());
        b.push(b'K');
        b.extend(tuple(&[Some("2"), None]));
        assert_eq!(decode(&b).unwrap(), Message::Delete { rel: 7, old: vec![Value::Text("2".into()), Value::Null] });

        let mut b = vec![b'T'];
        b.extend(2i32.to_be_bytes());
        b.push(0);
        b.extend(7u32.to_be_bytes());
        b.extend(9u32.to_be_bytes());
        assert_eq!(decode(&b).unwrap(), Message::Truncate { rels: vec![7, 9] });

        assert_eq!(decode(b"Yxx").unwrap(), Message::Other(b'Y'));
        assert!(decode(b"I\0\0").is_err(), "truncated");
        assert!(decode(b"S\0\0\0\0").is_err(), "streaming");
    }

    #[test]
    fn lsns() {
        assert_eq!(parse_lsn("0/3C6E6048").unwrap(), 0x3C6E_6048);
        assert_eq!(parse_lsn("16/B374D848").unwrap(), (0x16 << 32) | 0xB374_D848);
        assert_eq!(format_lsn((0x16 << 32) | 0xB374_D848), "16/B374D848");
        assert!(parse_lsn("garbage").is_err());
    }

    /// A transaction committed before the snapshot is in the copy; one in progress at it, or begun after, is not.
    #[test]
    fn snapshot_visibility() {
        let s = Snapshot::parse("100:110:103,107").unwrap();
        assert_eq!(s.xip, vec![103, 107]);
        assert!(s.sees(99) && s.sees(100) && s.sees(105) && s.sees(109));
        assert!(!s.sees(103) && !s.sees(107), "in progress at the snapshot");
        assert!(!s.sees(110) && !s.sees(5000), "begun after it");
        // Across the 32-bit wrap: full ids in the next epoch.
        let base = 5u64 << 32;
        let s = Snapshot::parse(&format!("{}:{}:", base - 3, base + 2)).unwrap();
        assert!(s.sees(u32::MAX - 3) && s.sees(1));
        assert!(s.sees(u32::MAX - 10), "older than xmin");
        assert!(!s.sees(2) && !s.sees(3));
        assert!(Snapshot::parse("1:2").is_err());
        assert_eq!(Snapshot::parse("1:2:").unwrap().xip, Vec::<u64>::new());
    }
}
