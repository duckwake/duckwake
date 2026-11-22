//! GTID sets, the position of a MySQL source: per server UUID, the transactions applied.
use anyhow::{bail, Context, Result};
use mysql_async::{GnoInterval, Sid};
use std::collections::BTreeMap;

/// A GTID set: per server UUID, intervals of transaction numbers (inclusive, sorted, merged).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct GtidSet(BTreeMap<String, Vec<(u64, u64)>>);

impl GtidSet {
    pub fn parse(s: &str) -> Result<Self> {
        let mut set = GtidSet::default();
        for part in s.split(',').map(str::trim).filter(|p| !p.is_empty()) {
            let mut it = part.split(':');
            let uuid = it.next().context("GTID set")?.to_lowercase();
            for iv in it {
                let (a, b) = iv.split_once('-').unwrap_or((iv, iv));
                let (a, b): (u64, u64) = match (a.parse(), b.parse()) {
                    (Ok(a), Ok(b)) => (a, b),
                    _ => bail!("GTID set {s:?}: tagged GTIDs (MySQL 8.4) are not supported"),
                };
                set.add_range(&uuid, a, b);
            }
        }
        Ok(set)
    }

    fn add_range(&mut self, uuid: &str, a: u64, b: u64) {
        let v = self.0.entry(uuid.to_string()).or_default();
        v.push((a, b));
        v.sort();
        let mut merged: Vec<(u64, u64)> = Vec::new();
        for (a, b) in v.drain(..) {
            match merged.last_mut() {
                Some(last) if a <= last.1 + 1 => last.1 = last.1.max(b),
                _ => merged.push((a, b)),
            }
        }
        *v = merged;
    }

    pub fn add(&mut self, uuid: &str, gno: u64) {
        self.add_range(uuid, gno, gno);
    }

    pub fn contains(&self, uuid: &str, gno: u64) -> bool {
        self.0.get(uuid).is_some_and(|v| v.iter().any(|(a, b)| *a <= gno && gno <= *b))
    }

    pub fn is_subset(&self, of: &GtidSet) -> bool {
        self.0.iter().all(|(u, v)| v.iter().all(|(a, b)| of.0.get(u).is_some_and(|w| w.iter().any(|(c, d)| c <= a && b <= d))))
    }

    /// How many transactions it holds.
    pub fn count(&self) -> u64 {
        self.0.values().flatten().map(|(a, b)| b - a + 1).sum()
    }

    pub(super) fn sids(&self) -> Result<Vec<Sid<'static>>> {
        self.0
            .iter()
            .map(|(u, v)| {
                let hex: String = u.chars().filter(|c| *c != '-').collect();
                let mut bytes = [0u8; 16];
                for (i, b) in bytes.iter_mut().enumerate() {
                    *b = u8::from_str_radix(hex.get(2 * i..2 * i + 2).context("server UUID")?, 16)?;
                }
                // The protocol's intervals end one past the last transaction.
                Ok(Sid::new(bytes).with_intervals(v.iter().map(|(a, b)| GnoInterval::new(*a, b + 1)).collect()))
            })
            .collect()
    }
}

impl std::fmt::Display for GtidSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let parts: Vec<String> = self
            .0
            .iter()
            .map(|(u, v)| {
                let ivs: Vec<String> = v.iter().map(|(a, b)| if a == b { a.to_string() } else { format!("{a}-{b}") }).collect();
                format!("{u}:{}", ivs.join(":"))
            })
            .collect();
        f.write_str(&parts.join(","))
    }
}

pub(super) fn uuid_text(sid: [u8; 16]) -> String {
    let h: String = sid.iter().map(|b| format!("{b:02x}")).collect();
    format!("{}-{}-{}-{}-{}", &h[0..8], &h[8..12], &h[12..16], &h[16..20], &h[20..32])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gtid_sets() {
        let s = GtidSet::parse("B2EB3B02-E621-11F1-9FB5-02FC00000001:1-7:9,\n0a0a0a0a-0000-0000-0000-000000000000:3").unwrap();
        assert_eq!(s.to_string(), "0a0a0a0a-0000-0000-0000-000000000000:3,b2eb3b02-e621-11f1-9fb5-02fc00000001:1-7:9");
        assert_eq!(s.count(), 9);
        let mut t = s.clone();
        t.add("b2eb3b02-e621-11f1-9fb5-02fc00000001", 8);
        assert_eq!(t.to_string(), "0a0a0a0a-0000-0000-0000-000000000000:3,b2eb3b02-e621-11f1-9fb5-02fc00000001:1-9");
        assert!(s.is_subset(&t) && !t.is_subset(&s));
        assert!(GtidSet::default().is_subset(&s));
        assert!(t.contains("b2eb3b02-e621-11f1-9fb5-02fc00000001", 8));
        assert!(GtidSet::parse("").unwrap().0.is_empty());
        assert!(GtidSet::parse("b2eb3b02-e621-11f1-9fb5-02fc00000001:tag:1-3").is_err());
        let sids = t.sids().unwrap();
        assert_eq!(sids.len(), 2);
        assert_eq!(uuid_text([0xb2, 0xeb, 0x3b, 0x02, 0xe6, 0x21, 0x11, 0xf1, 0x9f, 0xb5, 0x02, 0xfc, 0, 0, 0, 1]), "b2eb3b02-e621-11f1-9fb5-02fc00000001");
    }
}
