//! Persisted group membership configuration for WAL `ConfigChange` records.
//!
//! `PxGroupConfig` is the durable, consensus-independent snapshot of a group's
//! intended membership. It is written to the WAL metadata lane whenever a
//! membership mutation completes (`add_remote_replicas`, `remove_remote_replica`).
//! On restart, replay recovers the latest config and seeds the rebuilt group so
//! that a node cannot accidentally start as a `quorum=1` singleton in the restore
//! window.

use crate::paxos::{PxGroupId, PxTerm};
use crate::wal::record::WALRecord;

/// A single member of a persisted group configuration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PxGroupMember {
    pub replica_id: u64,
    pub endpoint: String,
    pub voting: bool,
}

/// Durable snapshot of a group's intended membership.
///
/// This is **not** the live consensus config (which for P3/P4 may be joint
/// consensus). It is the operator-visible, last-known committed membership.
/// A restarted node uses this to know which peers it should expect to contact.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PxGroupConfig {
    pub group_id: PxGroupId,
    pub term: PxTerm,
    pub members: Vec<PxGroupMember>,
}

impl PxGroupConfig {
    /// Serialize to a compact byte payload.
    ///
    /// Wire format (version 1):
    /// ```text
    /// [group_id    : u64 LE]
    /// [term        : u64 LE]
    /// [member_count: u32 LE]
    /// for each member:
    ///   [replica_id: u64 LE]
    ///   [voting    : u8    ]
    ///   [endpoint_len: u16 LE]
    ///   [endpoint bytes]
    /// ```
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&self.group_id.to_le_bytes());
        buf.extend_from_slice(&self.term.to_le_bytes());
        let count = u32::try_from(self.members.len()).unwrap_or(u32::MAX);
        buf.extend_from_slice(&count.to_le_bytes());
        for m in &self.members {
            buf.extend_from_slice(&m.replica_id.to_le_bytes());
            buf.push(u8::from(m.voting));
            let ep_len = u16::try_from(m.endpoint.len()).unwrap_or(u16::MAX);
            buf.extend_from_slice(&ep_len.to_le_bytes());
            buf.extend_from_slice(m.endpoint.as_bytes());
        }
        buf
    }

    /// Decode from the serialized payload.
    ///
    /// # Panics
    ///
    /// Panics only on internal invariant violation (the `need!` macro checks
    /// bounds before each fixed-size read, so the `try_into().unwrap()` calls
    /// are unreachable in practice).
    ///
    /// # Errors
    /// Returns an error string if the payload is truncated or malformed.
    pub fn decode(payload: &[u8]) -> Result<Self, String> {
        let mut off = 0usize;
        macro_rules! need {
            ($n:expr, $label:expr) => {
                if payload.len() - off < $n {
                    return Err(format!("truncated {}", $label));
                }
            };
        }
        macro_rules! read_u64 {
            () => {{
                let v = u64::from_le_bytes(payload[off..off + 8].try_into().unwrap());
                off += 8;
                v
            }};
        }

        need!(8, "group_id");
        let group_id = read_u64!();

        need!(8, "term");
        let term = read_u64!();

        need!(4, "member_count");
        let count = u32::from_le_bytes(payload[off..off + 4].try_into().unwrap()) as usize;
        off += 4;

        let mut members = Vec::with_capacity(count);
        for _ in 0..count {
            need!(8, "replica_id");
            let replica_id = read_u64!();

            need!(1, "voting");
            let voting = payload[off] != 0;
            off += 1;

            need!(2, "endpoint_len");
            let ep_len = u16::from_le_bytes(payload[off..off + 2].try_into().unwrap()) as usize;
            off += 2;

            need!(ep_len, "endpoint");
            let endpoint = String::from_utf8(payload[off..off + ep_len].to_vec())
                .map_err(|e| format!("invalid endpoint utf8: {e}"))?;
            off += ep_len;

            members.push(PxGroupMember {
                replica_id,
                endpoint,
                voting,
            });
        }

        Ok(Self {
            group_id,
            term,
            members,
        })
    }

    /// Encode this config into a WAL `ConfigChange` record.
    ///
    /// The record is written on the metadata lane (slot 0) and is not a
    /// consensus slot; it is a local durability aid for the restore window.
    #[must_use]
    pub fn to_record(&self) -> WALRecord {
        WALRecord::from_config_change(self.group_id, self.term, self.encode())
    }

    /// Decode a `ConfigChange` WAL record back into a group config.
    ///
    /// Returns `None` if the record is not a `ConfigChange` or if the payload is
    /// malformed.
    #[must_use]
    pub fn from_record(record: &WALRecord) -> Option<Self> {
        if !record.is_config_change() {
            return None;
        }
        Self::decode(record.payload.as_ref()).ok()
    }

    /// Total number of voting members, including the local replica if it is part
    /// of this config.
    #[must_use]
    pub fn voting_count(&self) -> usize {
        self.members.iter().filter(|m| m.voting).count()
    }

    /// Compute quorum size for the persisted config.
    ///
    /// Returns `0` if there are no voting members.
    #[must_use]
    pub fn quorum(&self) -> usize {
        let n = self.voting_count();
        if n == 0 {
            return 0;
        }
        n / 2 + 1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_config() {
        let cfg = PxGroupConfig {
            group_id: 7,
            term: 3,
            members: vec![
                PxGroupMember {
                    replica_id: 1,
                    endpoint: "127.0.0.1:10001".into(),
                    voting: true,
                },
                PxGroupMember {
                    replica_id: 2,
                    endpoint: "127.0.0.1:10002".into(),
                    voting: true,
                },
                PxGroupMember {
                    replica_id: 3,
                    endpoint: "127.0.0.1:10003".into(),
                    voting: true,
                },
            ],
        };
        let encoded = cfg.encode();
        let decoded = PxGroupConfig::decode(&encoded).expect("decode");
        assert_eq!(cfg, decoded);
    }

    #[test]
    fn roundtrip_record() {
        let cfg = PxGroupConfig {
            group_id: 7,
            term: 3,
            members: vec![PxGroupMember {
                replica_id: 1,
                endpoint: "127.0.0.1:10001".into(),
                voting: true,
            }],
        };
        let record = cfg.to_record();
        let decoded = PxGroupConfig::from_record(&record).expect("from record");
        assert_eq!(cfg, decoded);
    }
}
