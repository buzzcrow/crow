//! Key–value layer types carried inside Paxos log entries.
//!
//! `PxLogEntry` lives here because its payload semantics (`kind`, `Operation`,
//! `client_id`, `seq`) are key-value specific, even though the Paxos protocol
//! treats the payload as opaque bytes.

use crate::paxos::types::{PxBallot, PxSlot, PxTerm};

/// Classification of a log entry's payload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LogEntryKind {
    Write,
    NoOp,
    ConfigChange,
    DedupCheckpoint,
}

/// One durable consensus log record.
///
/// `payload` semantics depend on `kind`:
/// - `Write`     — a serialized batch of `Operation` tuples.
/// - `NoOp`      — empty (used to fill repair gaps).
/// - `ConfigChange`     — serialized `crate::group::types::PxGroupConfig`.
/// - `DedupCheckpoint`  — serialized dedup-cache snapshot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PxLogEntry {
    pub slot: PxSlot,
    pub ballot: PxBallot,
    pub term: PxTerm,
    pub kind: LogEntryKind,
    pub payload: Vec<u8>,
    pub client_id: Option<u64>,
    pub seq: Option<u64>,
}

/// Single key-value operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OpKind {
    Put,
    Delete,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Operation {
    pub key: Vec<u8>,
    pub op: OpKind,
    pub value: Option<Vec<u8>>,
}
