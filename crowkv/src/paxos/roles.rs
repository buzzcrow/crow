use crate::group::group::PxNodeId;
use serde::{Deserialize, Serialize};

pub trait Proposer {
    fn propose(&self);
}

pub trait Acceptor {
    #[allow(clippy::unused_async)]
    async fn accept(&mut self, entry: LogEntry) -> AcceptReply;
    #[allow(clippy::unused_async)]
    async fn prepare(&mut self, slot: SlotIndex, ballot: Ballot) -> PrepareReply;

    fn accepted_at(&self, slot: SlotIndex) -> Option<LogEntry>;
    fn promised_at(&self, slot: SlotIndex) -> Option<Ballot>;

    fn reclaim(&self) -> usize;
    fn trim(&self, before_slot: SlotIndex);
    fn trim_slot(&self) -> SlotIndex;
}

pub trait Learner {
    /// Apply a chosen log entry to the state machine.
    fn learn(&self, entry: LogEntry);
}

/// Paxos proposal number, ordered first by `round`, then by `leader_id`.
///
/// In steady state a leader uses `(0, leader_id)` for Phase-2-only writes.
/// `round` is bumped only by classic-Paxos repair at a single slot, or by a
/// new leader's bulk Phase-1 round (where `round = term`).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, Hash)]
pub struct Ballot {
    pub round: u64,
    pub leader_id: PxNodeId,
}

impl Ballot {
    pub const fn new(round: u64, leader_id: PxNodeId) -> Self {
        Self { round, leader_id }
    }
}

pub type SlotIndex = u64;

/// Classification of a log entry's payload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LogEntryKind {
    Write,
    NoOp,
    ConfigChange,
    DedupCheckpoint,
}

/// Single key-value operation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OpKind {
    Put,
    Delete,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Operation {
    pub key: Vec<u8>,
    pub op: OpKind,
    pub value: Option<Vec<u8>>,
}

/// One durable consensus log record.
///
/// `payload` semantics depend on `kind`:
/// - `Write`     — a serialized batch of `Operation` tuples.
/// - `NoOp`      — empty (used to fill repair gaps).
/// - `ConfigChange`     — serialized `crate::group::types::PxGroupConfig`.
/// - `DedupCheckpoint`  — serialized dedup-cache snapshot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogEntry {
    pub slot: SlotIndex,
    pub ballot: Ballot,
    pub term: u64,
    pub kind: LogEntryKind,
    pub payload: Vec<u8>,
    pub client_id: Option<u64>,
    pub seq: Option<u64>,
}

/// Reply to a Phase-1 `Prepare`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PrepareReply {
    /// Promise accepted. If the acceptor previously accepted a value at this slot,
    /// the proposer must adopt it (classic Paxos value-recovery rule).
    Promised {
        slot: SlotIndex,
        accepted: Option<LogEntry>,
    },
    /// Promise rejected because the slot already promised at a higher-or-equal ballot.
    /// The proposer should retry with a strictly higher ballot.
    Rejected {
        slot: SlotIndex,
        current_promised: Ballot,
    },
}

/// Reply to a Phase-2 `Accept`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AcceptReply {
    /// Value accepted at the entry's `(slot, ballot)`.
    Accepted { slot: SlotIndex, ballot: Ballot },
    /// Rejected because the slot promised a higher ballot.
    Rejected {
        slot: SlotIndex,
        current_promised: Ballot,
    },
}
