use crate::paxos::PxNodeId;
use std::sync::Arc;

pub trait Proposer {
    fn propose(&self);
}

#[allow(async_fn_in_trait)]
pub trait Acceptor {
    #[allow(clippy::unused_async)]
    async fn accept(&self, entry: PxLogEntry) -> PxAcceptReply;
    #[allow(clippy::unused_async)]
    async fn prepare(&self, slot: SlotIndex, ballot: PxBallot) -> PxPrepareReply;

    fn accepted_at(&self, slot: SlotIndex) -> Option<PxLogEntry>;
    fn promised_at(&self, slot: SlotIndex) -> Option<PxBallot>;

    fn reclaim(&self) -> usize;
    fn trim(&self, before_slot: SlotIndex);
    fn trim_slot(&self) -> SlotIndex;
}

pub trait Learner {
    /// Apply a chosen log entry to the state machine.
    fn learn(&self, entry: PxLogEntry);
}

/// Paxos proposal number, ordered first by `round`, then by `leader_id`.
///
/// In steady state a leader uses `(0, leader_id)` for Phase-2-only writes.
/// `round` is bumped only by classic-Paxos repair at a single slot, or by a
/// new leader's bulk Phase-1 round (where `round = term`).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, Hash)]
pub struct PxBallot {
    pub round: u64,
    pub leader_id: PxNodeId,
}

impl PxBallot {
    #[must_use]
    pub const fn new(round: u64, leader_id: PxNodeId) -> Self {
        Self { round, leader_id }
    }
}

pub type SlotIndex = u64;

/// Classification of a log entry's payload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PxLogEntryKind {
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
///
/// `payload` uses `Arc<Vec<u8>>` to enable cheap cloning (refcount increment) instead
/// of deep copying the entire payload. This is important because log entries are cloned
/// frequently during Paxos phases and learner propagation, and payloads can be large.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PxLogEntry {
    pub slot: SlotIndex,
    pub ballot: PxBallot,
    pub term: u64,
    pub kind: PxLogEntryKind,
    pub payload: Arc<Vec<u8>>,
    pub client_id: Option<u64>,
    pub seq: Option<u64>,
}

/// Reply to a Phase-1 `Prepare`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PxPrepareReply {
    /// Promise accepted. If the acceptor previously accepted a value at this slot,
    /// the proposer must adopt it (classic Paxos value-recovery rule).
    Promised { slot: SlotIndex, accepted: Option<PxLogEntry> },
    /// Promise rejected because the slot already promised at a higher-or-equal ballot.
    /// The proposer should retry with a strictly higher ballot.
    Rejected { slot: SlotIndex, current_promised: PxBallot },
}

/// Reply to a Phase-2 `Accept`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PxAcceptReply {
    /// Value accepted at the entry's `(slot, ballot)`.
    Accepted { slot: SlotIndex, ballot: PxBallot },
    /// Rejected because the slot promised a higher ballot.
    Rejected { slot: SlotIndex, current_promised: PxBallot },
}
