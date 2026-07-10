//! Core Paxos data types.
//!
//! Shapes are frozen at the end of P1 M1 per `doc/plan-consensus.md` §3.
//! Downstream phases (P2 WAL, P3 storage, P4 RPC) depend on these definitions
//! and must not change them without an explicit version bump.

pub type PxTerm = u64;
pub type PxSlot = u64;
pub type PxNodeId = u64;
pub type PxGroupId = u64;

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
    pub const fn new(round: u64, leader_id: PxNodeId) -> Self {
        Self { round, leader_id }
    }
}
