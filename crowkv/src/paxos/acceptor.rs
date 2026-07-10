//! Acceptor state machine for one consensus group.
//!
//! Implements P1 M1 of `doc/plan-consensus.md`. Holds per-slot promised and accepted
//! state in memory; persistence is added in P2 (WAL). All public handlers are `async fn`
//! per the project concurrency model (`doc/plan.md` §7), even when they have no `await`
//! points yet, so the API surface is stable across phases.
//!
//! Invariants enforced here (cross-ref `doc/test-design-consensus.md` §1):
//! - **C2 — Ballot monotonic per slot.** `prepare`/`accept` reject any ballot strictly
//!   lower than the slot's current promise; equal-ballot accepts are idempotent.

use std::collections::BTreeMap;

use crate::kv::types::PxLogEntry;
use crate::paxos::types::{PxBallot, PxSlot};

/// Reply to a Phase-1 `Prepare`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PrepareReply {
    /// Promise accepted. If the acceptor previously accepted a value at this slot,
    /// the proposer must adopt it (classic Paxos value-recovery rule).
    Promised {
        slot: PxSlot,
        accepted: Option<PxLogEntry>,
    },
    /// Promise rejected because the slot already promised at a higher-or-equal ballot.
    /// The proposer should retry with a strictly higher ballot.
    Rejected {
        slot: PxSlot,
        current_promised: PxBallot,
    },
}

/// Reply to a Phase-2 `Accept`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AcceptReply {
    /// Value accepted at the entry's `(slot, ballot)`.
    Accepted { slot: PxSlot, ballot: PxBallot },
    /// Rejected because the slot promised a higher ballot.
    Rejected {
        slot: PxSlot,
        current_promised: PxBallot,
    },
}

#[derive(Default, Debug)]
pub struct Acceptor {
    /// Per-slot highest ballot promised. Persisted in P2.
    promised: BTreeMap<PxSlot, PxBallot>,
    /// Per-slot highest accepted (ballot, value). Persisted in P2.
    accepted: BTreeMap<PxSlot, PxLogEntry>,
}

impl Acceptor {
    pub fn new() -> Self {
        Self::default()
    }

    /// Phase 1 of Paxos.
    ///
    /// Promises not to accept any future proposal with a ballot strictly less than
    /// `ballot` at `slot`. If the promise is granted and a value was previously
    /// accepted at `slot`, returns it so the proposer can re-propose it (Paxos
    /// safety: no chosen value is ever overwritten).
    //
    // NOTE: `async fn` with no `await` points is intentional in P1 M1. Per
    // `doc/plan.md` §7 (async-everywhere policy), the public Acceptor API surface
    // must be `async` so P2 (WAL) can add awaits for fsync without breaking call
    // sites. The lint is suppressed locally rather than globally so accidental
    // unused-async elsewhere is still caught.
    #[allow(clippy::unused_async)]
    pub async fn prepare(&mut self, slot: PxSlot, ballot: PxBallot) -> PrepareReply {
        match self.promised.get(&slot).copied() {
            Some(current) if ballot < current => PrepareReply::Rejected {
                slot,
                current_promised: current,
            },
            _ => {
                self.promised.insert(slot, ballot);
                PrepareReply::Promised {
                    slot,
                    accepted: self.accepted.get(&slot).cloned(),
                }
            }
        }
    }

    /// Phase 2 of Paxos.
    ///
    /// Accepts `entry` at `entry.slot, entry.ballot` iff the ballot is at least the
    /// current per-slot promise. On accept, the slot's promised ballot is also
    /// raised to the accept ballot (matching the standard Paxos formulation).
    //
    // NOTE: `async fn` with no `await` points is intentional. See `prepare` above.
    #[allow(clippy::unused_async)]
    pub async fn accept(&mut self, entry: PxLogEntry) -> AcceptReply {
        let slot = entry.slot;
        let ballot = entry.ballot;
        if let Some(&current) = self.promised.get(&slot) {
            if ballot < current {
                return AcceptReply::Rejected {
                    slot,
                    current_promised: current,
                };
            }
        }
        self.promised.insert(slot, ballot);
        self.accepted.insert(slot, entry);
        AcceptReply::Accepted { slot, ballot }
    }

    /// Read-only accessor used by tests and (later) by replay/repair logic.
    pub fn accepted_at(&self, slot: PxSlot) -> Option<&PxLogEntry> {
        self.accepted.get(&slot)
    }

    /// Read-only accessor used by tests and (later) by replay logic.
    pub fn promised_at(&self, slot: PxSlot) -> Option<PxBallot> {
        self.promised.get(&slot).copied()
    }
}
