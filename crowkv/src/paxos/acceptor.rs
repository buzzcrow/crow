//! Acceptor state machine for one consensus group.
//!
//! Implements P1 M1 of `doc/plan/plan-consensus.md`. Holds per-slot promised and accepted
//! state in a lock-free `SlotList`; persistence is added in P2 (WAL).
//!
//! Invariants enforced here (cross-ref `doc/test/test-design-consensus.md` §1):
//! - **C2 — Ballot monotonic per slot.** `prepare`/`accept` reject any ballot strictly
//!   lower than the slot's current promise; equal-ballot accepts are idempotent.

#![allow(unsafe_code)]

use crate::paxos::roles::{
    Acceptor, PxAcceptReply, PxBallot, PxLogEntry, PxPrepareReply, SlotIndex,
};
use crate::paxos::slot_list::PxSlotList;
use crate::paxos::slot_node::{get_or_prepare_slot, PxSlotNode};
use std::sync::atomic::Ordering;

#[derive(Debug, PartialEq, Eq)]
enum PxPrepareResult {
    Promised { accepted: Option<PxLogEntry> },
    Rejected(PxBallot),
}

#[derive(Debug, PartialEq, Eq)]
enum PxAcceptResult {
    Accepted { slot: SlotIndex, ballot: PxBallot },
    Rejected(PxBallot),
}

#[derive(Default)]
pub struct PxAcceptor {
    slot_list: PxSlotList<PxSlotNode>,
}

impl PxAcceptor {
    pub fn new() -> Self {
        Self::default()
    }

    // ---------- internals ----------

    fn inner_prepare(&self, slot: SlotIndex, ballot: PxBallot) -> Option<PxPrepareResult> {
        let node = get_or_prepare_slot(&self.slot_list, slot)?;
        loop {
            let current_ptr = node.promised.load(Ordering::Acquire);
            if !current_ptr.is_null() {
                let current = unsafe { &*current_ptr };
                if ballot < *current {
                    return Some(PxPrepareResult::Rejected(*current));
                }
            }
            match node.cas_promised(current_ptr, ballot) {
                Ok(_) => {
                    return Some(PxPrepareResult::Promised {
                        accepted: node.accepted_cloned(),
                    });
                }
                Err(_) => {} // another writer raced, retry
            }
        }
    }

    fn inner_accept(&self, entry: PxLogEntry) -> Option<PxAcceptResult> {
        let slot = entry.slot;
        let ballot = entry.ballot;
        let node = get_or_prepare_slot(&self.slot_list, slot)?;
        loop {
            let promised_ptr = node.promised.load(Ordering::Acquire);
            if !promised_ptr.is_null() {
                let promised = unsafe { &*promised_ptr };
                if ballot < *promised {
                    return Some(PxAcceptResult::Rejected(*promised));
                }
            }
            // Ensure promised is at least the accept ballot (Paxos formulation).
            if ballot > node.promised_cloned().unwrap_or(ballot) {
                match node.cas_promised(promised_ptr, ballot) {
                    Ok(_) | Err(_) => {} // either way, continue to accepted CAS
                }
            }
            let accepted_ptr = node.accepted.load(Ordering::Acquire);
            match node.cas_accepted(accepted_ptr, entry.clone()) {
                Ok(_) => return Some(PxAcceptResult::Accepted { slot, ballot }),
                Err(_) => {} // another writer raced, retry
            }
        }
    }
}

impl Acceptor for PxAcceptor {
    #[allow(clippy::unused_async)]
    async fn accept(&self, entry: PxLogEntry) -> PxAcceptReply {
        let slot = entry.slot;
        match self.inner_accept(entry) {
            Some(PxAcceptResult::Accepted { slot: s, ballot: b }) => {
                PxAcceptReply::Accepted { slot: s, ballot: b }
            }
            Some(PxAcceptResult::Rejected(current)) => PxAcceptReply::Rejected {
                slot,
                current_promised: current,
            },
            None => PxAcceptReply::Rejected {
                slot,
                current_promised: PxBallot::new(0, 0),
            },
        }
    }
    #[allow(clippy::unused_async)]
    async fn prepare(&self, slot: SlotIndex, ballot: PxBallot) -> PxPrepareReply {
        match self.inner_prepare(slot, ballot) {
            Some(PxPrepareResult::Promised { accepted }) => {
                PxPrepareReply::Promised { slot, accepted }
            }
            Some(PxPrepareResult::Rejected(current)) => PxPrepareReply::Rejected {
                slot,
                current_promised: current,
            },
            None => PxPrepareReply::Rejected {
                slot,
                current_promised: PxBallot::new(0, 0),
            },
        }
    }
    fn accepted_at(&self, slot: SlotIndex) -> Option<PxLogEntry> {
        self.slot_list.get(slot)?.accepted_cloned()
    }
    fn promised_at(&self, slot: SlotIndex) -> Option<PxBallot> {
        self.slot_list.get(slot)?.promised_cloned()
    }
    fn reclaim(&self) -> usize {
        self.slot_list.reclaim()
    }
    fn trim(&self, before_slot: SlotIndex) {
        self.slot_list.trim(before_slot);
    }
    fn trim_slot(&self) -> SlotIndex {
        self.slot_list.trim_slot()
    }
}
