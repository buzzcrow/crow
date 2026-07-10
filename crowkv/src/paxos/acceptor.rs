//! Acceptor state machine for one consensus group.
//!
//! Implements P1 M1 of `doc/plan/plan-consensus.md`. Holds per-slot promised and accepted
//! state in a lock-free `SlotList`; persistence is added in P2 (WAL).
//!
//! Invariants enforced here (cross-ref `doc/test/test-design-consensus.md` §1):
//! - **C2 — Ballot monotonic per slot.** `prepare`/`accept` reject any ballot strictly
//!   lower than the slot's current promise; equal-ballot accepts are idempotent.

#![allow(unsafe_code)]

use crate::paxos::roles::{AcceptReply, Acceptor, Ballot, LogEntry, PrepareReply, SlotIndex};
use crate::paxos::slot_list::SlotList;
use crate::paxos::slot_node::{get_or_prepare_slot, PxSlotNode};
use std::sync::atomic::Ordering;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PrepareResult {
    Promised { accepted: Option<LogEntry> },
    Rejected(Ballot),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AcceptResult {
    Accepted { slot: SlotIndex, ballot: Ballot },
    Rejected(Ballot),
}

#[derive(Default)]
pub struct PxAcceptor {
    log: SlotList<PxSlotNode>,
}

impl PxAcceptor {
    pub fn new() -> Self {
        Self::default()
    }

    // ---------- internals ----------

    fn inner_prepare(&self, slot: SlotIndex, ballot: Ballot) -> Option<PrepareResult> {
        let node = get_or_prepare_slot(&self.log, slot)?;
        loop {
            let current_ptr = node.promised.load(Ordering::Acquire);
            if !current_ptr.is_null() {
                let current = unsafe { &*current_ptr };
                if ballot < *current {
                    return Some(PrepareResult::Rejected(*current));
                }
            }
            match node.cas_promised(current_ptr, ballot) {
                Ok(_) => {
                    return Some(PrepareResult::Promised {
                        accepted: node.accepted_cloned(),
                    });
                }
                Err(_) => continue, // another writer raced, retry
            }
        }
    }

    fn inner_accept(&self, entry: LogEntry) -> Option<AcceptResult> {
        let slot = entry.slot;
        let ballot = entry.ballot;
        let node = get_or_prepare_slot(&self.log, slot)?;
        loop {
            let promised_ptr = node.promised.load(Ordering::Acquire);
            if !promised_ptr.is_null() {
                let promised = unsafe { &*promised_ptr };
                if ballot < *promised {
                    return Some(AcceptResult::Rejected(*promised));
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
                Ok(_) => return Some(AcceptResult::Accepted { slot, ballot }),
                Err(_) => continue, // another writer raced, retry
            }
        }
    }
}

impl Acceptor for PxAcceptor {
    #[allow(clippy::unused_async)]
    async fn accept(&mut self, entry: LogEntry) -> AcceptReply {
        let slot = entry.slot;
        match self.inner_accept(entry) {
            Some(AcceptResult::Accepted { slot: s, ballot: b }) => {
                AcceptReply::Accepted { slot: s, ballot: b }
            }
            Some(AcceptResult::Rejected(current)) => AcceptReply::Rejected {
                slot,
                current_promised: current,
            },
            None => AcceptReply::Rejected {
                slot,
                current_promised: Ballot::new(0, 0),
            },
        }
    }
    #[allow(clippy::unused_async)]
    async fn prepare(&mut self, slot: SlotIndex, ballot: Ballot) -> PrepareReply {
        match self.inner_prepare(slot, ballot) {
            Some(PrepareResult::Promised { accepted }) => PrepareReply::Promised { slot, accepted },
            Some(PrepareResult::Rejected(current)) => PrepareReply::Rejected {
                slot,
                current_promised: current,
            },
            None => PrepareReply::Rejected {
                slot,
                current_promised: Ballot::new(0, 0),
            },
        }
    }
    fn accepted_at(&self, slot: SlotIndex) -> Option<LogEntry> {
        self.log.get(slot)?.accepted_cloned()
    }
    fn promised_at(&self, slot: SlotIndex) -> Option<Ballot> {
        self.log.get(slot)?.promised_cloned()
    }
    fn reclaim(&self) -> usize {
        self.log.reclaim()
    }
    fn trim(&self, before_slot: SlotIndex) {
        self.log.trim(before_slot);
    }
    fn trim_slot(&self) -> SlotIndex {
        self.log.trim_slot()
    }
}
