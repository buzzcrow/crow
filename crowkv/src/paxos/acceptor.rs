// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Acceptor state machine for one consensus group.
//!
//! Implements P1 M1 consensus logic. Holds per-slot promised and accepted
//! state in a lock-free `SlotList`; persistence is added in P2 (WAL).
//!
//! Invariants enforced:
//! - **C2 — Ballot monotonic per slot.** `prepare`/`accept` reject any ballot strictly
//!   lower than the slot's current promise; equal-ballot accepts are idempotent.

#![allow(unsafe_code)]

use crate::paxos::roles::{Acceptor, PxAcceptReply, PxBallot, PxLogEntry, PxPrepareReply, SlotIndex};
use crate::paxos::slot_list::PxSlotList;
use crate::paxos::slot_node::{get_or_prepare_slot, PxSlotNode};
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, PartialEq, Eq)]
enum PxAcceptResult {
    Accepted { slot: SlotIndex, ballot: PxBallot },
    Rejected(PxBallot),
}

#[derive(Default)]
pub struct PxAcceptor {
    slot_list: PxSlotList<PxSlotNode>,
    /// Highest slot index ever opened on this acceptor via `prepare` or
    /// `accept`. Used as the bulk-Phase-1 ceiling input so a new leader
    /// can re-Prepare every slot a previous leader may have touched here,
    /// even ones whose values were never chosen.
    highest_seen_slot: AtomicU64,
}

impl PxAcceptor {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Highest slot ever opened (monotonic).
    #[must_use]
    pub fn highest_seen_slot(&self) -> SlotIndex {
        self.highest_seen_slot.load(Ordering::Acquire)
    }

    #[must_use]
    pub fn accepted_log_tip(&self) -> Option<(SlotIndex, u64)> {
        let highest = self.highest_seen_slot();
        for slot in (1..=highest).rev() {
            if let Some(entry) = self.slot_list.get(slot).and_then(|node| node.accepted_cloned()) {
                return Some((entry.slot, entry.term));
            }
        }
        None
    }

    fn bump_highest_seen(&self, slot: SlotIndex) {
        let mut prev = self.highest_seen_slot.load(Ordering::Relaxed);
        while slot > prev {
            match self.highest_seen_slot.compare_exchange_weak(
                prev,
                slot,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => prev = actual,
            }
        }
    }

    // ---------- internals ----------

    #[allow(clippy::unused_async)]
    async fn prepare(&self, slot: SlotIndex, ballot: PxBallot) -> PxPrepareReply {
        let Some(node) = get_or_prepare_slot(&self.slot_list, slot) else {
            return PxPrepareReply::Rejected {
                slot,
                current_promised: PxBallot::new(0, 0),
            };
        };
        self.bump_highest_seen(slot);
        loop {
            let current_ptr = node.promised.load(Ordering::Acquire);
            if !current_ptr.is_null() {
                let current = unsafe { &*current_ptr };
                if ballot <= *current {
                    return PxPrepareReply::Rejected {
                        slot,
                        current_promised: *current,
                    };
                }
            }
            let accepted = node.accepted_cloned();
            if node.cas_promised(current_ptr, ballot).is_ok() {
                return PxPrepareReply::Promised { slot, accepted };
            } // another writer raced, retry
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn inner_accept(&self, entry: &PxLogEntry) -> Option<PxAcceptResult> {
        let slot = entry.slot;
        let ballot = entry.ballot;
        let node = get_or_prepare_slot(&self.slot_list, slot)?;
        self.bump_highest_seen(slot);
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
            if node.cas_accepted(accepted_ptr, entry.clone()).is_ok() {
                return Some(PxAcceptResult::Accepted { slot, ballot });
            } // another writer raced, retry
        }
    }
}

impl Acceptor for PxAcceptor {
    #[allow(clippy::unused_async)]
    async fn accept(&self, entry: PxLogEntry) -> PxAcceptReply {
        let slot = entry.slot;
        match self.inner_accept(&entry) {
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
        self.prepare(slot, ballot).await
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
