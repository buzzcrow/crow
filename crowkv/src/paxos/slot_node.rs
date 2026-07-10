#![allow(unsafe_code)]

use std::sync::atomic::{AtomicPtr, Ordering};
use std::ptr::null_mut;

use crate::group::types::{PxNodeId};
use crate::paxos::slot_list::{SlotIndex, SlotList};

// ------------------------------------------------------------------
// Paxos core types that belong logically with the slot node
// ------------------------------------------------------------------

/// Classification of a log entry's payload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LogEntryKind {
    Write,
    NoOp,
    ConfigChange,
    DedupCheckpoint,
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

/// One durable consensus log record.
///
/// `payload` semantics depend on `kind`:
/// - `Write`     — a serialized batch of `Operation` tuples.
/// - `NoOp`      — empty (used to fill repair gaps).
/// - `ConfigChange`     — serialized `crate::group::types::PxGroupConfig`.
/// - `DedupCheckpoint`  — serialized dedup-cache snapshot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PxLogEntry {
    pub slot: SlotIndex,
    pub ballot: PxBallot,
    pub term: u64,
    pub kind: LogEntryKind,
    pub payload: Vec<u8>,
    pub client_id: Option<u64>,
    pub seq: Option<u64>,
}

// ------------------------------------------------------------------
// Slot node
// ------------------------------------------------------------------

/// Per-slot state stored inside a `SlotList`.
///
/// The node pointer itself is installed once via `SlotList::insert` or a CAS on
/// the slot pointer; after that only the *fields* inside the node mutate, keeping
/// the outer pointer stable.
#[derive(Default)]
pub struct PxSlotNode {
    /// Highest ballot promised. Null until first `prepare`.
    pub(crate) promised: AtomicPtr<PxBallot>,
    /// Accepted entry. Null until first `accept`.
    pub(crate) accepted: AtomicPtr<PxLogEntry>,

    // ---------- deferred reclamation state (correctness-critical) ----------
    // Replaced field pointers are pushed here and reclaimed when node drops.
    retired_promised: AtomicPtr<RetiredPtr<PxBallot>>,
    retired_accepted: AtomicPtr<RetiredPtr<PxLogEntry>>,
}

impl Drop for PxSlotNode {
    fn drop(&mut self) {
        let promised = self.promised.load(Ordering::Acquire);
        if !promised.is_null() {
            unsafe { drop(Box::from_raw(promised)); }
        }

        let accepted = self.accepted.load(Ordering::Acquire);
        if !accepted.is_null() {
            unsafe { drop(Box::from_raw(accepted)); }
        }

        Self::drain_retired(&self.retired_promised);
        Self::drain_retired(&self.retired_accepted);
    }
}

impl PxSlotNode {
    fn push_retired<U>(head: &AtomicPtr<RetiredPtr<U>>, ptr: *mut U) {
        if ptr.is_null() {
            return;
        }
        let node = Box::into_raw(Box::new(RetiredPtr {
            ptr,
            next: null_mut(),
        }));
        loop {
            let old = head.load(Ordering::Acquire);
            unsafe { (*node).next = old; }
            if head
                .compare_exchange(old, node, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                break;
            }
        }
    }

    fn drain_retired<U>(head: &AtomicPtr<RetiredPtr<U>>) {
        let mut node = head.load(Ordering::Acquire);
        while !node.is_null() {
            let next = unsafe { (*node).next };
            let ptr = unsafe { (*node).ptr };
            if !ptr.is_null() {
                unsafe { drop(Box::from_raw(ptr)); }
            }
            unsafe { drop(Box::from_raw(node)); }
            node = next;
        }
    }

    /// Load the current promised ballot (may be null).
    pub fn promised(&self) -> Option<&PxBallot> {
        let p = self.promised.load(Ordering::Acquire);
        if p.is_null() {
            None
        } else {
            Some(unsafe { &*p })
        }
    }

    pub fn promised_cloned(&self) -> Option<PxBallot> {
        self.promised().copied()
    }

    /// CAS the promised ballot from `expected` to `new`.
    ///
    /// Returns `Ok(_)` on success, `Err(actual)` on failure.
    pub fn cas_promised(
        &self,
        expected: *mut PxBallot,
        new: PxBallot,
    ) -> Result<*mut PxBallot, *mut PxBallot> {
        let new_ptr = Box::into_raw(Box::new(new));
        match self.promised.compare_exchange(
            expected,
            new_ptr,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(old) => {
                Self::push_retired(&self.retired_promised, old);
                Ok(new_ptr)
            }
            Err(actual) => {
                unsafe { drop(Box::from_raw(new_ptr)); }
                Err(actual)
            }
        }
    }

    /// Load the current accepted entry (may be null).
    pub fn accepted(&self) -> Option<&PxLogEntry> {
        let p = self.accepted.load(Ordering::Acquire);
        if p.is_null() {
            None
        } else {
            Some(unsafe { &*p })
        }
    }

    pub fn accepted_cloned(&self) -> Option<PxLogEntry> {
        self.accepted().cloned()
    }

    /// CAS the accepted entry from `expected` to `new`.
    ///
    /// Returns `Ok(_)` on success, `Err(actual)` on failure.
    pub fn cas_accepted(
        &self,
        expected: *mut PxLogEntry,
        new: PxLogEntry,
    ) -> Result<*mut PxLogEntry, *mut PxLogEntry> {
        let new_ptr = Box::into_raw(Box::new(new));
        match self.accepted.compare_exchange(
            expected,
            new_ptr,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(old) => {
                Self::push_retired(&self.retired_accepted, old);
                Ok(new_ptr)
            }
            Err(actual) => {
                unsafe { drop(Box::from_raw(new_ptr)); }
                Err(actual)
            }
        }
    }
}

/// Fast path: tail-first lookup for already-created slots.
/// Slow path: `insert_if_empty` to create the chunk and slot node.
pub fn get_or_prepare_slot(
    list: &SlotList<PxSlotNode>,
    slot: SlotIndex,
) -> Option<&PxSlotNode> {
    if let Some(ptr_guard) = list.get_tail_ptr(slot) {
        let slot_atomic = &*ptr_guard;
        let node_ptr = slot_atomic.load(Ordering::Acquire);
        if !node_ptr.is_null() {
            return Some(unsafe { &*node_ptr });
        }
        // Chunk exists but slot is empty → CAS-install default node.
        let new = Box::into_raw(Box::new(PxSlotNode::default()));
        match slot_atomic.compare_exchange(
            null_mut(),
            new,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => return Some(unsafe { &*new }),
            Err(p) => {
                unsafe { drop(Box::from_raw(new)); }
                return Some(unsafe { &*p });
            }
        }
    }
    // Slow path: chunk does not exist yet → use insert to create it.
    let guard = list.insert_if_empty(slot, PxSlotNode::default());
    let node: &PxSlotNode = &*guard;
    let ptr = node as *const PxSlotNode;
    // guard is dropped here (chunk pin released), but the node itself
    // remains valid because SlotList::insert never replaces a slot pointer.
    Some(unsafe { &*ptr })
}

struct RetiredPtr<T> {
    ptr: *mut T,
    next: *mut RetiredPtr<T>,
}
