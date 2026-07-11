use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::DashMap;
use parking_lot::Mutex;

use crate::paxos::roles::{Learner, PxLogEntry, SlotIndex};
use crate::paxos::PxTerm;

/// In-memory state-machine backed by a `DashMap`, plus the chosen-slot
/// frontier needed by leader-election safety checks and bulk Phase 1.
///
/// `learn` is called once a log entry has been chosen (i.e. accepted by a
/// quorum). The payload is the minimal binary format emitted by
/// `PxReplica::encode_kv_payload`.
///
/// Key work: KV store apply, contiguous-chosen / contiguous-applied watermarks,
/// `last_chosen_slot` / `last_chosen_term` (for `RequestVote` log-up-to-date check).
pub struct PxLearner {
    store: DashMap<Vec<u8>, Vec<u8>>,
    /// Highest slot S such that every slot in `[1, S]` has been learned.
    contiguous_chosen: AtomicU64,
    /// Highest slot S such that every slot in `[1, S]` has been applied to
    /// the KV store. In V1 every `learn()` call applies synchronously so this
    /// tracks `contiguous_chosen`; a future async-apply path will let them
    /// diverge.
    contiguous_applied: AtomicU64,
    /// Highest slot ever seen as chosen (gaps allowed). Used as the responder
    /// side of the Raft-style log-up-to-date check.
    last_chosen_slot: AtomicU64,
    /// Term of the entry at `last_chosen_slot`.
    last_chosen_term: AtomicU64,
    /// Out-of-order chosen slots awaiting a gap-fill from a lower slot. Maps
    /// slot → term so the frontier advance step can also bump
    /// `last_chosen_term` if it crosses an out-of-order slot.
    out_of_order: Mutex<BTreeMap<SlotIndex, PxTerm>>,
}

impl Default for PxLearner {
    fn default() -> Self {
        Self {
            store: DashMap::new(),
            contiguous_chosen: AtomicU64::new(0),
            contiguous_applied: AtomicU64::new(0),
            last_chosen_slot: AtomicU64::new(0),
            last_chosen_term: AtomicU64::new(0),
            out_of_order: Mutex::new(BTreeMap::new()),
        }
    }
}

impl PxLearner {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn store(&self) -> &DashMap<Vec<u8>, Vec<u8>> {
        &self.store
    }

    /// Highest contiguous chosen slot.
    #[must_use]
    pub fn contiguous_chosen(&self) -> SlotIndex {
        self.contiguous_chosen.load(Ordering::Acquire)
    }

    /// Highest contiguous applied slot.
    #[must_use]
    pub fn contiguous_applied(&self) -> SlotIndex {
        self.contiguous_applied.load(Ordering::Acquire)
    }

    /// Highest slot ever seen as chosen (gaps allowed).
    #[must_use]
    pub fn last_chosen_slot(&self) -> SlotIndex {
        self.last_chosen_slot.load(Ordering::Acquire)
    }

    /// Term of the entry at [`Self::last_chosen_slot`].
    #[must_use]
    pub fn last_chosen_term(&self) -> PxTerm {
        self.last_chosen_term.load(Ordering::Acquire)
    }

    /// Receive a peer's notification that `(slot, term)` is chosen.
    ///
    /// Updates `last_chosen_slot` / `last_chosen_term` only; never
    /// touches the contiguous-chosen / contiguous-applied watermarks
    /// because the receiver has no value to apply yet (notices carry
    /// no payload). Idempotent.
    ///
    /// Returns `true` if the high-water mark advanced, `false` if the
    /// notice was already at or behind the current `last_chosen_slot`.
    pub fn note_chosen(&self, slot: SlotIndex, term: PxTerm) -> bool {
        let mut prev = self.last_chosen_slot.load(Ordering::Relaxed);
        loop {
            if slot <= prev {
                return false;
            }
            match self.last_chosen_slot.compare_exchange_weak(prev, slot, Ordering::AcqRel, Ordering::Relaxed) {
                Ok(_) => {
                    let _guard = self.out_of_order.lock();
                    self.last_chosen_term.store(term, Ordering::Release);
                    return true;
                }
                Err(actual) => prev = actual,
            }
        }
    }

    /// Update the frontier for a newly learned `(slot, term)`.
    ///
    /// Idempotent: re-applying an already-learned slot is a no-op.
    fn update_frontier(&self, slot: SlotIndex, term: PxTerm) {
        // `last_chosen_slot` is the max ever seen (gaps allowed).
        let mut prev = self.last_chosen_slot.load(Ordering::Relaxed);
        loop {
            if slot <= prev {
                break;
            }
            match self.last_chosen_slot.compare_exchange_weak(prev, slot, Ordering::AcqRel, Ordering::Relaxed) {
                Ok(_) => {
                    // Race-free under `&self`: lock the out-of-order map for the
                    // term write so we don't race with a concurrent advance.
                    let _guard = self.out_of_order.lock();
                    self.last_chosen_term.store(term, Ordering::Release);
                    break;
                }
                Err(actual) => prev = actual,
            }
        }

        // Advance the contiguous-chosen watermark.
        let mut map = self.out_of_order.lock();
        let mut cc = self.contiguous_chosen.load(Ordering::Acquire);
        match slot.cmp(&(cc + 1)) {
            std::cmp::Ordering::Less => {
                // Already chosen (slot <= cc). No advance.
            }
            std::cmp::Ordering::Equal => {
                cc = slot;
                // Drain consecutive out-of-order slots.
                while let Some((&next_slot, &_next_term)) = map.iter().next() {
                    if next_slot == cc + 1 {
                        cc = next_slot;
                        map.remove(&next_slot);
                    } else {
                        break;
                    }
                }
                self.contiguous_chosen.store(cc, Ordering::Release);
                // V1: apply == learn, so contiguous_applied tracks contiguous_chosen.
                self.contiguous_applied.store(cc, Ordering::Release);
            }
            std::cmp::Ordering::Greater => {
                map.insert(slot, term);
            }
        }
    }

    /// Decode `payload` and apply each operation to the store.
    ///
    /// Wire format (per `PxReplica::encode_kv_payload`):
    ///   [`op_count`: u8]
    ///   for each op:
    ///     [`kind`: u8]  0=Put, 1=Delete
    ///     [`key_len`: u32 LE]
    ///     [key bytes]
    ///     [`value_len`: u32 LE]  (0 for Delete)
    ///     [value bytes]
    fn apply_payload(&self, payload: &[u8]) {
        if payload.is_empty() {
            return;
        }
        let op_count = payload[0] as usize;
        let mut offset = 1usize;

        for _ in 0..op_count {
            if offset >= payload.len() {
                break;
            }
            let kind = payload[offset];
            offset += 1;

            let key_len = read_u32_le(payload, offset) as usize;
            offset += 4;
            let key = payload.get(offset..offset + key_len).unwrap_or(&[]).to_vec();
            offset += key_len;

            let value_len = read_u32_le(payload, offset) as usize;
            offset += 4;
            let value = payload.get(offset..offset + value_len).unwrap_or(&[]).to_vec();
            offset += value_len;

            if kind == 0 {
                self.store.insert(key, value);
            } else {
                self.store.remove(&key);
            }
        }
    }
}

fn read_u32_le(buf: &[u8], offset: usize) -> u32 {
    let b = buf.get(offset..offset + 4).unwrap_or(&[0; 4]);
    u32::from_le_bytes([b[0], b[1], b[2], b[3]])
}

impl Learner for PxLearner {
    fn learn(&self, entry: PxLogEntry) {
        self.apply_payload(&entry.payload);
        self.update_frontier(entry.slot, entry.term);
    }
}
