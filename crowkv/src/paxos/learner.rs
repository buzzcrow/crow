// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::DashMap;
use parking_lot::Mutex;

use crate::kv::{Batch, InMemKV, KVEngine};
use crate::paxos::roles::{Learner, PxLogEntry, SlotIndex};
use crate::paxos::PxTerm;

/// Per-client dedup record: the highest applied client sequence number and the
/// commit slot it landed at. Stored one-per-client (latest wins); a retry of
/// any `seq <= last_seq` is treated as already-applied (idempotent retry).
#[derive(Clone, Copy, Debug)]
struct DedupEntry {
    last_seq: u64,
    last_slot: SlotIndex,
}

/// State-machine driver: applies chosen log entries to a pluggable
/// [`KVEngine`], plus the chosen-slot frontier needed by leader-election safety
/// checks and bulk Phase 1.
///
/// `learn` is called once a log entry has been chosen (i.e. accepted by a
/// quorum). The payload is the minimal binary format emitted by
/// `PxKvStore::encode_kv_payload`, decoded here via [`Batch::decode`] and
/// handed to the engine with the entry's slot for per-key highest-slot-wins
/// apply.
///
/// Key work: engine apply, contiguous-chosen / contiguous-applied watermarks,
/// `last_chosen_slot` / `last_chosen_term` (for `RequestVote` log-up-to-date check).
pub struct PxLearner {
    /// Materialized key-value state. Boxed so the backend (in-memory, file,
    /// crowtree) is a runtime choice; the whole `PxLearner` is `Arc`-shared
    /// across replica rebuilds, so the engine is shared with it.
    engine: Box<dyn KVEngine>,
    /// Highest slot S such that every slot in `[1, S]` has been learned.
    contiguous_chosen: AtomicU64,
    /// Highest slot S such that every slot in `[1, S]` has been applied to
    /// the KV store. In V1 every `learn` call applies synchronously so this
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
    /// Per-`client_id` idempotency cache. Updated on every `learn` that
    /// carries a `(client_id, seq)`; consulted by the proposer to short-
    /// circuit a retried request to its prior commit slot without re-running
    /// Paxos. In-memory only — lost on crash/restart; retried requests after
    /// a restart simply get a new Paxos slot (same value, no corruption).
    dedup: DashMap<u64, DedupEntry>,
}

impl Default for PxLearner {
    fn default() -> Self {
        Self {
            engine: Box::new(InMemKV::new()),
            contiguous_chosen: AtomicU64::new(0),
            contiguous_applied: AtomicU64::new(0),
            last_chosen_slot: AtomicU64::new(0),
            last_chosen_term: AtomicU64::new(0),
            out_of_order: Mutex::new(BTreeMap::new()),
            dedup: DashMap::new(),
        }
    }
}

impl PxLearner {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Construct a learner with a caller-supplied engine backend (e.g.
    /// [`crate::kv::CrowtreeEngine`]) instead of the default [`InMemKV`].
    #[must_use]
    pub fn with_engine(engine: Box<dyn KVEngine>) -> Self {
        Self {
            engine,
            ..Self::default()
        }
    }

    /// Borrow the materialized state engine (point/range reads, `compare`).
    #[must_use]
    pub fn engine(&self) -> &dyn KVEngine {
        self.engine.as_ref()
    }

    /// Live value and its resolved slot for `key`, or `None` if unset or
    /// tombstoned. Convenience wrapper over [`KVEngine::get`].
    ///
    /// `async fn`: `.await`s the `KVFuture` directly instead of `into_ready` --
    /// [`crate::kv::CrowtreeEngine::get`] can now genuinely construct
    /// `KVFuture::Pending` for a demand-load miss, and `into_ready` would
    /// panic on that case. The fast (`Ready`) path costs nothing extra: a
    /// `KVFuture::poll` on `Ready` resolves on the very first poll, so this
    /// `.await` never actually suspends for it.
    #[must_use]
    pub async fn engine_get(&self, key: &[u8]) -> Option<(SlotIndex, Vec<u8>)> {
        self.engine.get(key).await
    }

    /// Ordered prefix scan of live entries; see [`KVEngine::scan`].
    /// `async fn` for signature uniformity with [`Self::engine_get`], but
    /// `KVEngine::scan` has no genuine `Pending` path yet (no
    /// `ct_scan_async` C API -- see `CrowtreeEngine`'s own doc comment), so
    /// this never actually suspends today either.
    #[must_use]
    #[allow(clippy::type_complexity)]
    pub async fn engine_scan(
        &self,
        prefix: &[u8],
        start_after: &[u8],
        limit: usize,
    ) -> (Vec<(Vec<u8>, SlotIndex, Vec<u8>)>, bool) {
        self.engine.scan(prefix, start_after, limit).await
    }

    /// Number of live (non-tombstoned) keys in the engine.
    #[must_use]
    pub fn live_key_count(&self) -> usize {
        self.engine.live_key_count()
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
            match self
                .last_chosen_slot
                .compare_exchange_weak(prev, slot, Ordering::AcqRel, Ordering::Relaxed)
            {
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
            match self
                .last_chosen_slot
                .compare_exchange_weak(prev, slot, Ordering::AcqRel, Ordering::Relaxed)
            {
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

    /// Fast-forward the chosen-slot frontier directly to `(slot, term)`,
    /// bypassing `update_frontier`'s sequential/out-of-order-map advance.
    ///
    /// Only safe to call once, before any `learn` call, on a
    /// freshly-constructed learner (does not merge with existing
    /// out-of-order state) — used exclusively by
    /// [`crate::cluster::local_replica::PxLocalReplica::restore_from_replay_with_engine`]
    /// to skip re-`learn`ing a WAL prefix the injected engine already
    /// durably reflects ([`crate::kv::KVEngine::resume_from_slot`]), while
    /// still landing at the same `contiguous_chosen`/`contiguous_applied`/
    /// `last_chosen_slot`/`last_chosen_term` state a full sequential replay
    /// through `learn` up to `slot` would have produced.
    pub(crate) fn seed_resume_frontier(&self, slot: SlotIndex, term: PxTerm) {
        self.contiguous_chosen.store(slot, Ordering::Release);
        self.contiguous_applied.store(slot, Ordering::Release);
        self.last_chosen_slot.store(slot, Ordering::Release);
        self.last_chosen_term.store(term, Ordering::Release);
    }

    /// Idempotency lookup: if `client_id`'s highest applied sequence number is
    /// `>= seq`, the request was already committed; return the commit slot of
    /// that client's latest applied request so the proposer can reply without
    /// re-running Paxos. `client_id == 0` is the "no client" sentinel and never
    /// dedups. Returns `None` for a fresh `(client, seq)`.
    #[must_use]
    pub fn dedup_lookup(&self, client_id: u64, seq: u64) -> Option<SlotIndex> {
        if client_id == 0 {
            return None;
        }
        self.dedup
            .get(&client_id)
            .filter(|e| seq <= e.last_seq)
            .map(|e| e.last_slot)
    }

    /// Record that `(client_id, seq)` committed at `slot`. Keeps the highest
    /// `seq` seen per client (monotonic; out-of-order / replayed lower seqs do
    /// not regress the record). No-op for the `client_id == 0` sentinel.
    fn record_dedup(&self, client_id: Option<u64>, seq: Option<u64>, slot: SlotIndex) {
        let (Some(client_id), Some(seq)) = (client_id, seq) else {
            return;
        };
        if client_id == 0 {
            return;
        }
        self.dedup
            .entry(client_id)
            .and_modify(|e| {
                if seq > e.last_seq {
                    e.last_seq = seq;
                    e.last_slot = slot;
                }
            })
            .or_insert(DedupEntry {
                last_seq: seq,
                last_slot: slot,
            });
    }

    /// Decode `payload` and apply it to the engine at `slot`.
    ///
    /// An empty payload (`NoOp` repair fill) decodes to an empty batch and is
    /// a no-op for the engine while still advancing the frontier in `learn`.
    /// Per-key highest-slot-wins is enforced inside [`KVEngine::apply`].
    /// `async fn` for the same reason as [`Self::engine_get`]; `KVEngine::apply`
    /// has no genuine `Pending` path yet either (no async apply C API), so
    /// this never actually suspends today.
    ///
    /// A local apply failure (e.g. a durable I/O error on a
    /// `CrowtreeEngine`) is logged at `ERROR` and otherwise swallowed here:
    /// the value is still Paxos-chosen (this is a local durability fault,
    /// not a consensus outcome), so it must not be treated as "not applied"
    /// or re-proposed. Detecting and reacting to a persistently-unhealthy
    /// local engine is [`KVEngine::apply`]'s caller's job at a layer that
    /// can see engine health across calls, not a single failed apply.
    async fn apply_entry(&self, slot: SlotIndex, payload: &[u8]) {
        let batch = Batch::decode(payload);
        if batch.ops.is_empty() {
            return;
        }
        if let Err(error) = self.engine.apply(slot, &batch).await {
            tracing::error!(
                slot,
                error = %error,
                "critical: KVEngine::apply failed -- this slot is Paxos-chosen but may not be \
                 durably reflected in the local engine; next step: check engine health and \
                 consider failing this node out of the group"
            );
        }
    }
}

impl Learner for PxLearner {
    async fn learn(&self, entry: PxLogEntry, client_id: Option<u64>, seq: Option<u64>) {
        self.apply_entry(entry.slot, entry.payload.as_ref()).await;
        self.update_frontier(entry.slot, entry.term);
        self.record_dedup(client_id, seq, entry.slot);
    }
}
