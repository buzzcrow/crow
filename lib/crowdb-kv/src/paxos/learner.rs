// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use dashmap::DashMap;
use parking_lot::Mutex;
use tokio::sync::Notify;

use crate::kv::{Batch, CrowdbTreeBackend, CrowdbTreeEngine, CrowdbTreeOptions, KVEngine};
use crate::paxos::roles::{DedupTag, Learner, PxLogEntry, SlotIndex};
use crate::paxos::PxTerm;

/// Per-client dedup retention: the last `DEDUP_WINDOW` committed
/// `(seq, slot)` mappings, in commit order. Exact-match lookup — a `seq`
/// that was itself recorded returns its slot; an unrecorded `seq` (lower or
/// otherwise) is a miss and falls into the "outside the window, outcome
/// unknown" case from `design.md` §10 (safe to re-propose). Sized to the
/// `design.md` "≥ 64 requests per client" floor, generously above
/// `max_inflight_proposals` (default 32) so a full window of concurrent
/// same-client requests never evicts an unresolved entry prematurely.
const DEDUP_WINDOW: usize = 64;

/// Per-client bounded dedup window. `VecDeque` (not a hash map): N is tiny
/// and the common case is a retry of the most-recent seq, scanned first.
#[derive(Debug, Default)]
struct DedupWindow {
    entries: VecDeque<(u64, SlotIndex)>,
}

impl DedupWindow {
    fn record(&mut self, seq: u64, slot: SlotIndex) {
        // Idempotent re-`learn` of an already-recorded seq (e.g. a duplicate
        // `Chosen` notice): leave the existing entry in place — no duplicate,
        // no slot overwrite.
        if self.entries.iter().any(|(s, _)| *s == seq) {
            return;
        }
        self.entries.push_back((seq, slot));
        if self.entries.len() > DEDUP_WINDOW {
            self.entries.pop_front();
        }
    }

    fn lookup(&self, seq: u64) -> Option<SlotIndex> {
        self.entries
            .iter()
            .rev()
            .find(|(s, _)| *s == seq)
            .map(|(_, slot)| *slot)
    }
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
    /// crowdb-tree) is a runtime choice; the whole `PxLearner` is `Arc`-shared
    /// across replica rebuilds, so the engine is shared with it.
    engine: Arc<dyn KVEngine>,
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
    /// Out-of-order **applied** slots awaiting a gap-fill from a lower slot.
    /// R17's `spawn_learn_chosen` defers the engine apply, and spawned
    /// applies can complete out of order, so `contiguous_applied` needs the
    /// same drain pattern `out_of_order` gives `contiguous_chosen`. Empty in
    /// steady state on the leader (propose slots are sequential); populated
    /// only under spawn reordering.
    applied_out_of_order: Mutex<BTreeSet<SlotIndex>>,
    /// Per-`client_id` idempotency cache. Updated on every `learn` that
    /// carries a `(client_id, seq)`; consulted by the proposer to short-
    /// circuit a retried request to its prior commit slot without re-running
    /// Paxos. In-memory only — lost on crash/restart; retried requests after
    /// a restart simply get a new Paxos slot (same value, no corruption).
    /// Retains the last `DEDUP_WINDOW` (64) `(seq, slot)` mappings per client;
    /// exact-match lookup — an unrecorded `seq` is a miss, never a false
    /// positive against a higher committed seq's slot.
    dedup: DashMap<u64, DedupWindow>,
    /// R35 apply fence: woken whenever `contiguous_applied` advances, so a
    /// Linearizable read awaiting `contiguous_applied >= read_slot` (after
    /// the leadership barrier resolves) can block until the async R17
    /// `spawn_learn_chosen` apply catches up instead of busy-spinning. The
    /// fast path (slot already applied) never awaits — `await_applied` does
    /// one `Acquire` load and returns.
    apply_notify: Notify,
    /// Test-only gate that holds `apply_entry` until the test releases it,
    /// so the R35 apply-fence test can deterministically park the spawned
    /// R17 apply and prove the Linearizable read's fence waits for it.
    /// `None` in production; set via `set_apply_gate_for_tests` under the
    /// `test-util` feature.
    #[cfg(feature = "test-util")]
    apply_gate: Mutex<Option<Arc<Notify>>>,
    /// Optional registry handle for engine-apply latency. Set via
    /// [`Self::set_engine_apply_summary`] when a registry is wired.
    engine_apply: OnceLock<Arc<crate::metrics::LatencySummary>>,
    /// Optional per-group watch registry. Set when the group is
    /// constructed; `None` in tests that don't use watch/notify. The
    /// apply-path hook in `apply_entry` loads this (lock-free
    /// `ArcSwap` read) then checks `has_watchers()` before touching
    /// the registry — zero overhead when no watchers. Uses
    /// `ArcSwapOption` (not `OnceLock`) because
    /// `inherit_local_state_from` shares the learner across group
    /// rebuilds and must re-wire the current group's registry; the
    /// apply path is a hot path (`learn`) so the read must be
    /// lock-free, not a `RwLock` read on every apply.
    watch_registry: arc_swap::ArcSwapOption<(u64, Arc<crate::cluster::watch_registry::WatchRegistry>)>,
    /// Gap 5 step 2: set by the group to the group's `flush_notify` so
    /// the apply path can wake the maintenance loop when a memtable
    /// freeze happens. `None` in tests that don't wire a group.
    flush_signal: OnceLock<Arc<Notify>>,
}

impl Default for PxLearner {
    fn default() -> Self {
        let opt = CrowdbTreeOptions {
            backend: CrowdbTreeBackend::MemBlock,
            ..Default::default()
        };
        let engine = CrowdbTreeEngine::open(&opt).expect("crowdb-tree mem-block open");
        Self {
            engine: Arc::new(engine),
            contiguous_chosen: AtomicU64::new(0),
            contiguous_applied: AtomicU64::new(0),
            last_chosen_slot: AtomicU64::new(0),
            last_chosen_term: AtomicU64::new(0),
            out_of_order: Mutex::new(BTreeMap::new()),
            applied_out_of_order: Mutex::new(BTreeSet::new()),
            dedup: DashMap::new(),
            apply_notify: Notify::new(),
            #[cfg(feature = "test-util")]
            apply_gate: Mutex::new(None),
            engine_apply: OnceLock::new(),
            watch_registry: arc_swap::ArcSwapOption::new(None),
            flush_signal: OnceLock::new(),
        }
    }
}

impl PxLearner {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Construct a learner with a caller-supplied engine backend (e.g.
    /// [`crate::kv::CrowdbTreeEngine`], or [`crate::kv::InMemKV`] in tests).
    #[must_use]
    pub fn with_engine(engine: Box<dyn KVEngine>) -> Self {
        Self {
            engine: Arc::from(engine),
            contiguous_chosen: AtomicU64::new(0),
            contiguous_applied: AtomicU64::new(0),
            last_chosen_slot: AtomicU64::new(0),
            last_chosen_term: AtomicU64::new(0),
            out_of_order: Mutex::new(BTreeMap::new()),
            applied_out_of_order: Mutex::new(BTreeSet::new()),
            dedup: DashMap::new(),
            apply_notify: Notify::new(),
            #[cfg(feature = "test-util")]
            apply_gate: Mutex::new(None),
            engine_apply: OnceLock::new(),
            watch_registry: arc_swap::ArcSwapOption::new(None),
            flush_signal: OnceLock::new(),
        }
    }

    /// Borrow the materialized state engine (point/range reads, `compare`).
    #[must_use]
    pub fn engine(&self) -> &dyn KVEngine {
        self.engine.as_ref()
    }

    /// Clone the engine handle as an `Arc`, for moving into `spawn_blocking`
    /// (the maintenance loop's `persist_snapshot` runs off the async runtime
    /// so it cannot stall the election driver / heartbeat task).
    #[must_use]
    pub fn engine_arc(&self) -> Arc<dyn KVEngine> {
        Arc::clone(&self.engine)
    }

    /// Gap 5 step 2: wire the group's `flush_notify` so the apply path
    /// can wake the maintenance loop when a memtable freeze happens.
    /// Called once during group creation.
    pub fn set_flush_signal(&self, notify: Arc<Notify>) {
        let _ = self.flush_signal.set(notify);
    }

    /// Wire the engine-apply latency summary. Called once during group
    /// creation when a metrics registry is available.
    pub(crate) fn set_engine_apply_summary(&self, summary: Arc<crate::metrics::LatencySummary>) {
        let _ = self.engine_apply.set(summary);
    }

    /// Wire the per-group watch registry. Called during `PxGroup`
    /// construction and re-wired by `inherit_local_state_from` when
    /// the learner is shared across a group rebuild. The apply-path
    /// hook in `apply_entry` checks `has_watchers()` before touching
    /// the registry — zero overhead when no watchers.
    pub(crate) fn set_watch_registry(
        &self,
        group_id: u64,
        registry: Arc<crate::cluster::watch_registry::WatchRegistry>,
    ) {
        self.watch_registry.store(Some(Arc::new((group_id, registry))));
    }

    /// Live value and its resolved slot for `key`, or `None` if unset or
    /// tombstoned. Convenience wrapper over [`KVEngine::get`].
    ///
    /// `async fn`: `.await`s the `KVFuture` directly instead of `into_ready` --
    /// [`crate::kv::CrowdbTreeEngine::get`] can now genuinely construct
    /// `KVFuture::Pending` for a demand-load miss, and `into_ready` would
    /// panic on that case. The fast (`Ready`) path costs nothing extra: a
    /// `KVFuture::poll` on `Ready` resolves on the very first poll, so this
    /// `.await` never actually suspends for it.
    #[must_use]
    pub async fn engine_get(&self, key: &[u8]) -> Option<(SlotIndex, Vec<u8>)> {
        self.engine.get(key).await
    }

    /// Like [`Self::engine_get`] but returns [`Bytes`] instead of `Vec<u8>`,
    /// via [`KVEngine::get_bytes`]. Avoids an intermediate `Vec<u8>` allocation
    /// on the crowdb-tree fast path.
    #[must_use]
    pub async fn engine_get_bytes(&self, key: &[u8]) -> Option<(SlotIndex, Bytes)> {
        self.engine.get_bytes(key).await
    }

    /// Ordered prefix scan of live entries; see [`KVEngine::scan`].
    /// `async fn` for signature uniformity with [`Self::engine_get`], but
    /// `KVEngine::scan` has no genuine `Pending` path yet (no
    /// `ct_scan_async` C API -- see `CrowdbTreeEngine`'s own doc comment), so
    /// this never actually suspends today either. Keys/values are
    /// zero-copy `Bytes`. `byte_budget` (`0` = unlimited) is pushed down
    /// into the engine. Errors propagate as `Err`.
    ///
    /// # Errors
    /// Returns `Err` if the underlying engine scan fails (e.g.
    /// `CtError::Corruption` from packed-result bounds checks).
    #[allow(clippy::type_complexity)]
    #[allow(clippy::too_many_arguments)]
    pub async fn engine_scan(
        &self,
        prefix: &[u8],
        start_after: &[u8],
        end_key: &[u8],
        limit: usize,
        byte_budget: usize,
        keys_only: bool,
        deadline_ms: u64,
    ) -> Result<(Vec<(bytes::Bytes, SlotIndex, bytes::Bytes)>, bool), String> {
        self.engine
            .scan(
                prefix,
                start_after,
                end_key,
                limit,
                byte_budget,
                keys_only,
                deadline_ms,
            )
            .await
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

    /// R35 apply fence: wait until `contiguous_applied >= slot`, then return.
    ///
    /// Used by the Linearizable read path after the leadership barrier
    /// resolves `read_slot` — with R17 (`async_engine_apply`) on, a
    /// just-chosen slot may not yet be applied, so the read must wait for
    /// the spawned `learn_chosen` apply to land before serving the engine
    /// get (read-your-writes). With R17 off, `contiguous_applied` already
    /// tracks `contiguous_chosen`, so the fast-path load returns immediately.
    ///
    /// Register-before-load: the `notified()` future is created **before**
    /// the `Acquire` load so a `notify_waiters` that fires between the load
    /// and registration is not missed — the load observes the
    /// `Release`-stored new frontier and returns without awaiting. Bounded
    /// by apply throughput (memtable insert is fast and contiguous).
    pub async fn await_applied(&self, slot: SlotIndex) {
        loop {
            let notified = self.apply_notify.notified();
            if self.contiguous_applied.load(Ordering::Acquire) >= slot {
                return;
            }
            notified.await;
        }
    }

    /// Test-only: hold `apply_entry` until the given [`Notify`] is signaled,
    /// so the R35 apply-fence test can deterministically park the spawned
    /// R17 apply and prove the Linearizable read's fence waits for it.
    #[cfg(feature = "test-util")]
    pub fn set_apply_gate_for_tests(&self, notify: Arc<Notify>) {
        *self.apply_gate.lock() = Some(notify);
    }

    /// Highest slot ever seen as chosen (gaps allowed).
    #[must_use]
    pub fn last_chosen_slot(&self) -> SlotIndex {
        self.last_chosen_slot.load(Ordering::Acquire)
    }

    /// R65: check whether a slot is chosen (safe to apply). A slot is
    /// chosen if it is in the continuous prefix (`≤ contiguous_chosen`)
    /// or in the out-of-order chosen set (individually confirmed via
    /// `ChosenNotice` with ballot match).
    #[must_use]
    pub fn is_chosen(&self, slot: SlotIndex) -> bool {
        if slot <= self.contiguous_chosen.load(Ordering::Acquire) {
            return true;
        }
        self.out_of_order.lock().contains_key(&slot)
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

    /// Advance the **chosen** frontier for a newly learned `(slot, term)`:
    /// `last_chosen_slot`/`term` (max ever seen) and `contiguous_chosen`
    /// (with out-of-order drain). Does **not** touch `contiguous_applied` —
    /// R17 splits the applied frontier into [`Self::advance_applied_frontier`]
    /// so the chosen frontier can advance synchronously (before `propose`
    /// returns) while the engine apply is deferred.
    ///
    /// Idempotent: re-applying an already-learned slot is a no-op.
    pub(crate) fn update_chosen_frontier(&self, slot: SlotIndex, term: PxTerm) {
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
            }
            std::cmp::Ordering::Greater => {
                map.insert(slot, term);
            }
        }
    }

    /// Advance the **applied** frontier for a slot whose engine apply just
    /// completed. R17 defers the apply, so this runs in the spawned
    /// `learn_chosen` task (and in the V1 sync `learn` path right after the
    /// sync apply). Spawned applies can complete out of order, so this
    /// mirrors `update_chosen_frontier`'s drain pattern with a separate
    /// `applied_out_of_order` map. Wakes any Linearizable read parked in
    /// [`Self::await_applied`] when `contiguous_applied` advances.
    ///
    /// Idempotent: re-advancing an already-applied slot is a no-op.
    pub(crate) fn advance_applied_frontier(&self, slot: SlotIndex) {
        let mut map = self.applied_out_of_order.lock();
        let mut ca = self.contiguous_applied.load(Ordering::Acquire);
        match slot.cmp(&(ca + 1)) {
            std::cmp::Ordering::Less => {
                // Already applied (slot <= ca). No advance.
            }
            std::cmp::Ordering::Equal => {
                ca = slot;
                // Drain consecutive out-of-order applied slots.
                while let Some(&next_slot) = map.iter().next() {
                    if next_slot == ca + 1 {
                        ca = next_slot;
                        map.remove(&next_slot);
                    } else {
                        break;
                    }
                }
                self.contiguous_applied.store(ca, Ordering::Release);
                // R35: wake any Linearizable read parked in `await_applied`
                // on the prior frontier. `notify_waiters` (not
                // `notify_one`) so every concurrent fenced read re-checks
                // together; woken readers that are still behind loop and
                // re-park. No-op when no reader is waiting.
                self.apply_notify.notify_waiters();
            }
            std::cmp::Ordering::Greater => {
                map.insert(slot);
            }
        }
    }

    /// Fast-forward the chosen-slot frontier directly to `(slot, term)`,
    /// bypassing `update_chosen_frontier`'s sequential/out-of-order-map advance.
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

    /// Idempotency lookup: if `client_id` has a recorded `(seq, slot)`
    /// mapping for this exact `seq`, return its commit slot so the proposer
    /// can reply without re-running Paxos. `client_id == 0` is the "no
    /// client" sentinel and never dedups. An unrecorded `seq` (lower or
    /// otherwise) returns `None` — it falls into the "outside the window,
    /// outcome unknown" case from `design.md` §10 and is safe to re-propose.
    #[must_use]
    pub fn dedup_lookup(&self, client_id: u64, seq: u64) -> Option<SlotIndex> {
        if client_id == 0 {
            return None;
        }
        self.dedup.get(&client_id).and_then(|w| w.lookup(seq))
    }

    /// Record every dedup tag in `tags` against `slot`. A coalesced
    /// multi-key batch passes one tag per client op; a single-key
    /// propose passes one; repair/election pass none. `client_id == 0`
    /// tags are skipped (sentinel).
    pub(crate) fn record_dedup_tags(&self, tags: &[DedupTag], slot: SlotIndex) {
        for tag in tags {
            if tag.client_id == 0 {
                continue;
            }
            self.dedup
                .entry(tag.client_id)
                .and_modify(|w| w.record(tag.seq, slot))
                .or_insert_with(|| {
                    let mut w = DedupWindow::default();
                    w.record(tag.seq, slot);
                    w
                });
        }
    }

    /// Decode `payload` and apply it to the engine at `slot`.
    ///
    /// An empty payload (`NoOp` repair fill) decodes to an empty batch and
    /// advances C++ `contiguous_slot_` via [`KVEngine::noop`] (Gap 1 fix) so
    /// `flush()` can drain past it; the Rust frontier is advanced by the
    /// caller in `learn`. Per-key highest-slot-wins is enforced inside
    /// [`KVEngine::apply`]. `async fn` for the same reason as
    /// [`Self::engine_get`]; `KVEngine::apply` has no genuine `Pending` path
    /// yet either (no async apply C API), so this never actually suspends
    /// today.
    ///
    /// Returns `Err` on a local apply failure (e.g. a durable I/O error on a
    /// `CrowdbTreeEngine`). The error is logged at `ERROR` here; the caller
    /// must NOT advance `contiguous_applied` on `Err` (Gap 2 fix) so
    /// linearizable reads stall at the failed slot instead of reading
    /// stale/missing data. The value is still Paxos-chosen (this is a local
    /// durability fault, not a consensus outcome), so it must not be
    /// re-proposed. Detecting and reacting to a persistently-unhealthy
    /// local engine is the caller's job at a layer that can see engine
    /// health across calls, not a single failed apply.
    pub(crate) async fn apply_entry(&self, slot: SlotIndex, payload: &Bytes) -> Result<(), String> {
        // Test-only apply gate: park until the test releases, so the R35
        // fence test can hold the spawned R17 apply deterministically. The
        // guard is dropped at the `;` so the `Notify` is awaited without a
        // non-Send lock guard held across the await.
        #[cfg(feature = "test-util")]
        let gate = self.apply_gate.lock().clone();
        #[cfg(feature = "test-util")]
        if let Some(gate) = gate {
            gate.notified().await;
        }
        let batch = Batch::decode(payload);
        if batch.ops.is_empty() {
            // Gap 1 fix: NoOp slots (from `repair_once` gap-fill) must
            // still advance C++ `contiguous_slot_` so `flush()` can drain
            // past them and `last_applied_slot_` advances. Without this,
            // a single NoOp permanently blocks the durable watermark.
            self.engine.noop(slot);
            return Ok(());
        }
        let apply_start = std::time::Instant::now();
        match self.engine.apply(slot, &batch).await {
            Ok(()) => {}
            Err(error) => {
                tracing::error!(
                    slot,
                    error = %error,
                    "critical: KVEngine::apply failed -- this slot is Paxos-chosen but may not be \
                     durably reflected in the local engine; next step: check engine health and \
                     consider failing this node out of the group"
                );
                // Gap 2 fix: return Err so the caller does NOT advance
                // `contiguous_applied`. This stalls linearizable reads at
                // the failed slot (visible, debuggable) instead of letting
                // them pass the fence and read stale/missing data.
                return Err(error);
            }
        }
        if let Some(h) = self.engine_apply.get() {
            h.observe(apply_start.elapsed().as_nanos().try_into().unwrap_or(u64::MAX));
        }
        // Gap 5 step 2: if the apply triggered a memtable freeze, wake
        // the maintenance loop so it flushes immediately instead of
        // waiting for the next tick. `flush_pending()` is a cheap atomic
        // read; the notify is a no-op if no signal is wired (tests).
        if self.engine.flush_pending() {
            if let Some(signal) = self.flush_signal.get() {
                signal.notify_one();
            }
        }
        // Watch/notify apply-path hook: if a watch registry is wired
        // and has watchers, emit the changed keys as notify frames.
        // Fires on ALL apply paths (learn, spawned apply, catch-up) —
        // the etcd model. Followers have empty registries (cleared on
        // step-down) so `has_watchers()` is false → zero overhead.
        // The `ArcSwap` load is lock-free (a single atomic load) so
        // the hot path pays no lock even when no registry is wired.
        if let Some(entry) = self.watch_registry.load().as_deref() {
            let (group_id, registry) = entry;
            if registry.has_watchers() {
                registry.emit(*group_id, slot, &batch.ops);
            }
        }
        Ok(())
    }
}

impl Learner for PxLearner {
    async fn learn(&self, entry: PxLogEntry, dedup_tags: &[DedupTag]) {
        // V1 sync path (followers, restore, R17-off leader): apply, then
        // advance both frontiers, then record dedup. With apply synchronous,
        // `contiguous_applied` tracks `contiguous_chosen` exactly — the R35
        // apply fence is a no-op fast path on this path.
        // Gap 2: only advance `contiguous_applied` if the apply succeeded.
        // On error, `contiguous_applied` stalls at the failed slot so
        // linearizable reads block (visible) instead of reading stale data.
        let apply_ok = self.apply_entry(entry.slot, &entry.payload).await.is_ok();
        self.update_chosen_frontier(entry.slot, entry.term);
        if apply_ok {
            self.advance_applied_frontier(entry.slot);
        }
        self.record_dedup_tags(dedup_tags, entry.slot);
    }
}
