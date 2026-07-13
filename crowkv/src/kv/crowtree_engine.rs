//! [`KVEngine`] implementation backed by the crowtree C++ storage engine
//! (FFI adapter over the crowtree C ABI, via `crowtree_ffi`).

use super::op::Cell;
use super::{Batch, KVEngine, Op};
use crowtree_ffi::{BatchOp as CtBatchOp, Crowtree, CtError};

pub use crowtree_ffi::Options as CrowtreeOptions;

/// `KVEngine` backed by [`crowtree_ffi::Crowtree`].
///
/// Wraps the existing *synchronous* `Crowtree` handle directly behind the
/// existing *synchronous* `KVEngine` trait, rather than bridging through
/// `AsyncCrowtree`'s `spawn_blocking` onto an async trait surface. crowtree's
/// own internals are still fully synchronous today (blocking `PageStore`, no
/// async I/O reactor yet), so bridging through `spawn_blocking` here would
/// add a thread-pool hop with no real asynchrony behind it. `PxLearner`
/// already calls `KVEngine` methods synchronously from within async gRPC
/// handlers (`PxKvStore::kv_get`/`kv_scan` call `engine_get`/`engine_scan`
/// inline, not `.await`ed), matching how `parking_lot`/`DashMap` locks are
/// used elsewhere in this codebase, so a brief synchronous crowtree call in
/// the same spot is consistent with the existing pattern. Revisit once the
/// underlying engine I/O is genuinely asynchronous.
///
/// **`iter_all`/`compare` caveat:** `get`/`scan` merge crowtree's in-memory
/// (L0) and durable-tree (L1) state internally, so every `apply` is visible
/// immediately, matching `InMemKV`. `iter_all` (and therefore the default
/// `compare`) instead reads a durable-tree-only snapshot view, so it flushes
/// the contiguous-applied prefix first to catch L0 up to L1; a slot that's
/// still blocked behind an earlier out-of-order gap remains invisible to
/// `iter_all` until the gap fills in, even though `get`/`scan` already see
/// it. `InMemKV` has no such gap (every apply is immediately visible), so
/// this is a real, engine-specific difference to be aware of.
pub struct CrowtreeEngine {
    inner: Crowtree,
}

impl CrowtreeEngine {
    /// Open (recovering durable state when `opt.path` is set, else fresh
    /// in-memory). See [`crowtree_ffi::Crowtree::open`].
    ///
    /// # Errors
    /// Returns the underlying [`CtError`] if the engine fails to open (e.g. a
    /// corrupt or unreadable durable file).
    pub fn open(opt: &CrowtreeOptions) -> Result<Self, CtError> {
        Ok(Self {
            inner: Crowtree::open(opt)?,
        })
    }

    /// Borrow the underlying FFI handle for engine-specific operations
    /// (`flush`/`snapshot`/`snapshot_export`/`snapshot_import`/GC watermark)
    /// that aren't part of the (still-synchronous, pre-#11) [`KVEngine`]
    /// trait surface.
    #[must_use]
    pub fn handle(&self) -> &Crowtree {
        &self.inner
    }
}

impl KVEngine for CrowtreeEngine {
    fn apply(&self, slot: u64, batch: &Batch) {
        if batch.ops.is_empty() {
            return;
        }
        let ops: Vec<CtBatchOp<'_>> = batch
            .ops
            .iter()
            .map(|b| match &b.op {
                Op::Put(v) => CtBatchOp::Put {
                    key: &b.key,
                    value: v,
                },
                Op::Delete => CtBatchOp::Delete { key: &b.key },
            })
            .collect();
        // crowtree enforces per-key highest-slot-wins internally against its
        // own resolved state, same contract InMemKV enforces itself, via one
        // atomic multi-key apply (ct_apply_batch) so a concurrent reader
        // never observes a partially-applied batch.
        //
        // Known gap: `apply` returns `()` in the current sync trait, so an
        // I/O failure surfaced by `apply_batch` (e.g. a synchronous flush
        // hitting a write error) is swallowed here; it's only observable
        // out-of-band via `Crowtree::io_failed()`. A future fallible/async
        // `KVEngine::apply` (returning a `Result`) would close this gap.
        let _ = self.inner.apply_batch(slot, &ops);
    }

    fn get(&self, key: &[u8]) -> Option<(u64, Vec<u8>)> {
        self.inner.get(key).ok().flatten()
    }

    fn scan(&self, prefix: &[u8], limit: usize) -> (Vec<(Vec<u8>, u64, Vec<u8>)>, bool) {
        match self.inner.scan(prefix, limit) {
            Ok((entries, truncated)) => (
                entries.into_iter().map(|e| (e.key, e.slot, e.value)).collect(),
                truncated,
            ),
            Err(_) => (Vec::new(), false),
        }
    }

    fn iter_all(&self) -> Vec<(Vec<u8>, u64, Cell)> {
        // `snapshot_view` only materializes the durable L1 tree, not the L0
        // MemTable an `apply` lands in first -- unlike `get`/`scan`, which
        // already merge L0+L1 internally. Flush the contiguous-applied
        // prefix first so `iter_all`/`compare` observe every `apply` that
        // isn't blocked behind an out-of-order gap, matching `InMemKV`'s
        // immediate visibility for the common (in-order) case. A genuine gap
        // (an out-of-order slot still waiting on an earlier one) is still
        // invisible here until it fills in -- see the crate-level docs.
        let _ = self.inner.flush();
        match self.inner.snapshot_view() {
            Ok((_at_slot, entries)) => entries
                .into_iter()
                .map(|e| {
                    let cell = if e.tombstone {
                        Cell::Tombstone
                    } else {
                        Cell::Value(e.value)
                    };
                    (e.key, e.slot, cell)
                })
                .collect(),
            Err(_) => Vec::new(),
        }
    }

    fn live_key_count(&self) -> usize {
        // No dedicated count primitive in the C API; a full unlimited scan
        // already excludes tombstones server-side, matching InMemKV's own
        // O(n) linear-scan cost for this method.
        self.inner.scan(b"", 0).map_or(0, |(entries, _)| entries.len())
    }

    fn clear(&self) {
        // No native wipe/reset primitive exists in crowtree's C API today (no
        // `ct_clear`). Not currently reachable from any production call site
        // (`KVEngine::clear` has no caller yet in this codebase), so a
        // deliberate panic here surfaces the gap immediately to whoever wires
        // up the first real caller (e.g. snapshot-install reset) instead of
        // silently doing the wrong thing for a file-backed engine.
        unimplemented!("CrowtreeEngine::clear: crowtree has no native reset/wipe primitive yet")
    }

    // `compare` uses the trait's default implementation (diffs `iter_all()`
    // of both sides); no override needed.
}
