//! [`KVEngine`] implementation backed by the crowtree C++ storage engine
//! (FFI adapter over the crowtree C ABI, via `crowtree_ffi`).

use super::op::Cell;
use super::{Batch, KVEngine, KVFuture, Op};
use crowtree_ffi::{AsyncCrowtree, BatchOp as CtBatchOp, Crowtree, CtError, GetOutcome, ScanOutcome};
use std::sync::Arc;

pub use crowtree_ffi::Options as CrowtreeOptions;
pub use crowtree_ffi::PageStoreBackend as CrowtreeBackend;
pub use crowtree_ffi::Stats as CrowtreeStats;

/// `KVEngine` backed by [`crowtree_ffi::Crowtree`], via [`AsyncCrowtree`].
///
/// `KVEngine::get`/`scan`/`apply` return [`KVFuture`]. `get` and `scan` both
/// genuinely construct [`KVFuture::Pending`] for a cold-leaf miss
/// (plan-tree.md #11 Phase 6), via [`AsyncCrowtree::try_get`]/
/// [`AsyncCrowtree::try_scan`] respectively -- a resident hit/miss
/// (`GetOutcome`/`ScanOutcome`'s `Ready` variant, the overwhelmingly common
/// case) still costs nothing beyond the enum tag, same as before this
/// wiring landed. `apply` stays [`KVFuture::ready`]-only: crowtree has no
/// `ct_apply_*_async` C API yet (only `ct_get_async`/`ct_scan_async`/
/// `ct_flush_async`/`ct_snapshot_async` exist -- see
/// `doc/design/design-crowtree-async.md` §4's table), so it can't
/// genuinely wait on the reactor today; a synchronous crowtree call in its
/// place is an unchanged, honest reflection of what the C API actually
/// offers, not a shortcut taken here.
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
    inner: AsyncCrowtree,
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
            inner: AsyncCrowtree::open(opt)?,
        })
    }

    /// Borrow the underlying FFI handle for engine-specific operations
    /// (`flush`/`snapshot`/`snapshot_export`/`snapshot_import`/GC watermark)
    /// that aren't part of the [`KVEngine`] trait surface. Cheap: an `Arc`
    /// clone, sharing the same tree `get`/`scan`/`apply` above operate on.
    #[must_use]
    pub fn handle(&self) -> Arc<Crowtree> {
        self.inner.handle()
    }

    /// Batched diagnostics snapshot (doc/todo-sm.md Step 6): durable/GC
    /// watermarks, `io_failed`, last-snapshot page/segment counts, and
    /// buffer-pool occupancy/hit-rate counters. Engine-specific (like
    /// [`Self::handle`]) rather than on the generic [`KVEngine`] trait --
    /// `InMemKV` has no comparable internals to report. O(1); safe to poll
    /// periodically for metrics/console display.
    #[must_use]
    pub fn stats(&self) -> CrowtreeStats {
        self.inner.handle().stats()
    }
}

impl KVEngine for CrowtreeEngine {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn apply(&self, slot: u64, batch: &Batch) -> KVFuture<Result<(), String>> {
        if batch.ops.is_empty() {
            return KVFuture::ready(Ok(()));
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
        let result = self
            .inner
            .handle()
            .apply_batch(slot, &ops)
            .map_err(|e| e.to_string());
        KVFuture::ready(result)
    }

    fn get(&self, key: &[u8]) -> KVFuture<Option<(u64, Vec<u8>)>> {
        // AsyncCrowtree::try_get does the same fast-path check
        // Crowtree::get itself does, first -- a resident hit/miss resolves
        // right here, no allocation, same as the old always-`Ready` body.
        // Only a genuine demand-load miss reaches `Pending`, wrapping the
        // reactor-driven future `try_get` already built for us.
        match self.inner.try_get(key.to_vec()) {
            GetOutcome::Ready(result) => KVFuture::ready(result.ok().flatten()),
            GetOutcome::Pending(fut) => KVFuture::Pending(Box::pin(async move { fut.await.ok().flatten() })),
        }
    }

    fn scan(&self, prefix: &[u8], limit: usize) -> KVFuture<(Vec<(Vec<u8>, u64, Vec<u8>)>, bool)> {
        // AsyncCrowtree::try_scan does the same fast-path check
        // Crowtree::scan itself does, first -- a scan whose whole range is
        // already resident resolves right here, no allocation beyond the
        // returned entries, same as the old always-`Ready` body. Only a
        // genuine cold-leaf miss reaches `Pending`, wrapping the
        // reactor-driven future `try_scan` already built for us.
        match self.inner.try_scan(prefix.to_vec(), limit) {
            ScanOutcome::Ready(result) => KVFuture::ready(decode_scan_result(result)),
            ScanOutcome::Pending(fut) => {
                KVFuture::Pending(Box::pin(async move { decode_scan_result(fut.await) }))
            }
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
        let _ = self.inner.handle().flush();
        match self.inner.handle().snapshot_view() {
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
        self.inner
            .handle()
            .scan(b"", 0)
            .map_or(0, |(entries, _)| entries.len())
    }

    fn clear(&self) {
        // `Crowtree::clear` (crowtree/ffi) wraps the new `ct_clear` C API
        // entry point, which duplicates the same wipe sequence (epoch-safe
        // resident-page retire + fresh empty root + memtable/watermark
        // reset) `Crowtree::install_snapshot` already performs on a live
        // tree before loading imported entries -- not a bespoke reset.
        // Not durable by itself: a caller that needs the wipe to survive a
        // crash must still call `persist_snapshot`/`handle().flush()`
        // afterward, same as `snapshot_import`'s own contract.
        //
        // `unwrap`: the only failure mode `Crowtree::clear` has is an
        // invalid-argument on a null tree pointer, which can't happen
        // through this safe wrapper -- matches this trait method's
        // infallible signature.
        self.inner
            .handle()
            .clear()
            .expect("CrowtreeEngine::clear should never fail through the safe FFI wrapper");
    }

    fn is_healthy(&self) -> bool {
        // `io_failed()` is a latched flag set when a demand-load hit an I/O
        // error or CRC mismatch on a committed page; it stays set until an
        // explicit `clear_io_error()`, which nothing in this codebase calls
        // yet -- so once tripped, this stays `false` for the engine's
        // lifetime, which is the intended "fail out, don't silently retry"
        // semantics for a durable-storage fault.
        !self.inner.handle().io_failed()
    }

    fn resume_from_slot(&self) -> u64 {
        // `Crowtree::last_applied_slot` is `contiguous_slot_` as of the last
        // `flush()` (see `Crowtree::apply`/`flush`/`recompute_contiguous_locked`
        // in crowtree.cpp) -- a durable, gap-free watermark, not just "the max
        // slot ever seen". On a fresh in-memory engine (`path: None`) or an
        // empty durable file this is `0`. On a recovered durable file it's
        // whatever the on-disk superblock last recorded (`persist.cpp`),
        // which only advances via an explicit `snapshot()`/persist -- a
        // conservative floor if no such call happened right before the
        // process that wrote this file exited, which is fine (see the trait
        // doc: under-reporting only means more, still-safe, replay work).
        self.inner.handle().last_applied_slot()
    }

    fn persist_snapshot(&self) -> u64 {
        // `Crowtree::snapshot` persists dirty *L1* pages + a fresh
        // superblock recording `last_applied_slot` -- but it doesn't drain
        // L0 itself (that's `flush`'s job; `snapshot` only walks the
        // already-durable-tree side). Flush first so a snapshot taken right
        // after a burst of `apply` calls captures them, instead of
        // silently persisting only whatever an earlier `flush()` already
        // moved into L1.
        let _ = self.inner.handle().flush();
        self.inner.handle().snapshot().unwrap_or(0)
    }

    fn set_gc_watermark(&self, snapshot_slot: u64, safe_slot: u64) {
        self.inner.handle().set_gc_watermark(snapshot_slot, safe_slot);
    }

    fn collect_garbage(&self) {
        let _ = self.inner.handle().collect_garbage();
    }

    fn snapshot_export(&self) -> Result<(u64, Vec<u8>), String> {
        // Flush first so the export reflects every `apply` up to now, not
        // just whatever an earlier `flush()` already moved into L1 --
        // same reasoning as `iter_all`/`persist_snapshot` above.
        let _ = self.inner.handle().flush();
        let stream = self.inner.handle().snapshot_export().map_err(|e| e.to_string())?;
        let at_slot = crowtree_snapshot_at_slot(&stream)?;
        Ok((at_slot, stream))
    }

    fn snapshot_import(&self, stream: &[u8]) -> Result<u64, String> {
        self.inner
            .handle()
            .snapshot_import(stream)
            .map_err(|e| e.to_string())
    }

    // `compare` uses the trait's default implementation (diffs `iter_all()`
    // of both sides); no override needed.
}

/// Shared tail of [`CrowtreeEngine::scan`]'s `Ready`/`Pending` arms:
/// converts a raw `crowtree_ffi::ScanEntry` result into the
/// `KVEngine::scan` return shape, collapsing an error to an empty,
/// non-truncated result (matching the prior always-synchronous body's
/// error handling).
type ScanResult = (Vec<(Vec<u8>, u64, Vec<u8>)>, bool);

fn decode_scan_result(result: Result<(Vec<crowtree_ffi::ScanEntry>, bool), CtError>) -> ScanResult {
    match result {
        Ok((entries, truncated)) => (
            entries.into_iter().map(|e| (e.key, e.slot, e.value)).collect(),
            truncated,
        ),
        Err(_) => (Vec::new(), false),
    }
}

/// Parse the `at_slot` field out of a crowtree portable-format snapshot
/// export stream's header, without waiting on a second FFI round-trip
/// ([`crowtree_ffi::Crowtree::last_applied_slot`]) that could race a
/// concurrent `apply`/`flush` between the two calls.
///
/// Portable header layout (`crowtree/src/snapshot_io.cpp`'s `kSnapHeader`,
/// little-endian): `[magic:u32][version:u32][format:u8][at_slot:u64]
/// [entry_count:u64]`. `ct_snapshot_export_begin` always uses
/// `snapshot_format::kPortable` (`crowtree/src/c_api.cpp`) -- crowtree's C
/// API has no format parameter, so this layout is the only one
/// [`crowtree_ffi::Crowtree::snapshot_export`] can ever produce.
fn crowtree_snapshot_at_slot(stream: &[u8]) -> Result<u64, String> {
    stream
        .get(9..17)
        .map(|b| u64::from_le_bytes(b.try_into().expect("slice len checked by get(9..17)")))
        .ok_or_else(|| "crowtree snapshot export: stream too short for header".to_string())
}
