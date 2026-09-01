// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! [`KVEngine`] implementation backed by the crowdb-tree C++ storage engine
//! (FFI adapter over the crowdb-tree C ABI, via `crowdb_tree_ffi`).

#[cfg(feature = "test-util")]
use super::op::Cell;
use super::{Batch, KVEngine, KVFuture, Op, SnapshotViewEntry};
use bytes::Bytes;
use crowdb_tree_ffi::{
    AsyncCrowdbtree, Crowdbtree, CtError, ExtOp, GetOutcome, PinnedGetOutcome, ScanOutcome,
};
use std::sync::Arc;

pub use crowdb_tree_ffi::Options as CrowdbTreeOptions;
pub use crowdb_tree_ffi::PageStoreBackend as CrowdbTreeBackend;
pub use crowdb_tree_ffi::Stats as CrowdbTreeStats;

/// `KVEngine` backed by [`crowdb_tree_ffi::Crowdbtree`], via [`AsyncCrowdbtree`].
///
/// `KVEngine::get`/`scan`/`apply` return [`KVFuture`]. `get` and `scan` both
/// genuinely construct [`KVFuture::Pending`] for a cold-leaf miss
/// , via [`AsyncCrowdbtree::try_get`]/
/// [`AsyncCrowdbtree::try_scan`] respectively -- a resident hit/miss
/// (`GetOutcome`/`ScanOutcome`'s `Ready` variant, the overwhelmingly common
/// case) still costs nothing beyond the enum tag, same as before this
/// wiring landed. `apply` stays [`KVFuture::ready`]-only: crowdb-tree has no
/// `ct_apply_*_async` C API yet (only `ct_get_async`/`ct_scan_async`/
/// `ct_flush_async`/`ct_snapshot_async` exist), so it can't
/// genuinely wait on the reactor today; a synchronous crowdb-tree call in its
/// place is an unchanged, honest reflection of what the C API actually
/// offers, not a shortcut taken here.
///
/// **`iter_all`/`compare` note:** `iter_all` uses `scan(b"", 0, true)`
/// which merges L0+L1 internally and includes tombstones, so every `apply`
/// is visible immediately without an explicit `flush()`, matching `InMemKV`.
pub struct CrowdbTreeEngine {
    inner: AsyncCrowdbtree,
}

impl CrowdbTreeEngine {
    /// Open (recovering durable state when `opt.path` is set, else fresh
    /// in-memory). See [`crowdb_tree_ffi::Crowdbtree::open`].
    ///
    /// # Errors
    /// Returns the underlying [`CtError`] if the engine fails to open (e.g. a
    /// corrupt or unreadable durable file).
    pub fn open(opt: &CrowdbTreeOptions) -> Result<Self, CtError> {
        Ok(Self {
            inner: AsyncCrowdbtree::open(opt)?,
        })
    }

    /// Borrow the underlying FFI handle for engine-specific operations
    /// (`flush`/`snapshot`/`snapshot_export`/`snapshot_import`/GC watermark)
    /// that aren't part of the [`KVEngine`] trait surface. Cheap: an `Arc`
    /// clone, sharing the same tree `get`/`scan`/`apply` above operate on.
    #[must_use]
    pub fn handle(&self) -> Arc<Crowdbtree> {
        self.inner.handle()
    }

    /// Batched diagnostics snapshot: durable/GC
    /// watermarks, `io_failed`, last-snapshot page/segment counts, and
    /// buffer-pool occupancy/hit-rate counters. Engine-specific (like
    /// [`Self::handle`]) rather than on the generic [`KVEngine`] trait --
    /// `InMemKV` has no comparable internals to report. O(1); safe to poll
    /// periodically for metrics/console display.
    #[must_use]
    pub fn stats(&self) -> CrowdbTreeStats {
        self.inner.handle().stats()
    }

    /// Flush C++ metrics into a formatted string for the `[cpp-tree]`
    /// log section. Delegates to `crowdb_tree_ffi::Crowdbtree::flush_metrics_str`.
    #[must_use]
    #[allow(dead_code)]
    pub(crate) fn flush_metrics_str(&self, window_secs: f64, timestamp: &str, width: usize) -> String {
        self.inner
            .handle()
            .flush_metrics_str(window_secs, timestamp, width)
    }

    /// Extended flush with negotiated column widths.
    #[must_use]
    pub fn flush_metrics_str_ext(
        &self,
        window_secs: f64,
        timestamp: &str,
        width: usize,
        count_w: usize,
        tps_w: usize,
    ) -> String {
        self.inner
            .handle()
            .flush_metrics_str_ext(window_secs, timestamp, width, count_w, tps_w)
    }

    /// Negotiate column widths with C++. Returns (`count_w`, `tps_w`).
    #[must_use]
    pub fn negotiate_widths(&self, rust_count_w: usize, rust_tps_w: usize) -> (usize, usize) {
        self.inner.handle().negotiate_widths(rust_count_w, rust_tps_w)
    }

    /// Current max metric name length from the C++ registry.
    #[must_use]
    pub fn max_name_len(&self) -> usize {
        self.inner.handle().max_name_len()
    }

    /// Full ordered stream including tombstones. Test-only utility.
    #[cfg(feature = "test-util")]
    #[must_use]
    pub fn iter_all_for_tests(&self) -> Vec<(Vec<u8>, u64, Cell)> {
        // Use scan(b"", 0, true) which merges L0+L1 internally and includes
        // tombstones — no flush() needed, matching InMemKV's immediate
        // visibility for both live entries and tombstones. Scan returns
        // Bytes; iter_all's Cell::Value(Vec<u8>) shape requires .to_vec().
        match self.inner.handle().scan(b"", b"", b"", 0, 0, false, 0, true) {
            Ok((entries, _)) => entries
                .into_iter()
                .map(|e| {
                    let cell = if e.tombstone {
                        Cell::Tombstone
                    } else {
                        Cell::Value(e.value.to_vec())
                    };
                    (e.key.to_vec(), e.slot, cell)
                })
                .collect(),
            Err(_) => Vec::new(),
        }
    }
}

impl KVEngine for CrowdbTreeEngine {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn apply(&self, slot: u64, batch: &Batch) -> KVFuture<Result<(), String>> {
        if batch.ops.is_empty() {
            return KVFuture::ready(Ok(()));
        }
        // R30 zero-copy apply: the value Bytes are borrowed by crowdb-tree via
        // kExternal buffers (no value memcpy on the apply critical path); the
        // copy is deferred to MemTable drain (flush, off the critical path).
        // Bytes::clone is an O(1) Arc ref-bump, so building ExtOps from the
        // borrowed Batch is zero-copy. crowdb-tree enforces per-key
        // highest-slot-wins internally, same contract InMemKV enforces itself,
        // via one atomic multi-key apply so a concurrent reader never observes
        // a partially-applied batch.
        let ops: Vec<ExtOp> = batch
            .ops
            .iter()
            .map(|b| match &b.op {
                Op::Put(v) => ExtOp::Put {
                    key: b.key.clone(),
                    value: v.clone(),
                },
                Op::Delete => ExtOp::Delete { key: b.key.clone() },
            })
            .collect();
        let result = self
            .inner
            .handle()
            .apply_batch_external(slot, ops)
            .map_err(|e| e.to_string());
        KVFuture::ready(result)
    }

    fn get(&self, key: &[u8]) -> KVFuture<Option<(u64, Vec<u8>)>> {
        // AsyncCrowdbtree::try_get does the same fast-path check
        // Crowdbtree::get itself does, first -- a resident hit/miss resolves
        // right here, no allocation, same as the old always-`Ready` body.
        // Only a genuine demand-load miss reaches `Pending`, wrapping the
        // reactor-driven future `try_get` already built for us.
        match self.inner.try_get(key) {
            GetOutcome::Ready(result) => KVFuture::ready(result.ok().flatten()),
            GetOutcome::Pending(fut) => KVFuture::Pending(Box::pin(async move { fut.await.ok().flatten() })),
        }
    }

    fn get_bytes(&self, key: &[u8]) -> KVFuture<Option<(u64, Bytes)>> {
        // try_get_pinned: fast path returns a PinnedValue borrowing directly
        // from the C++ frame. into_bytes() creates a Bytes backed by the
        // C++ frame via Bytes::from_owner — zero-copy, no copy_from_slice.
        // The page refcount pins stay alive until the Bytes is dropped (on
        // any thread, R6). Slow path is identical to get's Pending arm.
        match self.inner.try_get_pinned(key) {
            PinnedGetOutcome::Ready(result) => {
                let mapped = result
                    .ok()
                    .flatten()
                    .map(|(slot, pinned)| (slot, pinned.into_bytes()));
                KVFuture::ready(mapped)
            }
            PinnedGetOutcome::Pending(fut) => KVFuture::Pending(Box::pin(async move {
                fut.await
                    .ok()
                    .flatten()
                    .map(|(slot, vec)| (slot, Bytes::from(vec)))
            })),
        }
    }

    fn scan(
        &self,
        prefix: &[u8],
        start_after: &[u8],
        end_key: &[u8],
        limit: usize,
        byte_budget: usize,
        keys_only: bool,
        deadline_ms: u64,
    ) -> KVFuture<Result<(Vec<(Bytes, u64, Bytes)>, bool), String>> {
        // start_after is pushed down into the C++ engine: the descent targets
        // the leaf containing start_after (instead of the prefix start), and
        // the merge loop skips keys <= start_after natively, so the engine
        // applies the limit without over-fetching the prefix range.
        // end_key is likewise pushed down: the merge loop early-stops once
        // the winner key reaches end_key, so the engine never over-reads
        // past the upper bound.
        // byte_budget is likewise pushed down: the C++ merge loop accumulates
        // key+value bytes and stops with truncated when the budget is
        // exceeded, always returning at least one entry.
        // keys_only is pushed down: the consider lambda skips value assembly
        // (no overflow-chain walk) and stages empty values; the byte budget
        // then accounts for key bytes only.
        // deadline_ms is pushed down: the merge loop checks periodically
        // (every 1024 entries) and breaks early with truncated when exceeded.
        // ScanEntry holds zero-copy Bytes slices into the packed buffer,
        // so decode_scan just re-packages them — no per-entry copy.
        // FFI errors propagate as Err instead of being swallowed.
        let prefix_owned = prefix.to_vec();
        let start_after_owned = start_after.to_vec();
        let end_key_owned = end_key.to_vec();

        match self.inner.try_scan(
            prefix_owned,
            start_after_owned,
            end_key_owned,
            limit,
            byte_budget,
            keys_only,
            deadline_ms,
        ) {
            ScanOutcome::Ready(result) => KVFuture::ready(decode_scan(result)),
            ScanOutcome::Pending(fut) => KVFuture::Pending(Box::pin(async move { decode_scan(fut.await) })),
        }
    }

    fn live_key_count(&self) -> usize {
        // No dedicated count primitive in the C API; a full unlimited scan
        // already excludes tombstones server-side, matching InMemKV's own
        // O(n) linear-scan cost for this method.
        self.inner
            .handle()
            .scan(b"", b"", b"", 0, 0, false, 0, false)
            .map_or(0, |(entries, _)| entries.len())
    }

    fn clear(&self) {
        // `Crowdbtree::clear` (crowdb-tree/ffi) wraps the new `ct_clear` C API
        // entry point, which duplicates the same wipe sequence (epoch-safe
        // resident-page retire + fresh empty root + memtable/watermark
        // reset) `Crowdbtree::install_snapshot` already performs on a live
        // tree before loading imported entries -- not a bespoke reset.
        // Not durable by itself: a caller that needs the wipe to survive a
        // crash must still call `persist_snapshot`/`handle.flush`
        // afterward, same as `snapshot_import`'s own contract.
        //
        // `unwrap`: the only failure mode `Crowdbtree::clear` has is an
        // invalid-argument on a null tree pointer, which can't happen
        // through this safe wrapper -- matches this trait method's
        // infallible signature.
        self.inner
            .handle()
            .clear()
            .expect("CrowdbTreeEngine::clear should never fail through the safe FFI wrapper");
    }

    fn is_healthy(&self) -> bool {
        // `io_failed` is a latched flag set when a demand-load hit an I/O
        // error or CRC mismatch on a committed page; it stays set until an
        // explicit `clear_io_error`, which nothing in this codebase calls
        // yet -- so once tripped, this stays `false` for the engine's
        // lifetime, which is the intended "fail out, don't silently retry"
        // semantics for a durable-storage fault.
        !self.inner.handle().io_failed()
    }

    fn flush(&self) {
        // Gap 3: log flush errors instead of silently swallowing. flush()
        // is in-memory (L0→L1 drain) so it can't hit I/O errors, but
        // internal tree errors (corruption, assertion failures) should be
        // visible. The `io_failed` flag is set by the C++ engine on
        // demand-load faults, not by flush — `is_healthy()` catches that.
        if let Err(e) = self.inner.handle().flush() {
            tracing::error!("flush failed: {:?}", e);
        }
    }

    fn noop(&self, slot: u64) {
        self.inner.handle().force_advance_slot(slot);
    }

    fn flush_pending(&self) -> bool {
        // Gap 5 step 2: check if any frozen memtables are waiting to be
        // drained. `frozen_table_count()` returns the current queue depth;
        // after `flush()` drains them all, it returns 0. This is a cheap
        // shared-lock read, safe to poll after every maintenance pass.
        self.inner.handle().frozen_table_count() > 0
    }

    fn resume_from_slot(&self) -> u64 {
        // `Crowdbtree::last_applied_slot` is `contiguous_slot_` as of the last
        // `flush` (see `Crowdbtree::apply`/`flush`/`recompute_contiguous_locked`
        // in crowdb-tree.cpp) -- a durable, gap-free watermark, not just "the max
        // slot ever seen". On a fresh in-memory engine (`path: None`) or an
        // empty durable file this is `0`. On a recovered durable file it's
        // whatever the on-disk superblock last recorded (`persist.cpp`),
        // which only advances via an explicit `snapshot`/persist -- a
        // conservative floor if no such call happened right before the
        // process that wrote this file exited, which is fine (see the trait
        // doc: under-reporting only means more, still-safe, replay work).
        self.inner.handle().last_applied_slot()
    }

    fn persist_snapshot(&self) -> u64 {
        // O7: callers must flush() before calling persist_snapshot() so L0
        // is already drained into L1. The maintenance loop flushes in step 1
        // before calling this in step 2; the shutdown path
        // (local_replica.rs) also flushes first. This avoids a redundant
        // write_mutex_ acquire inside persist_snapshot when the caller
        // already flushed — the snapshot path itself does NOT hold
        // write_mutex_ (it walks L1 pages which are immutable once
        // published), so pipelining is possible: the next tick's flush can
        // overlap with this snapshot's disk I/O.
        let snap_t0 = std::time::Instant::now();
        let snap_result = self.inner.handle().snapshot();
        let snap_ms = snap_t0.elapsed().as_millis();
        match &snap_result {
            Ok(slot) => tracing::info!("persist_snapshot: snapshot_ms={} snapshot_slot={}", snap_ms, slot),
            Err(e) => tracing::error!("persist_snapshot FAILED: snapshot_ms={} err={:?}", snap_ms, e),
        }
        snap_result.unwrap_or(0)
    }

    fn set_gc_watermark(&self, snapshot_slot: u64, safe_slot: u64) {
        self.inner.handle().set_gc_watermark(snapshot_slot, safe_slot);
    }

    fn collect_garbage(&self) {
        let _ = self.inner.handle().collect_garbage();
    }

    fn snapshot_export(&self) -> Result<(u64, Vec<u8>), String> {
        // Flush first so the export reflects every `apply` up to now, not
        // just whatever an earlier `flush` already moved into L1 --
        // same reasoning as `iter_all`/`persist_snapshot` above.
        let _ = self.inner.handle().flush();
        let stream = self.inner.handle().snapshot_export().map_err(|e| e.to_string())?;
        let at_slot = crowdb_tree_snapshot_at_slot(&stream)?;
        Ok((at_slot, stream))
    }

    fn snapshot_import(&self, stream: &[u8]) -> Result<u64, String> {
        self.inner
            .handle()
            .snapshot_import(stream)
            .map_err(|e| e.to_string())
    }

    fn snapshot_view(&self) -> Result<(u64, Vec<SnapshotViewEntry>), String> {
        // Flush first so the view reflects every `apply` up to now, not
        // just whatever an earlier `flush` already moved into L1.
        let _ = self.inner.handle().flush();
        let (at_slot, entries) = self
            .inner
            .handle()
            .snapshot_view()
            .map_err(|e| format!("snapshot_view: {e:?}"))?;
        let out = entries
            .into_iter()
            .map(|e| SnapshotViewEntry {
                key: e.key,
                slot: e.slot,
                tombstone: e.tombstone,
                value: e.value,
            })
            .collect();
        Ok((at_slot, out))
    }

    // `compare` uses the trait's default implementation (diffs `iter_all`
    // of both sides); no override needed.
}

/// Shared tail of [`CrowdbTreeEngine::scan`]'s `Ready`/`Pending` arms:
/// converts a raw `crowdb_tree_ffi::ScanEntry` result (keys/values are
/// already zero-copy `Bytes` slices into the packed buffer) into the
/// `KVEngine::scan` return shape. The C++ engine already applied both the
/// `start_after` exclusive lower bound and the `limit`, so the truncated
/// flag is directly trustworthy. Errors propagate as `Err` instead of
/// being silently swallowed as an empty `ok` result.
type ScanResult = Result<(Vec<(Bytes, u64, Bytes)>, bool), String>;

fn decode_scan(result: Result<(Vec<crowdb_tree_ffi::ScanEntry>, bool), CtError>) -> ScanResult {
    match result {
        Ok((entries, truncated)) => {
            let items: Vec<(Bytes, u64, Bytes)> =
                entries.into_iter().map(|e| (e.key, e.slot, e.value)).collect();
            Ok((items, truncated))
        }
        Err(e) => Err(e.to_string()),
    }
}

/// Parse the `at_slot` field out of a crowdb-tree portable-format snapshot
/// export stream's header, without waiting on a second FFI round-trip
/// ([`crowdb_tree_ffi::Crowdbtree::last_applied_slot`]) that could race a
/// concurrent `apply`/`flush` between the two calls.
///
/// Portable header layout (`crowdb-tree/src/snapshot_io.cpp`'s `kSnapHeader`,
/// little-endian): `[magic:u32][version:u32][format:u8][at_slot:u64]
/// [entry_count:u64]`. `ct_snapshot_export_begin` always uses
/// `snapshot_format::kPortable` (`crowdb-tree/src/c_api.cpp`) -- crowdb-tree's C
/// API has no format parameter, so this layout is the only one
/// [`crowdb_tree_ffi::Crowdbtree::snapshot_export`] can ever produce.
fn crowdb_tree_snapshot_at_slot(stream: &[u8]) -> Result<u64, String> {
    stream
        .get(9..17)
        .map(|b| u64::from_le_bytes(b.try_into().expect("slice len checked by get(9..17)")))
        .ok_or_else(|| "crowdb-tree snapshot export: stream too short for header".to_string())
}
