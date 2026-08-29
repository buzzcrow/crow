// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::kv_future::KVFuture;
use super::Batch;

use bytes::Bytes;

/// Storage engine surface. All reads are non-mutating and may run concurrently
/// with `apply`.
///
/// `apply`/`get`/`scan` return [`KVFuture`] rather than their value directly:
/// the common case (in-memory hit / no I/O) resolves immediately at zero
/// cost via [`KVFuture::ready`], while a genuine I/O path (crowdb-tree
/// demand-load miss, via the `io_uring` reactor) returns a real `Pending` future. [`super::CrowdbTreeEngine::get`] and
/// [`super::CrowdbTreeEngine::scan`] both construct `Pending` for a genuine
/// cold-leaf miss (via `crowdb_tree_ffi::AsyncCrowdbtree::try_get`/`try_scan`);
/// `InMemKV` and `CrowdbTreeEngine::apply` always resolve `Ready` (no
/// `ct_apply_*_async` C API exists yet).
pub trait KVEngine: Send + Sync {
    /// Apply `batch` at `slot`. Atomic to readers and idempotent: an op for
    /// key `k` is skipped when `slot <= resolved_slot(k)`. The last occurrence
    /// of a repeated key within the batch wins.
    ///
    /// # Errors
    /// Returns an error string if the underlying write fails (e.g. a durable
    /// I/O error on a [`super::CrowdbTreeEngine`]). The value is still
    /// Paxos-chosen even on an `Err` here -- this reports a *local* apply/
    /// durability failure, not a consensus outcome, so callers must not
    /// treat it as "not applied" for consensus purposes. `InMemKV` has no
    /// I/O path and always returns `Ok()`.
    fn apply(&self, slot: u64, batch: &Batch) -> KVFuture<Result<(), String>>;

    /// Live value and its resolved slot, or `None` if unset or tombstoned.
    fn get(&self, key: &[u8]) -> KVFuture<Option<(u64, Vec<u8>)>>;

    /// Like [`Self::get`] but returns [`Bytes`] instead of `Vec<u8>`, so
    /// the crowdb-rpc response path can avoid an extra allocation. The default
    /// implementation delegates to [`Self::get`] and converts the
    /// `Vec<u8>` into `Bytes` (zero-copy move). [`super::CrowdbTreeEngine`]
    /// overrides this to use a pinned-value FFI path that eliminates the
    /// intermediate `Vec<u8>` allocation on the fast path.
    fn get_bytes(&self, key: &[u8]) -> KVFuture<Option<(u64, Bytes)>> {
        match self.get(key) {
            KVFuture::Ready(v) => KVFuture::ready(v.flatten().map(|(slot, vec)| (slot, Bytes::from(vec)))),
            KVFuture::Pending(fut) => KVFuture::Pending(Box::pin(async move {
                fut.await.map(|(slot, vec)| (slot, Bytes::from(vec)))
            })),
        }
    }

    /// Live entries (no tombstones) whose key starts with `prefix`, in key
    /// order, capped at `limit` (`0` = unlimited). Returns `(items, truncated)`
    /// where `truncated` is set when more matches existed than were returned.
    /// `start_after` is an exclusive lower bound (empty = start from
    /// beginning); only keys strictly greater than `start_after` are
    /// returned, enabling cursor-based pagination. `end_key` is an exclusive
    /// upper bound (empty = unbounded); only keys strictly less than
    /// `end_key` are returned. `byte_budget` (`0` = unlimited) caps the total
    /// key+value bytes emitted; the scan stops with `truncated = true` when
    /// exceeded, always returning at least one entry (so a single oversized
    /// entry still makes progress). `keys_only` skips value materialization
    /// (no overflow-chain assembly): entries carry empty values and the byte
    /// budget accounts for key bytes only. Entry keys and values are zero-copy
    /// `Bytes`: for `CrowdbTreeEngine` they slice into the C++ packed result
    /// buffer; for `InMemKV` they are `Bytes::from` of the owned `Vec<u8>`
    /// (one copy, same as its get path). Errors (e.g. `CtError::Corruption`
    /// from packed-result bounds checks) propagate as `Err` instead of being
    /// silently swallowed as an empty result — callers map to an error
    /// response, not a wrong `ok` answer.
    #[allow(clippy::type_complexity)]
    #[allow(clippy::too_many_arguments)]
    fn scan(
        &self,
        prefix: &[u8],
        start_after: &[u8],
        end_key: &[u8],
        limit: usize,
        byte_budget: usize,
        keys_only: bool,
        deadline_ms: u64,
    ) -> KVFuture<Result<(Vec<(Bytes, u64, Bytes)>, bool), String>>;

    /// Number of live (non-tombstoned) keys.
    fn live_key_count(&self) -> usize;

    /// Drop all state. Used by snapshot-install reset (before importing a
    /// peer's snapshot) and by tests that need to simulate a wiped replica.
    fn clear(&self);

    /// Type-erased downcast escape hatch. Lets a caller holding only
    /// `&dyn KVEngine` (e.g. `PxLearner::engine`) recover the concrete
    /// engine type for an operation deliberately kept off this trait
    /// because it's meaningful for only one engine kind -- e.g.
    /// [`super::CrowdbTreeEngine::stats`]: `InMemKV`
    /// has no comparable internals, so putting `stats` on the trait itself
    /// would mean a dummy/`Option`-wrapped implementation for it, the same
    /// reasoning that already keeps [`super::CrowdbTreeEngine::handle`] off
    /// this trait. Every implementor's body is just `self`.
    fn as_any(&self) -> &dyn std::any::Any;

    /// Whether this engine's underlying storage is currently healthy enough
    /// to trust for durable reads/writes. Distinct from a single failed
    /// [`Self::apply`] call (a point-in-time event, already logged by the
    /// caller): this reflects a *latched* fault condition a caller can poll
    /// independently of any particular operation, e.g. to decide whether
    /// this replica should be failed out of its group -- the same fail-out
    /// semantics specified for a WAL disk failure, extended here to an
    /// engine-level fault (note: as of this writing there is no automatic
    /// step-out trigger wired to *either* signal yet, only the manual admin
    /// remove-replica path).
    ///
    /// Default `true` (`InMemKV` has no I/O path to fail).
    /// [`super::CrowdbTreeEngine::is_healthy`] overrides this with
    /// `!Crowdbtree::io_failed`.
    fn is_healthy(&self) -> bool {
        true
    }

    /// Drain the in-memory write buffer (L0 memtable) into the in-memory
    /// B+tree (L1), advancing `last_applied_slot` **in memory only** — not
    /// durable across crashes. A subsequent [`Self::persist_snapshot`] is
    /// needed to make the advanced watermark crash-safe. Cheap when L0 is
    /// empty (early-return no-op). Default: no-op (`InMemKV` has no L0/L1
    /// distinction).
    fn flush(&self) {}

    /// Highest slot `S` such that every slot in `[1, S]` is durably reflected
    /// in this engine already — i.e. a caller rebuilding state from a WAL can
    /// safely skip re-`apply`ing slots `<= S` and start at `S + 1`. Must be
    /// contiguous (no gaps): `S` itself, and every earlier slot, has to
    /// actually be applied, not just "some slot `<= S` was seen".
    ///
    /// The default (`0`) is always correct for a fresh/non-durable engine
    /// ([`super::InMemKV`], or a `CrowdbTreeEngine` opened with `path: None`) —
    /// it just means "nothing to skip, replay everything". A durable engine
    /// that overrides this (e.g. [`super::CrowdbTreeEngine`], via
    /// `crowdb_tree_ffi::Crowdbtree::last_applied_slot`) lets
    /// [`crate::cluster::local_replica::PxLocalReplica::restore_from_replay_with_engine`]
    /// skip re-walking an already-durable WAL prefix. Only meaningful to call
    /// once, right after opening a freshly-recovered engine and before any
    /// `apply` calls in the current process — an engine that has taken live
    /// applies since its last durable checkpoint may report a value staler
    /// than its true in-memory state (safe: callers only use this to decide
    /// what to *skip*, so under-reporting just means more (harmless,
    /// idempotent) replay work, never less).
    fn resume_from_slot(&self) -> u64 {
        0
    }

    /// Persist a durable snapshot now.
    /// Returns the slot the snapshot covers (everything in `[1, slot]` is
    /// now durably reflected — the same contract [`Self::resume_from_slot`]
    /// documents), or `0` if nothing was persisted (default: `InMemKV` has
    /// no durable store to snapshot). The default `0` is always safe for
    /// callers that feed this into a GC watermark: it just means "nothing
    /// yet safe to reclaim on account of this engine's own durability".
    fn persist_snapshot(&self) -> u64 {
        0
    }

    /// Set the logical retention watermark: `gc_slot =
    /// min(snapshot_slot, safe_slot)`. Tombstones and stale versions with
    /// slot `<= gc_slot` become eligible for reclamation on the next
    /// [`Self::collect_garbage`] call. Default is a no-op (`InMemKV` does
    /// not track a retention watermark or drop tombstones today).
    fn set_gc_watermark(&self, snapshot_slot: u64, safe_slot: u64) {
        let _ = (snapshot_slot, safe_slot);
    }

    /// Best-effort reclamation sweep below the watermark set by the last
    /// [`Self::set_gc_watermark`] call. Default is a no-op.
    fn collect_garbage(&self) {}

    /// Export this engine's entire current state as an opaque,
    /// engine-specific byte stream, for the new-member join flow: a fresh/far-lagging
    /// replica pulls this over [`crate::rpc::SnapshotService`] instead of
    /// replaying full Paxos history. Returns `(at_slot, stream)`: `at_slot`
    /// is the highest slot durably reflected in `stream` (same contract as
    /// [`Self::resume_from_slot`]/[`Self::persist_snapshot`]); `stream` is
    /// only ever meaningful fed back into **this same engine kind's**
    /// [`Self::snapshot_import`] — never across engine kinds.
    ///
    /// Default: unsupported (`InMemKV` and [`super::CrowdbTreeEngine`] both
    /// override this with a real implementation; a future engine kind that
    /// doesn't gets a clear error instead of silently returning empty
    /// state).
    ///
    /// # Errors
    /// Returns an error string if this engine kind does not support
    /// snapshot export, or if the underlying export fails.
    fn snapshot_export(&self) -> Result<(u64, Vec<u8>), String> {
        Err("snapshot export not supported by this engine".to_string())
    }

    /// Import a byte stream produced by [`Self::snapshot_export`] on
    /// **another replica's same-kind engine**, replacing this engine's
    /// entire state. Returns the `at_slot` the imported snapshot covers
    /// (the same value the exporter returned).
    ///
    /// Only ever called on a freshly-constructed, still-empty engine — the
    /// join flow's contract, mirroring [`Self::resume_from_slot`]'s "before
    /// any `apply` calls in this process" precondition. Never called on a
    /// live engine with existing local state.
    ///
    /// Default: unsupported.
    ///
    /// # Errors
    /// Returns an error string if this engine kind does not support
    /// snapshot import, or if `stream` is malformed / fails to decode.
    fn snapshot_import(&self, stream: &[u8]) -> Result<u64, String> {
        let _ = stream;
        Err("snapshot import not supported by this engine".to_string())
    }

    /// Pin a point-in-time-consistent L1 view at `last_applied_slot`
    /// (after a `flush` drains L0 → L1). Returns `(at_slot, entries)`:
    /// a frozen, key-sorted vector including tombstones. Default:
    /// unsupported — `InMemKV` has no L1 pinning; `CrowdbTreeEngine`
    /// overrides with the real FFI `snapshot_view`.
    ///
    /// # Errors
    /// Returns an error string if the engine does not support snapshot
    /// views, or if the underlying pin fails.
    fn snapshot_view(&self) -> Result<(u64, Vec<SnapshotViewEntry>), String> {
        Err("snapshot_view not supported by this engine".to_string())
    }
}

/// A single entry in a pinned snapshot view — key, slot, tombstone flag,
/// value. Key-sorted; includes tombstones (callers filter them for live-
/// key scans).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotViewEntry {
    pub key: Vec<u8>,
    pub slot: u64,
    pub tombstone: bool,
    pub value: Vec<u8>,
}
