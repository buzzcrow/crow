// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

use super::kv_future::KVFuture;
use super::op::{Cell, EngineDiff};
use super::Batch;

/// Storage engine surface. All reads are non-mutating and may run concurrently
/// with `apply`.
///
/// `apply`/`get`/`scan` return [`KVFuture`] rather than their value directly:
/// the common case (in-memory hit / no I/O) resolves immediately at zero
/// cost via [`KVFuture::ready`], while a genuine I/O path (crowtree
/// demand-load miss, via the `io_uring` reactor) returns a real `Pending` future. [`super::CrowtreeEngine::get`] and
/// [`super::CrowtreeEngine::scan`] both construct `Pending` for a genuine
/// cold-leaf miss (via `crowtree_ffi::AsyncCrowtree::try_get`/`try_scan`);
/// `InMemKV` and `CrowtreeEngine::apply` always resolve `Ready` (no
/// `ct_apply_*_async` C API exists yet).
pub trait KVEngine: Send + Sync {
    /// Apply `batch` at `slot`. Atomic to readers and idempotent: an op for
    /// key `k` is skipped when `slot <= resolved_slot(k)`. The last occurrence
    /// of a repeated key within the batch wins.
    ///
    /// # Errors
    /// Returns an error string if the underlying write fails (e.g. a durable
    /// I/O error on a [`super::CrowtreeEngine`]). The value is still
    /// Paxos-chosen even on an `Err` here -- this reports a *local* apply/
    /// durability failure, not a consensus outcome, so callers must not
    /// treat it as "not applied" for consensus purposes. `InMemKV` has no
    /// I/O path and always returns `Ok()`.
    fn apply(&self, slot: u64, batch: &Batch) -> KVFuture<Result<(), String>>;

    /// Live value and its resolved slot, or `None` if unset or tombstoned.
    fn get(&self, key: &[u8]) -> KVFuture<Option<(u64, Vec<u8>)>>;

    /// Live entries (no tombstones) whose key starts with `prefix`, in key
    /// order, capped at `limit` (`0` = unlimited). Returns `(items, truncated)`
    /// where `truncated` is set when more matches existed than were returned.
    #[allow(clippy::type_complexity)]
    fn scan(&self, prefix: &[u8], limit: usize) -> KVFuture<(Vec<(Vec<u8>, u64, Vec<u8>)>, bool)>;

    /// Full ordered stream including tombstones, for `compare`.
    fn iter_all(&self) -> Vec<(Vec<u8>, u64, Cell)>;

    /// Number of live (non-tombstoned) keys.
    fn live_key_count(&self) -> usize;

    /// Drop all state. Used by snapshot-install reset (before importing a
    /// peer's snapshot) and by tests that need to simulate a wiped replica.
    fn clear(&self);

    /// Type-erased downcast escape hatch. Lets a caller holding only
    /// `&dyn KVEngine` (e.g. `PxLearner::engine`) recover the concrete
    /// engine type for an operation deliberately kept off this trait
    /// because it's meaningful for only one engine kind -- e.g.
    /// [`super::CrowtreeEngine::stats`]: `InMemKV`
    /// has no comparable internals, so putting `stats` on the trait itself
    /// would mean a dummy/`Option`-wrapped implementation for it, the same
    /// reasoning that already keeps [`super::CrowtreeEngine::handle`] off
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
    /// [`super::CrowtreeEngine::is_healthy`] overrides this with
    /// `!Crowtree::io_failed`.
    fn is_healthy(&self) -> bool {
        true
    }

    /// Highest slot `S` such that every slot in `[1, S]` is durably reflected
    /// in this engine already — i.e. a caller rebuilding state from a WAL can
    /// safely skip re-`apply`ing slots `<= S` and start at `S + 1`. Must be
    /// contiguous (no gaps): `S` itself, and every earlier slot, has to
    /// actually be applied, not just "some slot `<= S` was seen".
    ///
    /// The default (`0`) is always correct for a fresh/non-durable engine
    /// ([`super::InMemKV`], or a `CrowtreeEngine` opened with `path: None`) —
    /// it just means "nothing to skip, replay everything". A durable engine
    /// that overrides this (e.g. [`super::CrowtreeEngine`], via
    /// `crowtree_ffi::Crowtree::last_applied_slot`) lets
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
    /// Default: unsupported (`InMemKV` and [`super::CrowtreeEngine`] both
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

    /// Logical diff against `other`, sorted by key. Empty means the two
    /// engines hold the same `(slot, cell)` for every key. Compared exactly,
    /// including resolved-slot and tombstones.
    fn compare(&self, other: &dyn KVEngine) -> Vec<EngineDiff> {
        let left = self.iter_all();
        let right = other.iter_all();
        let mut diffs = Vec::new();
        let (mut i, mut j) = (0usize, 0usize);
        while i < left.len() || j < right.len() {
            match (left.get(i), right.get(j)) {
                (Some(l), Some(r)) => match l.0.cmp(&r.0) {
                    std::cmp::Ordering::Less => {
                        diffs.push(EngineDiff {
                            key: l.0.clone(),
                            left: Some((l.1, l.2.clone())),
                            right: None,
                        });
                        i += 1;
                    }
                    std::cmp::Ordering::Greater => {
                        diffs.push(EngineDiff {
                            key: r.0.clone(),
                            left: None,
                            right: Some((r.1, r.2.clone())),
                        });
                        j += 1;
                    }
                    std::cmp::Ordering::Equal => {
                        if l.1 != r.1 || l.2 != r.2 {
                            diffs.push(EngineDiff {
                                key: l.0.clone(),
                                left: Some((l.1, l.2.clone())),
                                right: Some((r.1, r.2.clone())),
                            });
                        }
                        i += 1;
                        j += 1;
                    }
                },
                (Some(l), None) => {
                    diffs.push(EngineDiff {
                        key: l.0.clone(),
                        left: Some((l.1, l.2.clone())),
                        right: None,
                    });
                    i += 1;
                }
                (None, Some(r)) => {
                    diffs.push(EngineDiff {
                        key: r.0.clone(),
                        left: None,
                        right: Some((r.1, r.2.clone())),
                    });
                    j += 1;
                }
                (None, None) => break,
            }
        }
        diffs
    }
}
