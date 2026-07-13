use super::kv_future::KVFuture;
use super::op::{Cell, EngineDiff};
use super::Batch;

/// Storage engine surface. All reads are non-mutating and may run concurrently
/// with `apply`.
///
/// `apply`/`get`/`scan` return [`KVFuture`] rather than their value directly:
/// the common case (in-memory hit / no I/O) resolves immediately at zero
/// cost via [`KVFuture::ready`], while a future engine's genuine I/O path
/// (crowtree demand-load miss, once `design-crowtree-async.md`'s reactor
/// exists) can return a real `Pending` future instead. No [`KVEngine`] impl
/// in this codebase constructs `Pending` yet — see
/// `doc/design/design-crowkv-async-kvengine.md` for the full design and the
/// deferred caller-side conversion plan.
pub trait KVEngine: Send + Sync {
    /// Apply `batch` at `slot`. Atomic to readers and idempotent: an op for
    /// key `k` is skipped when `slot <= resolved_slot(k)`. The last occurrence
    /// of a repeated key within the batch wins.
    fn apply(&self, slot: u64, batch: &Batch) -> KVFuture<()>;

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

    /// Persist a durable snapshot now (`design-crowtree-snapshot-gc.md §3`).
    /// Returns the slot the snapshot covers (everything in `[1, slot]` is
    /// now durably reflected — the same contract [`Self::resume_from_slot`]
    /// documents), or `0` if nothing was persisted (default: `InMemKV` has
    /// no durable store to snapshot). The default `0` is always safe for
    /// callers that feed this into a GC watermark: it just means "nothing
    /// yet safe to reclaim on account of this engine's own durability".
    fn persist_snapshot(&self) -> u64 {
        0
    }

    /// Set the logical retention watermark
    /// (`design-crowtree-snapshot-gc.md §1/§4`): `gc_slot =
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
