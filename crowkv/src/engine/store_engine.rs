use super::op::{Cell, EngineDiff};
use super::Batch;

/// Storage engine surface. All reads are non-mutating and may run concurrently
/// with `apply`.
pub trait Engine: Send + Sync {
    /// Apply `batch` at `slot`. Atomic to readers and idempotent: an op for
    /// key `k` is skipped when `slot <= resolved_slot(k)`. The last occurrence
    /// of a repeated key within the batch wins.
    fn apply(&self, slot: u64, batch: &Batch);

    /// Live value and its resolved slot, or `None` if unset or tombstoned.
    fn get(&self, key: &[u8]) -> Option<(u64, Vec<u8>)>;

    /// Live entries (no tombstones) whose key starts with `prefix`, in key
    /// order, capped at `limit` (`0` = unlimited). Returns `(items, truncated)`
    /// where `truncated` is set when more matches existed than were returned.
    #[allow(clippy::type_complexity)]
    fn scan(&self, prefix: &[u8], limit: usize) -> (Vec<(Vec<u8>, u64, Vec<u8>)>, bool);

    /// Full ordered stream including tombstones, for `compare`.
    fn iter_all(&self) -> Vec<(Vec<u8>, u64, Cell)>;

    /// Number of live (non-tombstoned) keys.
    fn live_key_count(&self) -> usize;

    /// Drop all state. Used by snapshot-install reset (before importing a
    /// peer's snapshot) and by tests that need to simulate a wiped replica.
    fn clear(&self);

    /// Logical diff against `other`, sorted by key. Empty means the two
    /// engines hold the same `(slot, cell)` for every key. Compared exactly,
    /// including resolved-slot and tombstones.
    fn compare(&self, other: &dyn Engine) -> Vec<EngineDiff> {
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
