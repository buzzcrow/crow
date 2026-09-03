// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Range guard — enforces that a chunkdb instance only processes
//! requests for chunks whose hash bucket falls within its owned
//! ranges.
//!
//! See `doc/working/design-r99-dynamic-range-binding.md` §3.

use std::sync::Arc;

use parking_lot::RwLock;

use crowdb_protocol::chunk_id::ChunkIdParts;
use crowdb_protocol::common::{ChunkId, ChunkdbRangeBindingValue};
use crowdb_protocol::key::ChunkdbRangeBindingKey;

use crate::routing::hash_to_bucket;

/// An owned hash bucket sub-range `[start, end]` (inclusive).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OwnedRange {
    pub start: u16,
    pub end: u16,
    pub sub_range_index: u32,
}

impl OwnedRange {
    /// Check if a bucket falls within this range `[start, end]`.
    fn contains(self, bucket: u16) -> bool {
        bucket >= self.start && bucket <= self.end
    }
}

/// Error returned when a chunk ID's bucket is outside the instance's
/// owned ranges. Carries the current owner's range so the service
/// layer can build a `NotMyRangeHint`.
#[derive(Debug, Clone, thiserror::Error)]
#[error("chunk bucket {bucket} not in owned ranges")]
pub struct NotMyRange {
    pub bucket: u16,
}

/// Range guard — checks chunk IDs against this instance's owned hash
/// bucket ranges.
///
/// When `allow_all_when_empty` is `true` (default for v1 backward
/// compat), an empty guard allows all requests — preserving the
/// single-instance behavior before R99. When `false`, an empty guard
/// rejects all mutating requests until the binding table is loaded.
pub struct RangeGuard {
    owned: Arc<RwLock<Vec<OwnedRange>>>,
    allow_all_when_empty: bool,
}

impl RangeGuard {
    /// Create a new empty range guard.
    #[must_use]
    pub fn new(allow_all_when_empty: bool) -> Self {
        Self {
            owned: Arc::new(RwLock::new(Vec::new())),
            allow_all_when_empty,
        }
    }

    /// Create a guard that allows all requests (v1 single-instance mode).
    /// Equivalent to `new(true)` with no ranges loaded.
    #[must_use]
    pub fn allow_all() -> Self {
        Self::new(true)
    }

    /// Check if a chunk ID's bucket is within owned ranges.
    ///
    /// # Errors
    /// Returns `NotMyRange` if the bucket is outside all owned ranges
    /// and the guard is non-empty, or if the guard is empty and
    /// `allow_all_when_empty` is `false`.
    pub fn check(&self, chunk_id: &ChunkId) -> Result<(), NotMyRange> {
        let bucket = hash_to_bucket(chunk_id);
        let ranges = self.owned.read();
        if ranges.is_empty() {
            if self.allow_all_when_empty {
                return Ok(());
            }
            return Err(NotMyRange { bucket });
        }
        if ranges.iter().any(|r| r.contains(bucket)) {
            Ok(())
        } else {
            Err(NotMyRange { bucket })
        }
    }

    /// Replace the owned ranges (from group-0 binding table or
    /// watch/notify update).
    pub fn replace(&self, ranges: Vec<OwnedRange>) {
        let mut sorted = ranges;
        sorted.sort_by_key(|r| r.start);
        *self.owned.write() = sorted;
    }

    /// Load owned ranges from group-0 binding table for this instance.
    /// Scans `/chunkdb/range_bind/` and filters for entries matching
    /// `instance_id`.
    ///
    /// # Errors
    /// Returns the underlying client error on scan/decode failure.
    pub async fn load_from_group0(
        &self,
        kv: &crowdb_kv_client::CrowdbKvClient,
        instance_id: u64,
    ) -> crowdb_kv_client::Result<()> {
        use crowdb_kv_client::ReadMode;

        let prefix = ChunkdbRangeBindingKey::text_prefix_all();
        let mut new_ranges: Vec<OwnedRange> = Vec::new();
        let mut start_after: Vec<u8> = Vec::new();
        loop {
            let outcome = kv
                .scan(
                    0,
                    0,
                    prefix.as_bytes(),
                    &start_after,
                    &[],
                    0,
                    ReadMode::Linearizable,
                    None,
                    false,
                    None,
                )
                .await?;
            for (k, v) in &outcome.items {
                let path = std::str::from_utf8(k).map_err(|e| crowdb_kv_client::Error::SysdataDecode {
                    key: prefix.clone(),
                    reason: e.to_string(),
                })?;
                let val: ChunkdbRangeBindingValue =
                    serde_json::from_slice(v).map_err(|e| crowdb_kv_client::Error::SysdataDecode {
                        key: path.to_string(),
                        reason: e.to_string(),
                    })?;
                if val.instance_id == instance_id {
                    new_ranges.push(OwnedRange {
                        start: u16::try_from(val.range_start).unwrap_or(0),
                        end: u16::try_from(val.range_end).unwrap_or(u16::MAX),
                        sub_range_index: val.sub_range_index,
                    });
                }
            }
            if !outcome.truncated || outcome.items.is_empty() {
                break;
            }
            if let Some((last_key, _)) = outcome.items.last() {
                start_after = last_key.to_vec();
            } else {
                break;
            }
        }
        new_ranges.sort_by_key(|r| r.start);
        *self.owned.write() = new_ranges;
        Ok(())
    }

    /// Get a snapshot of the owned ranges.
    pub fn snapshot(&self) -> Vec<OwnedRange> {
        self.owned.read().clone()
    }

    /// Check if the guard has any owned ranges loaded.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.owned.read().is_empty()
    }
}

/// Extract the bucket from a chunk ID (utility for the service layer).
pub fn chunk_bucket(chunk_id: &ChunkId) -> u16 {
    ChunkIdParts::from_proto(chunk_id).hash_to_bucket()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn owned(start: u16, end: u16) -> OwnedRange {
        OwnedRange {
            start,
            end,
            sub_range_index: 0,
        }
    }

    fn chunk_with_bucket(bucket: u16) -> ChunkId {
        // Construct a chunk ID that hashes to the given bucket by
        // brute-force search. This is deterministic for testing.
        for high in 0..u64::MAX {
            for low in 0..1024 {
                let id = ChunkId { high, low };
                if hash_to_bucket(&id) == bucket {
                    return id;
                }
            }
        }
        // Fallback — should never reach here.
        ChunkId { high: 0, low: 0 }
    }

    #[test]
    fn check_in_range_ok() {
        let guard = RangeGuard::new(false);
        guard.replace(vec![owned(0, 32_767)]);
        let id = chunk_with_bucket(1000);
        assert!(guard.check(&id).is_ok());
    }

    #[test]
    fn check_out_of_range_rejects() {
        let guard = RangeGuard::new(false);
        guard.replace(vec![owned(0, 1000)]);
        let id = chunk_with_bucket(5000);
        let err = guard.check(&id).unwrap_err();
        assert_eq!(err.bucket, 5000);
    }

    #[test]
    fn check_empty_guard_allow_all() {
        let guard = RangeGuard::new(true);
        let id = chunk_with_bucket(1000);
        assert!(guard.check(&id).is_ok());
    }

    #[test]
    fn check_empty_guard_rejects() {
        let guard = RangeGuard::new(false);
        let id = chunk_with_bucket(1000);
        assert!(guard.check(&id).is_err());
    }

    #[test]
    fn replace_updates_ranges() {
        let guard = RangeGuard::new(false);
        guard.replace(vec![owned(0, 1000)]);
        let id_in = chunk_with_bucket(500);
        assert!(guard.check(&id_in).is_ok());
        // Replace with a new range that excludes bucket 500.
        guard.replace(vec![owned(2000, 3000)]);
        assert!(guard.check(&id_in).is_err());
    }

    #[test]
    fn multiple_disjoint_ranges() {
        let guard = RangeGuard::new(false);
        guard.replace(vec![owned(0, 100), owned(200, 300), owned(500, 600)]);
        let id_in_first = chunk_with_bucket(50);
        let id_in_second = chunk_with_bucket(250);
        let id_in_third = chunk_with_bucket(550);
        let id_outside = chunk_with_bucket(150);
        assert!(guard.check(&id_in_first).is_ok());
        assert!(guard.check(&id_in_second).is_ok());
        assert!(guard.check(&id_in_third).is_ok());
        assert!(guard.check(&id_outside).is_err());
    }

    #[test]
    fn allow_all_factory() {
        let guard = RangeGuard::allow_all();
        let id = chunk_with_bucket(1000);
        assert!(guard.check(&id).is_ok());
    }

    #[test]
    fn is_empty_true_then_false_after_replace() {
        let guard = RangeGuard::new(false);
        assert!(guard.is_empty());
        guard.replace(vec![owned(0, 100)]);
        assert!(!guard.is_empty());
    }
}
