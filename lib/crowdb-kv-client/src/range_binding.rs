// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#![allow(clippy::missing_errors_doc, clippy::cast_possible_truncation)]

//! chunkdb instance range binding client.
//!
//! Reads the chunkdb instance binding table from group 0
//! (`/chunkdb/range_bind/<range_start>` → `ChunkdbRangeBindingValue`)
//! and routes chunk IDs to the owning chunkdb instance. The binding
//! table is cached locally and refreshed on demand or via watch/notify.
//!
//! See `doc/working/design-r99-dynamic-range-binding.md` §2.

use std::sync::Arc;

use parking_lot::RwLock;
use tracing::warn;

use crowdb_protocol::chunk_id::ChunkIdParts;
use crowdb_protocol::common::ChunkId;
use crowdb_protocol::common::{ChunkdbRangeBindingValue, RangeStatus};
use crowdb_protocol::key::ChunkdbRangeBindingKey;

use crate::watch_notify::WatchNotifyClient;
use crate::{CrowdbKvClient, Error, ReadMode, Result};

const G0_STORE: u64 = 0;
const G0_GROUP: u64 = 0;

/// A chunkdb instance range binding: a sub-range → chunkdb instance.
/// The sub-range covers bucket `[range_start, range_end]` (inclusive).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkdbRangeBinding {
    pub sub_range_index: u32,
    pub range_start: u16,
    pub range_end: u16,
    pub instance_id: u64,
    pub rpc_endpoint: String,
    pub original_instance_id: u64,
    pub original_endpoint: String,
    pub status: RangeStatus,
    pub last_change_time_ms: u64,
}

impl ChunkdbRangeBinding {
    /// Check if a bucket falls within this range `[start, end]`.
    fn contains(&self, bucket: u16) -> bool {
        bucket >= self.range_start && bucket <= self.range_end
    }

    /// Convert from the proto value type.
    fn from_proto(v: &ChunkdbRangeBindingValue) -> Self {
        Self {
            sub_range_index: v.sub_range_index,
            range_start: u16::try_from(v.range_start).unwrap_or(0),
            range_end: u16::try_from(v.range_end).unwrap_or(u16::MAX),
            instance_id: v.instance_id,
            rpc_endpoint: v.rpc_endpoint.clone(),
            original_instance_id: v.original_instance_id,
            original_endpoint: v.original_endpoint.clone(),
            status: RangeStatus::try_from(v.status).unwrap_or(RangeStatus::Stable),
            last_change_time_ms: v.last_change_time_ms,
        }
    }
}

/// Route result with optional fallback for transition states.
#[derive(Debug, Clone)]
pub struct RouteWithFallback {
    /// Current owner — try this first.
    pub primary: ChunkdbRangeBinding,
    /// Original owner — fall back to this on `NotMyRange` or connection
    /// error when `status = InTransition`.
    pub fallback: Option<ChunkdbRangeBinding>,
}

/// Range routing error.
#[derive(Debug, thiserror::Error)]
pub enum RangeRouteError {
    #[error("binding table empty — no chunkdb instance routing information")]
    NoBinding,
    #[error("bucket {bucket} has no chunkdb instance binding")]
    BucketUnbound { bucket: u16 },
    #[error("binding refresh failed: {0}")]
    Refresh(String),
}

/// Client for the chunkdb instance range binding table in group 0.
///
/// All methods target store 0, group 0. The wrapped `CrowdbKvClient`
/// must have its topology seeded with a group-0 leader endpoint.
pub struct RangeBindingClient {
    kv: Arc<CrowdbKvClient>,
    /// Cached binding table, sorted by `sub_range_index` (the
    /// canonical key). Protected by a `RwLock` — reads (routing) take
    /// a read lock, refreshes take a write lock.
    bindings: Arc<RwLock<Vec<ChunkdbRangeBinding>>>,
}

impl RangeBindingClient {
    /// Wrap an already-shared `CrowdbKvClient` for chunkdb range binding
    /// access.
    #[must_use]
    pub fn from_shared(kv: Arc<CrowdbKvClient>) -> Self {
        Self {
            kv,
            bindings: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Access the underlying `CrowdbKvClient`.
    #[must_use]
    pub fn kv(&self) -> &CrowdbKvClient {
        &self.kv
    }

    /// Refresh the cached binding table by scanning group 0 for all
    /// `/chunkdb/range_bind/` entries. Replaces the entire cache.
    pub async fn refresh(&self) -> Result<()> {
        let prefix = ChunkdbRangeBindingKey::text_prefix_all();
        let mut new_bindings: Vec<ChunkdbRangeBinding> = Vec::new();
        let mut start_after: Vec<u8> = Vec::new();
        loop {
            let outcome = self
                .kv
                .scan(
                    G0_STORE,
                    G0_GROUP,
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
                let path = std::str::from_utf8(k).map_err(|e| Error::SysdataDecode {
                    key: prefix.clone(),
                    reason: e.to_string(),
                })?;
                let val: ChunkdbRangeBindingValue =
                    serde_json::from_slice(v).map_err(|e| Error::SysdataDecode {
                        key: path.to_string(),
                        reason: e.to_string(),
                    })?;
                new_bindings.push(ChunkdbRangeBinding::from_proto(&val));
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
        // Sort by sub_range_index — the canonical key, matching `replace`.
        new_bindings.sort_by_key(|b| b.sub_range_index);
        *self.bindings.write() = new_bindings;
        Ok(())
    }

    /// Route a chunk ID to its owning chunkdb instance. If the cache
    /// is empty, triggers a synchronous `refresh()` first.
    pub async fn route(
        &self,
        chunk_id: &ChunkId,
    ) -> std::result::Result<ChunkdbRangeBinding, RangeRouteError> {
        if self.bindings.read().is_empty() {
            self.refresh()
                .await
                .map_err(|e| RangeRouteError::Refresh(e.to_string()))?;
        }
        let bucket = ChunkIdParts::from_proto(chunk_id).hash_to_bucket();
        self.route_bucket(bucket)
    }

    /// Refresh the binding cache from group-0, then route the chunk
    /// ID to its owning instance. Used by clients on `NotMyRange` —
    /// the server only signals "not my range" (it does not carry the
    /// owning instance endpoint), so the client refreshes + re-routes
    /// in one call.
    pub async fn refresh_and_route(
        &self,
        chunk_id: &ChunkId,
    ) -> std::result::Result<ChunkdbRangeBinding, RangeRouteError> {
        self.refresh()
            .await
            .map_err(|e| RangeRouteError::Refresh(e.to_string()))?;
        let bucket = ChunkIdParts::from_proto(chunk_id).hash_to_bucket();
        self.route_bucket(bucket)
    }

    /// Route a bucket directly to its owning chunkdb instance.
    /// Linear scan over the cached binding table — with the default
    /// 1024 sub-ranges this is cheap; a denser layout could switch to
    /// binary search on `range_start` later if needed.
    pub fn route_bucket(&self, bucket: u16) -> std::result::Result<ChunkdbRangeBinding, RangeRouteError> {
        let bindings = self.bindings.read();
        if bindings.is_empty() {
            return Err(RangeRouteError::NoBinding);
        }
        for b in bindings.iter() {
            if b.contains(bucket) {
                return Ok(b.clone());
            }
        }
        Err(RangeRouteError::BucketUnbound { bucket })
    }

    /// Route a bucket and return both the current owner and the
    /// original owner (if in transition). The caller should try the
    /// current owner first; on `NotMyRange` or connection error, fall
    /// back to the original owner.
    pub fn route_with_fallback(
        &self,
        bucket: u16,
    ) -> std::result::Result<RouteWithFallback, RangeRouteError> {
        let binding = self.route_bucket(bucket)?;
        if binding.status == RangeStatus::InTransition && binding.original_instance_id != 0 {
            Ok(RouteWithFallback {
                primary: binding.clone(),
                fallback: Some(ChunkdbRangeBinding {
                    instance_id: binding.original_instance_id,
                    rpc_endpoint: binding.original_endpoint.clone(),
                    ..binding.clone()
                }),
            })
        } else {
            Ok(RouteWithFallback {
                primary: binding,
                fallback: None,
            })
        }
    }

    /// Check if the binding cache is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.bindings.read().is_empty()
    }

    /// Get a snapshot of the cached binding table.
    #[must_use]
    pub fn snapshot(&self) -> Vec<ChunkdbRangeBinding> {
        self.bindings.read().clone()
    }

    /// Replace the cached binding table (for watch/notify updates or
    /// test injection). Sorts by `sub_range_index` for deterministic
    /// routing.
    pub fn replace(&self, bindings: Vec<ChunkdbRangeBinding>) {
        let mut sorted = bindings;
        sorted.sort_by_key(|b| b.sub_range_index);
        *self.bindings.write() = sorted;
    }

    /// Spawn a watch/notify subscriber that refreshes the binding
    /// cache whenever `/chunkdb/range_bind/` entries change in group
    /// 0. Returns a `JoinHandle` for the notifier task; dropping it
    /// does not stop the notifier — abort the handle to stop it, or
    /// drop the returned `WatchSubscription` (kept alive inside the
    /// task) to close the stream.
    ///
    /// The notifier is optional — the client works with periodic
    /// `refresh()` alone. This is a safety-net accelerator: missed
    /// notifies are caught by the caller's periodic refresh.
    pub fn spawn_notifier(&self) -> Result<tokio::task::JoinHandle<()>> {
        let wn = WatchNotifyClient::from_shared(Arc::clone(&self.kv));
        let prefix = ChunkdbRangeBindingKey::text_prefix_all();
        let mut sub = wn.subscribe(G0_STORE, G0_GROUP, prefix.as_bytes())?;
        let bindings = Arc::clone(&self.bindings);
        let kv = Arc::clone(&self.kv);
        let handle = tokio::spawn(async move {
            while let Some(_frame) = sub.notify_rx.recv().await {
                // Re-scan the binding table on any notify in the prefix.
                let client = RangeBindingClient {
                    kv: Arc::clone(&kv),
                    bindings: Arc::clone(&bindings),
                };
                if let Err(e) = client.refresh().await {
                    warn!(error = %e, "range binding notify refresh failed");
                }
            }
            // sub drops here, closing the watch stream.
        });
        Ok(handle)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn binding(start: u16, end: u16, instance: u64, endpoint: &str) -> ChunkdbRangeBinding {
        ChunkdbRangeBinding {
            sub_range_index: 0,
            range_start: start,
            range_end: end,
            instance_id: instance,
            rpc_endpoint: endpoint.to_string(),
            original_instance_id: 0,
            original_endpoint: String::new(),
            status: RangeStatus::Stable,
            last_change_time_ms: 0,
        }
    }

    #[test]
    fn route_bucket_returns_correct_instance() {
        let client = RangeBindingClient {
            kv: Arc::new(CrowdbKvClient::new(crate::ClientConfig::new(vec![
                "http://127.0.0.1:1".into(),
            ]))),
            bindings: Arc::new(RwLock::new(vec![
                binding(0, 32_767, 1, "http://a:1"),
                binding(32_768, u16::MAX, 2, "http://b:1"),
            ])),
        };
        let r = client.route_bucket(20_000).unwrap();
        assert_eq!(r.instance_id, 1);
        assert_eq!(r.rpc_endpoint, "http://a:1");
        let r = client.route_bucket(40_000).unwrap();
        assert_eq!(r.instance_id, 2);
        assert_eq!(r.rpc_endpoint, "http://b:1");
    }

    #[test]
    fn route_bucket_empty_returns_no_binding() {
        let client = RangeBindingClient {
            kv: Arc::new(CrowdbKvClient::new(crate::ClientConfig::new(vec![
                "http://127.0.0.1:1".into(),
            ]))),
            bindings: Arc::new(RwLock::new(Vec::new())),
        };
        assert!(matches!(client.route_bucket(0), Err(RangeRouteError::NoBinding)));
    }

    #[test]
    fn route_bucket_unbound_returns_bucket_unbound() {
        let client = RangeBindingClient {
            kv: Arc::new(CrowdbKvClient::new(crate::ClientConfig::new(vec![
                "http://127.0.0.1:1".into(),
            ]))),
            bindings: Arc::new(RwLock::new(vec![binding(0, 1000, 1, "http://a:1")])),
        };
        // Bucket 5000 is outside [0, 1000).
        assert!(matches!(
            client.route_bucket(5000),
            Err(RangeRouteError::BucketUnbound { bucket: 5000 })
        ));
    }

    #[test]
    fn replace_sorts_by_sub_range_index() {
        let client = RangeBindingClient {
            kv: Arc::new(CrowdbKvClient::new(crate::ClientConfig::new(vec![
                "http://127.0.0.1:1".into(),
            ]))),
            bindings: Arc::new(RwLock::new(Vec::new())),
        };
        let mut b1 = binding(32_768, u16::MAX, 2, "http://b:1");
        b1.sub_range_index = 1;
        let mut b0 = binding(0, 32_767, 1, "http://a:1");
        b0.sub_range_index = 0;
        client.replace(vec![b1, b0]);
        let snap = client.snapshot();
        assert_eq!(snap.len(), 2);
        assert_eq!(snap[0].sub_range_index, 0);
        assert_eq!(snap[1].sub_range_index, 1);
    }

    #[test]
    fn is_empty_true_then_false_after_replace() {
        let client = RangeBindingClient {
            kv: Arc::new(CrowdbKvClient::new(crate::ClientConfig::new(vec![
                "http://127.0.0.1:1".into(),
            ]))),
            bindings: Arc::new(RwLock::new(Vec::new())),
        };
        assert!(client.is_empty());
        client.replace(vec![binding(0, u16::MAX, 1, "http://a:1")]);
        assert!(!client.is_empty());
    }

    #[test]
    fn contains_checks_range_correctly() {
        let b = binding(100, 200, 1, "http://a:1");
        assert!(b.contains(100));
        assert!(b.contains(200));
        assert!(!b.contains(201));
        assert!(!b.contains(99));
    }

    fn binding_in_transition(
        start: u16,
        end: u16,
        instance: u64,
        endpoint: &str,
        orig_instance: u64,
        orig_endpoint: &str,
    ) -> ChunkdbRangeBinding {
        ChunkdbRangeBinding {
            sub_range_index: 0,
            range_start: start,
            range_end: end,
            instance_id: instance,
            rpc_endpoint: endpoint.to_string(),
            original_instance_id: orig_instance,
            original_endpoint: orig_endpoint.to_string(),
            status: RangeStatus::InTransition,
            last_change_time_ms: 0,
        }
    }

    #[test]
    fn route_with_fallback_stable_returns_no_fallback() {
        let client = RangeBindingClient {
            kv: Arc::new(CrowdbKvClient::new(crate::ClientConfig::new(vec![
                "http://127.0.0.1:1".into(),
            ]))),
            bindings: Arc::new(RwLock::new(vec![binding(0, 1000, 1, "http://a:1")])),
        };
        let r = client.route_with_fallback(500).unwrap();
        assert_eq!(r.primary.instance_id, 1);
        assert!(r.fallback.is_none());
    }

    #[test]
    fn route_with_fallback_in_transition_returns_fallback() {
        let client = RangeBindingClient {
            kv: Arc::new(CrowdbKvClient::new(crate::ClientConfig::new(vec![
                "http://127.0.0.1:1".into(),
            ]))),
            bindings: Arc::new(RwLock::new(vec![binding_in_transition(
                0,
                1000,
                2,
                "http://b:1",
                1,
                "http://a:1",
            )])),
        };
        let r = client.route_with_fallback(500).unwrap();
        assert_eq!(r.primary.instance_id, 2);
        assert_eq!(r.primary.rpc_endpoint, "http://b:1");
        assert!(r.fallback.is_some());
        assert_eq!(r.fallback.unwrap().instance_id, 1);
    }

    #[test]
    fn route_with_fallback_in_transition_no_original_returns_no_fallback() {
        let client = RangeBindingClient {
            kv: Arc::new(CrowdbKvClient::new(crate::ClientConfig::new(vec![
                "http://127.0.0.1:1".into(),
            ]))),
            bindings: Arc::new(RwLock::new(vec![{
                let mut b = binding(0, 1000, 1, "http://a:1");
                b.status = RangeStatus::InTransition;
                b
            }])),
        };
        let r = client.route_with_fallback(500).unwrap();
        assert_eq!(r.primary.instance_id, 1);
        assert!(r.fallback.is_none());
    }
}
