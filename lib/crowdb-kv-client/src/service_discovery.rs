// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#![allow(clippy::missing_errors_doc, clippy::missing_panics_doc)]

//! [`ServiceDiscoveryClient`]: group-0 service discovery with caching.
//!
//! Wraps [`ServiceRegistryClient`] with a per-service `DashMap` cache
//! and TTL-based refresh. Clients call `discover_all` / `discover_one`
//! to find living service instances by service name without hardcoding
//! addresses. The cache is poll-on-demand: the first call queries
//! group-0, subsequent calls within the TTL return the cached result.
//!
//! See `doc/design/kv/design-crowdb-kv-group0.md` §4.4.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use dashmap::DashMap;

use crowdb_protocol::common::InstanceValue;
use crowdb_protocol::common_type::InstanceId;

use crate::error::{Error, Result};
use crate::{CrowdbKvClient, ServiceRegistryClient};

/// Default cache TTL: 5 seconds. Matches the service heartbeat
/// interval — the discovery client sees new registrations within one
/// heartbeat cycle.
const DEFAULT_CACHE_TTL_MS: u64 = 5_000;

#[allow(clippy::cast_possible_truncation)]
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

#[allow(clippy::cast_possible_truncation)]
fn duration_to_ms(ttl: Duration) -> u64 {
    ttl.as_millis() as u64
}

/// Cached discovery result for a single service.
struct CachedEntry {
    instances: Vec<(InstanceId, InstanceValue)>,
    refreshed_at_ms: u64,
}

/// Client for discovering living service instances via the group-0
/// service registry. Caches results per service with a configurable
/// TTL. Round-robin selection for `discover_one`.
///
/// All reads target store 0, group 0 (group-0 sysdata). The wrapped
/// `ServiceRegistryClient` shares the same `Arc<CrowdbKvClient>` as
/// the rest of the application, so topology re-seeding and connection
/// pooling are shared.
#[derive(Clone)]
pub struct ServiceDiscoveryClient {
    svc: ServiceRegistryClient,
    /// `service_name -> CachedEntry`.
    cache: Arc<DashMap<String, CachedEntry>>,
    /// Cache TTL in milliseconds.
    cache_ttl_ms: Arc<AtomicU64>,
    /// Round-robin cursor per service: `service_name -> next_index`.
    rr_cursor: Arc<DashMap<String, usize>>,
}

impl ServiceDiscoveryClient {
    /// Wrap a `ServiceRegistryClient` for cached discovery.
    #[must_use]
    pub fn new(svc: ServiceRegistryClient) -> Self {
        Self {
            svc,
            cache: Arc::new(DashMap::new()),
            cache_ttl_ms: Arc::new(AtomicU64::new(DEFAULT_CACHE_TTL_MS)),
            rr_cursor: Arc::new(DashMap::new()),
        }
    }

    /// Construct from a shared `CrowdbKvClient`. The discovery client
    /// wraps a new `ServiceRegistryClient` that shares the same
    /// underlying KV client + connection pool.
    #[must_use]
    pub fn from_shared_kv(kv: Arc<CrowdbKvClient>) -> Self {
        Self::new(ServiceRegistryClient::from_shared(kv))
    }

    /// Set a custom cache TTL. Default is 5 seconds. A TTL of 0
    /// disables caching (every call queries group-0).
    #[must_use]
    pub fn with_cache_ttl(self, ttl: Duration) -> Self {
        self.cache_ttl_ms.store(duration_to_ms(ttl), Ordering::Relaxed);
        self
    }

    /// Discover all living instances of `service`. Returns from cache
    /// if fresh; otherwise queries group-0 and updates the cache.
    ///
    /// On group-0 unreachable, the cache is **not** invalidated — a
    /// stale cache is returned if available, otherwise the error
    /// propagates.
    pub async fn discover_all(&self, service: &str) -> Result<Vec<(InstanceId, InstanceValue)>> {
        let ttl_ms = self.cache_ttl_ms.load(Ordering::Relaxed);
        let now = now_ms();

        // Fast path: cache hit within TTL.
        if let Some(entry) = self.cache.get(service) {
            if now.saturating_sub(entry.refreshed_at_ms) < ttl_ms {
                return Ok(entry.instances.clone());
            }
        }

        // Slow path: query group-0.
        let instances = match self.svc.read_all_instances(service).await {
            Ok(v) => v,
            Err(e) => {
                // On failure, return stale cache if available.
                if let Some(entry) = self.cache.get(service) {
                    return Ok(entry.instances.clone());
                }
                return Err(Error::DiscoveryUnreachable {
                    service: service.to_string(),
                    source: Box::new(e),
                });
            }
        };

        // Update cache.
        self.cache.insert(
            service.to_string(),
            CachedEntry {
                instances: instances.clone(),
                refreshed_at_ms: now,
            },
        );

        Ok(instances)
    }

    /// Discover one living instance of `service` (round-robin among
    /// the cached/refreshed set). Returns `Error::NoLivingInstances`
    /// if the registry has zero live entries for the service.
    pub async fn discover_one(&self, service: &str) -> Result<InstanceValue> {
        let instances = self.discover_all(service).await?;
        if instances.is_empty() {
            return Err(Error::NoLivingInstances {
                service: service.to_string(),
            });
        }

        // Round-robin: atomically increment the cursor and pick the
        // instance at `cursor % len`.
        let idx = {
            let mut cursor = self.rr_cursor.entry(service.to_string()).or_insert(0);
            let i = *cursor % instances.len();
            *cursor = (*cursor + 1) % instances.len().max(1);
            i
        };

        Ok(instances[idx].1.clone())
    }

    /// Discover the instance whose `rpc_endpoint` matches `endpoint`.
    /// Returns `Ok(None)` if no living instance has that endpoint.
    /// Used by callers that have an explicit override address and want
    /// to verify liveness — if the endpoint is not in the registry,
    /// the caller may still connect directly (override semantics).
    pub async fn discover_by_endpoint(&self, service: &str, endpoint: &str) -> Result<Option<InstanceValue>> {
        let instances = self.discover_all(service).await?;
        Ok(instances
            .into_iter()
            .find(|(_, v)| v.rpc_endpoint == endpoint)
            .map(|(_, v)| v))
    }

    /// Invalidate the cache for `service` (or all services if
    /// `None`). The next `discover_*` call re-queries group-0.
    /// Called after a known topology change (e.g. `cluster_init`,
    /// `deploy_diskdb`) so the next read picks up new registrations
    /// immediately.
    pub fn invalidate(&self, service: Option<&str>) {
        match service {
            Some(s) => {
                self.cache.remove(s);
            }
            None => {
                self.cache.clear();
            }
        }
    }

    /// Access the underlying `ServiceRegistryClient` for direct
    /// registry operations (register, heartbeat, unregister).
    #[must_use]
    pub fn registry(&self) -> &ServiceRegistryClient {
        &self.svc
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ClientConfig;

    #[test]
    fn default_cache_ttl_is_5s() {
        let client = ServiceDiscoveryClient::new(ServiceRegistryClient::new(CrowdbKvClient::new(
            ClientConfig::new(vec!["http://127.0.0.1:10000".into()]),
        )));
        assert_eq!(client.cache_ttl_ms.load(Ordering::Relaxed), 5_000);
    }

    #[test]
    fn with_cache_ttl_sets_ttl() {
        let client = ServiceDiscoveryClient::new(ServiceRegistryClient::new(CrowdbKvClient::new(
            ClientConfig::new(vec!["http://127.0.0.1:10000".into()]),
        )))
        .with_cache_ttl(Duration::from_millis(500));
        assert_eq!(client.cache_ttl_ms.load(Ordering::Relaxed), 500);
    }

    #[test]
    fn invalidate_removes_single_service() {
        let client = ServiceDiscoveryClient::new(ServiceRegistryClient::new(CrowdbKvClient::new(
            ClientConfig::new(vec!["http://127.0.0.1:10000".into()]),
        )));
        client.cache.insert(
            "diskdb".into(),
            CachedEntry {
                instances: vec![],
                refreshed_at_ms: 0,
            },
        );
        client.cache.insert(
            "chunkdb".into(),
            CachedEntry {
                instances: vec![],
                refreshed_at_ms: 0,
            },
        );
        client.invalidate(Some("diskdb"));
        assert!(client.cache.get("diskdb").is_none());
        assert!(client.cache.get("chunkdb").is_some());
    }

    #[test]
    fn invalidate_all_clears_cache() {
        let client = ServiceDiscoveryClient::new(ServiceRegistryClient::new(CrowdbKvClient::new(
            ClientConfig::new(vec!["http://127.0.0.1:10000".into()]),
        )));
        client.cache.insert(
            "diskdb".into(),
            CachedEntry {
                instances: vec![],
                refreshed_at_ms: 0,
            },
        );
        client.invalidate(None);
        assert!(client.cache.is_empty());
    }
}
