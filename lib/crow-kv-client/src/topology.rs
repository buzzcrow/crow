// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Topology cache: `(store_id, group_id) -> leader_endpoint`, sourced from
//! `crow-kv-server`'s HTTP management API (`GET /topology`). There is no gRPC
//! `DescribeCluster` RPC — this is the only discovery mechanism.

use std::collections::HashSet;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use tokio::sync::Mutex as AsyncMutex;

use crow_protocol::mgmt::TopologyResponse;

use crate::error::{Error, Result};

/// Eviction hook: called with the set of `(store_id, group_id)` keys
/// that disappeared from the fresh `/topology` body. Used by
/// `CrowkvClient` to evict stale `write_slot_highwater` entries.
pub type EvictionHook = Arc<dyn Fn(&HashSet<(u64, u64)>) + Send + Sync>;

pub struct TopologyCache {
    seeds: RwLock<Vec<String>>,
    http: reqwest::Client,
    leaders: DashMap<(u64, u64), String>,
    /// Per-`(store_id, group_id)` full replica endpoint list (local +
    /// remotes), populated from the same `/topology` fetch as `leaders`.
    /// Used by the `AnyReplica` read-endpoint selector; `Leader` policy
    /// never reads it. Refreshed only by `refresh()` — `set_leader` (the
    /// `NotLeaderHint` fast path) does not touch it, since a hint only
    /// carries the leader endpoint.
    replicas: DashMap<(u64, u64), Vec<String>>,
    min_refresh_interval: Duration,
    /// Single-flight guard: while held, a fetch is either in flight or was
    /// just completed within `min_refresh_interval`. Concurrent `refresh`
    /// callers queue on this lock rather than each issuing their own HTTP
    /// request (: "not a storm").
    refresh_gate: AsyncMutex<Instant>,
    /// Optional eviction hook called with groups that disappeared from
    /// a fresh `/topology` body. Set by `CrowkvClient::new` to evict
    /// stale `write_slot_highwater` entries.
    eviction_hook: Option<EvictionHook>,
}

impl TopologyCache {
    #[must_use]
    #[cfg(test)]
    pub fn new(seeds: Vec<String>, min_refresh_interval: Duration) -> Self {
        Self::with_eviction_hook(seeds, min_refresh_interval, None)
    }

    #[must_use]
    pub fn with_eviction_hook(
        seeds: Vec<String>,
        min_refresh_interval: Duration,
        eviction_hook: Option<EvictionHook>,
    ) -> Self {
        Self {
            seeds: RwLock::new(seeds),
            http: reqwest::Client::new(),
            leaders: DashMap::new(),
            replicas: DashMap::new(),
            min_refresh_interval,
            // Far enough in the past that the first `refresh` always fetches.
            refresh_gate: AsyncMutex::new(
                Instant::now()
                    .checked_sub(Duration::from_secs(3600))
                    .unwrap_or_else(Instant::now),
            ),
            eviction_hook,
        }
    }

    /// Cached leader endpoint for a group, if known. Never performs I/O.
    #[must_use]
    pub fn leader(&self, store_id: u64, group_id: u64) -> Option<String> {
        self.leaders.get(&(store_id, group_id)).map(|v| v.clone())
    }

    /// Test-only: get the current seed list.
    #[cfg(test)]
    pub fn seeds_for_test(&self) -> Vec<String> {
        self.seeds.read().unwrap().clone()
    }

    /// Test-only: replace the seed list.
    #[cfg(test)]
    pub fn set_seeds_for_test(&self, seeds: Vec<String>) {
        *self.seeds.write().unwrap() = seeds;
    }

    /// Cached full replica endpoint list for a group (local + remotes),
    /// if known. Never performs I/O. Used by the `AnyReplica`
    /// read-endpoint selector; returns `None` until the first
    /// `refresh()` lands a `/topology` body that includes this group.
    #[must_use]
    pub fn replicas(&self, store_id: u64, group_id: u64) -> Option<Vec<String>> {
        self.replicas.get(&(store_id, group_id)).map(|v| v.clone())
    }

    /// Directly seed the cache with a leader endpoint learned from a
    /// `NotLeaderHint` on a KV response. Cheaper and more precise than a
    /// full `/topology` refresh since the hint is already the answer.
    pub fn set_leader(&self, store_id: u64, group_id: u64, endpoint: String) {
        self.leaders.insert((store_id, group_id), endpoint);
    }

    /// Replace the seed list used for future `/topology` fetches. Lets a
    /// long-lived cache track a growing/changing set of nodes (e.g.
    /// `crow-console` adding a server at runtime) without rebuilding the
    /// cache or losing already-learned leader endpoints.
    ///
    /// # Panics
    /// Panics if the internal lock is poisoned.
    pub fn set_seeds(&self, seeds: Vec<String>) {
        *self.seeds.write().unwrap() = seeds;
    }

    /// Refresh the cache from `/topology` on the first reachable seed.
    /// Coalesces concurrent callers into a single HTTP fetch when they land
    /// within `min_refresh_interval` of each other.
    ///
    /// # Errors
    /// Returns `Error::Topology` only if every seed is unreachable.
    pub async fn refresh(&self) -> Result<()> {
        let mut last = self.refresh_gate.lock().await;
        if last.elapsed() < self.min_refresh_interval {
            return Ok(());
        }
        let result = self.fetch_and_merge().await;
        *last = Instant::now();
        result
    }

    async fn fetch_and_merge(&self) -> Result<()> {
        let seeds = self.seeds.read().unwrap().clone();
        let mut last_err = None;
        for seed in &seeds {
            let url = format!("{}/topology", seed.trim_end_matches('/'));
            match self.http.get(&url).send().await {
                Ok(resp) => match resp.json::<TopologyResponse>().await {
                    Ok(body) => {
                        self.merge(body);
                        return Ok(());
                    }
                    Err(e) => last_err = Some(format!("{seed}: decode error: {e}")),
                },
                Err(e) => last_err = Some(format!("{seed}: request error: {e}")),
            }
        }
        Err(Error::Topology(
            last_err.unwrap_or_else(|| "no seeds configured".to_string()),
        ))
    }

    fn merge(&self, body: TopologyResponse) {
        // Collect the set of (store_id, group_id) present in the fresh
        // body so we can evict stale entries after the insert loop.
        let mut fresh_keys: HashSet<(u64, u64)> = HashSet::new();
        for store in &body.stores {
            for group in &store.groups {
                fresh_keys.insert((store.store_id, group.group_id));
            }
        }

        for store in body.stores {
            // `listen_addr` is the local replica's gRPC endpoint; it is
            // `None` only for a server that hasn't bound its listener yet,
            // in which case this store contributes no endpoints.
            let local_endpoint = store.listen_addr.clone();
            for group in store.groups {
                let leader_id = group.leader_id;
                let endpoint = if group.local_replica.id == leader_id {
                    local_endpoint.clone()
                } else {
                    group
                        .remotes
                        .iter()
                        .find(|r| r.id == leader_id)
                        .map(|r| r.endpoint.clone())
                };
                if let Some(endpoint) = endpoint {
                    self.leaders.insert((store.store_id, group.group_id), endpoint);
                }

                // Full replica endpoint list for the `AnyReplica`
                // read-endpoint selector: the local replica (via
                // `listen_addr`) plus every remote's `endpoint`. Skip the
                // local entry when `listen_addr` is `None` (server not
                // bound yet) — a partial list would mis-route reads.
                let mut replicas: Vec<String> = Vec::with_capacity(group.remotes.len() + 1);
                if let Some(addr) = &local_endpoint {
                    replicas.push(addr.clone());
                }
                for r in &group.remotes {
                    replicas.push(r.endpoint.clone());
                }
                if !replicas.is_empty() {
                    self.replicas.insert((store.store_id, group.group_id), replicas);
                }
            }
        }

        // Evict stale entries: groups present in the cache but absent
        // from the fresh body. A removed group's stale leader endpoint
        // self-heals via `NotLeaderHint`, but evicting keeps the cache
        // clean. The eviction hook lets `CrowkvClient` evict stale
        // `write_slot_highwater` entries — a stale `min_slot`
        // high-watermark does NOT self-heal (silent empty reads forever).
        let evicted: Vec<(u64, u64)> = self
            .leaders
            .iter()
            .filter_map(|e| {
                if fresh_keys.contains(e.key()) {
                    None
                } else {
                    Some(*e.key())
                }
            })
            .collect();
        if evicted.is_empty() {
            return;
        }
        let evicted_set: HashSet<(u64, u64)> = evicted.iter().copied().collect();
        for key in &evicted {
            self.leaders.remove(key);
            self.replicas.remove(key);
        }
        if let Some(hook) = &self.eviction_hook {
            hook(&evicted_set);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use axum::extract::State;
    use axum::routing::get;
    use axum::{Json, Router};

    use super::*;

    /// Spawns a minimal axum server serving `/topology` from a fixed body,
    /// counting hits via `counter`. Returns the bound `http://host:port`.
    async fn spawn_topology_server(body: serde_json::Value, counter: Arc<AtomicUsize>) -> String {
        async fn handler(
            State((body, counter)): State<(serde_json::Value, Arc<AtomicUsize>)>,
        ) -> Json<serde_json::Value> {
            counter.fetch_add(1, Ordering::SeqCst);
            Json(body)
        }
        let app = Router::new()
            .route("/topology", get(handler))
            .with_state((body, counter));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{addr}")
    }

    fn sample_topology(store_id: u64, group_id: u64, leader_endpoint: &str) -> serde_json::Value {
        serde_json::json!({
            "stores": [{
                "store_id": store_id,
                "listen_addr": leader_endpoint,
                "status": "ok",
                "groups": [{
                    "group_id": group_id,
                    "leader_id": 1,
                    "local_replica_id": 1,
                    "force_classic": false,
                    "status": "ok",
                    "local_replica": {
                        "id": 1,
                        "role": "leader",
                        "voting": true,
                        "status": "ok",
                        "kv_store": { "key_count": 0, "engine_healthy": true }
                    },
                    "remotes": []
                }]
            }]
        })
    }

    #[tokio::test]
    async fn refresh_populates_leader_from_local_replica() {
        let counter = Arc::new(AtomicUsize::new(0));
        let seed = spawn_topology_server(sample_topology(7, 42, "http://10.0.0.1:9001"), counter).await;
        let cache = TopologyCache::new(vec![seed], Duration::from_millis(50));

        cache.refresh().await.unwrap();

        assert_eq!(cache.leader(7, 42), Some("http://10.0.0.1:9001".to_string()));
    }

    #[tokio::test]
    async fn concurrent_refresh_coalesces_into_one_fetch() {
        let counter = Arc::new(AtomicUsize::new(0));
        let seed = spawn_topology_server(sample_topology(1, 1, "http://x:1"), counter.clone()).await;
        let cache = Arc::new(TopologyCache::new(vec![seed], Duration::from_secs(5)));

        let mut handles = Vec::new();
        for _ in 0..8 {
            let cache = cache.clone();
            handles.push(tokio::spawn(async move { cache.refresh().await }));
        }
        for h in handles {
            h.await.unwrap().unwrap();
        }

        // All 8 concurrent refreshes land within the 5s coalescing window,
        // so exactly one HTTP fetch should have happened.
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn set_leader_overrides_cache_without_io() {
        let cache = TopologyCache::new(vec!["http://unused:1".to_string()], Duration::from_secs(60));
        cache.set_leader(3, 9, "http://leader:8080".to_string());
        assert_eq!(cache.leader(3, 9), Some("http://leader:8080".to_string()));
    }

    /// Build a topology body with multiple groups in one store.
    fn multi_group_topology(store_id: u64, group_ids: &[u64], leader_endpoint: &str) -> serde_json::Value {
        let groups: Vec<serde_json::Value> = group_ids
            .iter()
            .map(|gid| {
                serde_json::json!({
                    "group_id": gid,
                    "leader_id": 1,
                    "local_replica_id": 1,
                    "force_classic": false,
                    "status": "ok",
                    "local_replica": {
                        "id": 1,
                        "role": "leader",
                        "voting": true,
                        "status": "ok",
                        "kv_store": { "key_count": 0, "engine_healthy": true }
                    },
                    "remotes": []
                })
            })
            .collect();
        serde_json::json!({
            "stores": [{
                "store_id": store_id,
                "listen_addr": leader_endpoint,
                "status": "ok",
                "groups": groups
            }]
        })
    }

    #[tokio::test]
    async fn merge_evicts_groups_absent_from_fresh_body() {
        let counter = Arc::new(AtomicUsize::new(0));
        // First server: 3 groups (1, 2, 3).
        let seed_full = spawn_topology_server(
            multi_group_topology(1, &[1, 2, 3], "http://10.0.0.1:9001"),
            counter.clone(),
        )
        .await;
        // Second server: 2 groups (1, 2) — group 3 is gone.
        let seed_partial =
            spawn_topology_server(multi_group_topology(1, &[1, 2], "http://10.0.0.1:9001"), counter).await;

        let cache = TopologyCache::new(vec![seed_full], Duration::from_millis(50));
        cache.refresh().await.unwrap();
        assert_eq!(cache.leader(1, 1), Some("http://10.0.0.1:9001".to_string()));
        assert_eq!(cache.leader(1, 2), Some("http://10.0.0.1:9001".to_string()));
        assert_eq!(cache.leader(1, 3), Some("http://10.0.0.1:9001".to_string()));

        // Swap the seed to the partial server and wait past the refresh
        // interval so the next refresh fetches from it.
        {
            let mut seeds = cache.seeds_for_test();
            seeds[0] = seed_partial.clone();
            cache.set_seeds_for_test(seeds);
        }
        tokio::time::sleep(Duration::from_millis(60)).await;
        cache.refresh().await.unwrap();

        // Groups 1 and 2 survive; group 3 is evicted.
        assert_eq!(cache.leader(1, 1), Some("http://10.0.0.1:9001".to_string()));
        assert_eq!(cache.leader(1, 2), Some("http://10.0.0.1:9001".to_string()));
        assert_eq!(cache.leader(1, 3), None);
    }

    #[tokio::test]
    async fn merge_eviction_hook_fires_for_evicted_groups() {
        let evicted: Arc<std::sync::Mutex<HashSet<(u64, u64)>>> =
            Arc::new(std::sync::Mutex::new(HashSet::new()));
        let evicted_clone = Arc::clone(&evicted);
        let hook: EvictionHook = Arc::new(move |keys| {
            let mut guard = evicted_clone.lock().unwrap();
            guard.extend(keys.iter().copied());
        });

        let counter = Arc::new(AtomicUsize::new(0));
        let seed_full = spawn_topology_server(
            multi_group_topology(1, &[1, 2, 3], "http://10.0.0.1:9001"),
            counter.clone(),
        )
        .await;
        let seed_partial =
            spawn_topology_server(multi_group_topology(1, &[1, 2], "http://10.0.0.1:9001"), counter).await;

        let cache = TopologyCache::with_eviction_hook(vec![seed_full], Duration::from_millis(50), Some(hook));
        cache.refresh().await.unwrap();
        assert!(evicted.lock().unwrap().is_empty());

        {
            let mut seeds = cache.seeds_for_test();
            seeds[0] = seed_partial.clone();
            cache.set_seeds_for_test(seeds);
        }
        tokio::time::sleep(Duration::from_millis(60)).await;
        cache.refresh().await.unwrap();

        let evicted_guard = evicted.lock().unwrap();
        assert_eq!(evicted_guard.len(), 1);
        assert!(evicted_guard.contains(&(1, 3)));
    }
}
