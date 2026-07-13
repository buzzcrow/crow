//! Topology cache: `(store_id, group_id) -> leader_endpoint`, sourced from
//! `crowkv-server`'s HTTP management API (`GET /topology`). There is no gRPC
//! `DescribeCluster` RPC — this is the only discovery mechanism.

use std::sync::RwLock;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use serde::Deserialize;
use tokio::sync::Mutex as AsyncMutex;

use crowkv::cluster::status::StoreStatus;

use crate::error::{Error, Result};

/// Wire shape of `GET /topology`, matching
/// `crowkv-server/src/mgmt_api.rs::TopologyResponse`.
#[derive(Deserialize)]
struct TopologyResponse {
    stores: Vec<StoreStatus>,
}

pub struct TopologyCache {
    seeds: RwLock<Vec<String>>,
    http: reqwest::Client,
    leaders: DashMap<(u64, u64), String>,
    min_refresh_interval: Duration,
    /// Single-flight guard: while held, a fetch is either in flight or was
    /// just completed within `min_refresh_interval`. Concurrent `refresh`
    /// callers queue on this lock rather than each issuing their own HTTP
    /// request (: "not a storm").
    refresh_gate: AsyncMutex<Instant>,
}

impl TopologyCache {
    #[must_use]
    pub fn new(seeds: Vec<String>, min_refresh_interval: Duration) -> Self {
        Self {
            seeds: RwLock::new(seeds),
            http: reqwest::Client::new(),
            leaders: DashMap::new(),
            min_refresh_interval,
            // Far enough in the past that the first `refresh` always fetches.
            refresh_gate: AsyncMutex::new(
                Instant::now()
                    .checked_sub(Duration::from_secs(3600))
                    .unwrap_or_else(Instant::now),
            ),
        }
    }

    /// Cached leader endpoint for a group, if known. Never performs I/O.
    #[must_use]
    pub fn leader(&self, store_id: u64, group_id: u64) -> Option<String> {
        self.leaders.get(&(store_id, group_id)).map(|v| v.clone())
    }

    /// Directly seed the cache with a leader endpoint learned from a
    /// `NotLeaderHint` on a KV response. Cheaper and more precise than a
    /// full `/topology` refresh since the hint is already the answer.
    pub fn set_leader(&self, store_id: u64, group_id: u64, endpoint: String) {
        self.leaders.insert((store_id, group_id), endpoint);
    }

    /// Replace the seed list used for future `/topology` fetches. Lets a
    /// long-lived cache track a growing/changing set of nodes (e.g.
    /// `crowkv-console` adding a server at runtime) without rebuilding the
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
        for store in body.stores {
            for group in store.groups {
                let leader_id = group.leader_id;
                let endpoint = if group.local_replica.id == leader_id {
                    store.listen_addr.clone()
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
            }
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
}
