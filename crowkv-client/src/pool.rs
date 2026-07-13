//! Per-endpoint `tonic::Channel` pool. A single HTTP/2 channel already
//! multiplexes concurrent requests, so `pool_size == 1` (the default) is
//! sufficient for most workloads; the pool exists so a deployment that
//! profiles a single-channel bottleneck can raise it without any API change
//! (`doc/plan-client.md` §6 Issue 4).

use std::sync::atomic::{AtomicUsize, Ordering};

use dashmap::DashMap;
use tonic::transport::Channel;

use crate::error::{Error, Result};

struct EndpointPool {
    channels: Vec<Channel>,
    next: AtomicUsize,
}

pub struct ConnectionPool {
    pool_size: usize,
    pools: DashMap<String, EndpointPool>,
}

impl ConnectionPool {
    #[must_use]
    pub fn new(pool_size: usize) -> Self {
        Self {
            pool_size: pool_size.max(1),
            pools: DashMap::new(),
        }
    }

    /// Round-robin channel for `endpoint`. Channels are created lazily
    /// (`Channel::connect_lazy`, no I/O) on first use of that endpoint, so
    /// this never blocks on a network round-trip; connection failures
    /// surface on the first RPC call instead, where the retry loop already
    /// handles them.
    pub fn get(&self, endpoint: &str) -> Result<Channel> {
        let url = normalize(endpoint);
        if let Some(pool) = self.pools.get(&url) {
            let idx = pool.next.fetch_add(1, Ordering::Relaxed) % pool.channels.len();
            return Ok(pool.channels[idx].clone());
        }
        let mut channels = Vec::with_capacity(self.pool_size);
        for _ in 0..self.pool_size {
            let channel = Channel::from_shared(url.clone())
                .map_err(|e| Error::InvalidEndpoint {
                    endpoint: endpoint.to_string(),
                    reason: e.to_string(),
                })?
                .connect_lazy();
            channels.push(channel);
        }
        let chosen = channels[0].clone();
        self.pools.insert(
            url,
            EndpointPool {
                channels,
                next: AtomicUsize::new(1),
            },
        );
        Ok(chosen)
    }

    /// Drop any pooled channels for `endpoint`, forcing a fresh lazy
    /// connection on the next `get`. Useful after learning an endpoint is
    /// stale (e.g. the process behind it restarted on a new port). Not yet
    /// called internally (lazy channels degrade gracefully on their own);
    /// kept as public API for callers with sharper staleness signals.
    #[allow(dead_code)]
    pub fn invalidate(&self, endpoint: &str) {
        self.pools.remove(&normalize(endpoint));
    }
}

fn normalize(endpoint: &str) -> String {
    if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        endpoint.to_string()
    } else {
        format!("http://{endpoint}")
    }
}
