// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! [`CrowkvClient`]: the C1-C3 client library (—
//! topology cache, retry/idempotency, and `ReadMode` routing on top of
//! `crowkv`'s generated `KvService` client.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use dashmap::DashMap;

use crowkv::rpc::kv_service_client::KvServiceClient;
use crowkv::rpc::{
    KvBatchItem, KvBatchWriteRequest, KvDeleteRequest, KvGetRequest, KvResponse, KvScanRequest, KvSetRequest,
    ReadMode,
};

use crate::config::{ClientConfig, RetryConfig};
use crate::error::{Error, Result};
use crate::pool::ConnectionPool;
use crate::topology::TopologyCache;

/// Outcome of a successful `put`/`delete`/`batch_write`.
#[derive(Debug, Clone)]
pub struct WriteOutcome {
    pub revision: u64,
    pub request_id: u64,
}

/// Outcome of a successful `get`.
#[derive(Debug, Clone)]
pub enum GetOutcome {
    Found { value: Vec<u8>, revision: u64 },
    NotFound,
}

/// Outcome of a successful `scan`.
#[derive(Debug, Clone)]
pub struct ScanOutcome {
    pub items: Vec<(Vec<u8>, Vec<u8>)>,
    pub truncated: bool,
}

/// One item of a `batch_write` call.
#[derive(Debug, Clone)]
pub enum BatchOp {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

/// Standalone `CrowKV` client: topology discovery over the HTTP management
/// API, per-group leader cache, retry loop reusing `(client_id, seq)` across
/// retries of one logical write, and `ReadMode` routing including
/// `ReadYourWrites` client-side slot tracking.
pub struct CrowkvClient {
    topology: TopologyCache,
    pool: ConnectionPool,
    retry: RetryConfig,
    client_id: u64,
    next_seq: AtomicU64,
    /// Per-`(store_id, group_id)` high-watermark of the last write's
    /// `revision`, auto-attached as `client_slot` on `ReadYourWrites` reads.
    /// Bounded by the number of groups this client has written to, not by
    /// keyspace size.
    write_watermark: DashMap<(u64, u64), u64>,
}

impl CrowkvClient {
    #[must_use]
    pub fn new(config: ClientConfig) -> Self {
        Self {
            topology: TopologyCache::new(config.mgmt_seeds, config.topology_min_refresh_interval),
            pool: ConnectionPool::new(config.pool_size_per_endpoint),
            retry: config.retry,
            client_id: new_client_id(),
            next_seq: AtomicU64::new(1),
            write_watermark: DashMap::new(),
        }
    }

    /// This client session's opaque `client_id`.
    #[must_use]
    pub fn client_id(&self) -> u64 {
        self.client_id
    }

    /// Force a topology refresh. Not required for normal operation (the
    /// client refreshes on cache miss and `NotLeaderHint` automatically);
    /// exposed for callers that want to warm the cache eagerly at startup.
    ///
    /// # Errors
    /// `Error::Topology` if every seed is unreachable.
    pub async fn refresh_topology(&self) -> Result<()> {
        self.topology.refresh().await
    }

    /// Replace the HTTP management-API seed list used for topology
    /// discovery, without losing already-cached leader endpoints. For
    /// long-lived embedders (e.g. `crowkv-console`) whose set of known
    /// nodes can grow at runtime.
    pub fn set_mgmt_seeds(&self, seeds: Vec<String>) {
        self.topology.set_seeds(seeds);
    }

    /// Directly seed the topology cache with a known leader endpoint for a
    /// group, bypassing `/topology` discovery entirely. For callers that
    /// already resolved an endpoint through some other discovery path
    /// (e.g. `crowkv-console`'s own management API) and just want
    /// `CrowkvClient`'s retry/pool machinery on top of it.
    pub fn seed_leader(&self, store_id: u64, group_id: u64, endpoint: String) {
        self.topology.set_leader(store_id, group_id, endpoint);
    }

    /// Resolve the current leader endpoint for `(store_id, group_id)`,
    /// retrying an "unknown leader" outcome ("Unknown leader:
    /// 1s-then-retry") rather than failing on the first
    /// miss. A single failed/empty `/topology` fetch is not conclusive: the
    /// group may simply be mid-election (a real, common case right after a
    /// node restart) or the seed just queried may be transiently down while
    /// others are fine. Bounded by the same `RetryConfig::max_retries`
    /// budget used for post-request retries.
    async fn resolve_leader(&self, store_id: u64, group_id: u64) -> Result<String> {
        if let Some(ep) = self.topology.leader(store_id, group_id) {
            return Ok(ep);
        }
        let mut attempts = 0u32;
        loop {
            // A fetch error and a fetch that succeeded but shows no leader
            // yet (mid-election) are both just "leader unknown right now"
            // from the caller's perspective -- collapse them into the same
            // retry path instead of surfacing the transport error early.
            let _ = self.topology.refresh().await;
            if let Some(ep) = self.topology.leader(store_id, group_id) {
                return Ok(ep);
            }
            attempts += 1;
            if self.retry.single_attempt || attempts > self.retry.max_retries {
                return Err(Error::NoLeader { store_id, group_id });
            }
            tokio::time::sleep(self.retry.unknown_leader_wait).await;
        }
    }

    fn record_write(&self, store_id: u64, group_id: u64, revision: u64) {
        self.write_watermark
            .entry((store_id, group_id))
            .and_modify(|w| *w = (*w).max(revision))
            .or_insert(revision);
    }

    /// Cached `client_slot` for `ReadYourWrites` reads against this group:
    /// the highest `revision` this client has observed from its own writes,
    /// or `0` if it has never written to this group.
    #[must_use]
    pub fn read_your_writes_slot(&self, store_id: u64, group_id: u64) -> u64 {
        self.write_watermark.get(&(store_id, group_id)).map_or(0, |v| *v)
    }

    /// `Put` a single key/value.
    ///
    /// `ids`: override `(client_id, seq)` for callers that manage their own
    /// idempotency keys (e.g. `crowkv-console`'s HTTP API, which lets an
    /// external caller supply these explicitly); `None` auto-generates and
    /// reuses this client's own `client_id` plus a fresh `seq` across all
    /// retries of this call.
    ///
    /// # Errors
    /// See [`Error`]. Retries transparently; returns `Err` only once the
    /// retry budget is exhausted or discovery fails outright.
    pub async fn put(
        &self,
        store_id: u64,
        group_id: u64,
        key: &[u8],
        value: &[u8],
        ids: Option<(u64, u64)>,
    ) -> Result<WriteOutcome> {
        let (client_id, seq) =
            ids.unwrap_or_else(|| (self.client_id, self.next_seq.fetch_add(1, Ordering::Relaxed)));
        let mut endpoint = self.resolve_leader(store_id, group_id).await?;
        let mut attempts = 0u32;
        let mut backoff = self.retry.backoff_base;
        loop {
            let req = KvSetRequest {
                version: 1,
                key: key.to_vec(),
                value: value.to_vec(),
                seq,
                ttl_ms: 0,
                client_id,
                request_id: next_request_id(),
                request_create_ms: now_ms(),
                group_id,
            };
            let channel = self.pool.get(&endpoint)?;
            match KvServiceClient::new(channel).put(req).await {
                Ok(resp) => {
                    let resp = resp.into_inner();
                    if resp.ok {
                        self.record_write(store_id, group_id, resp.revision);
                        return Ok(WriteOutcome {
                            revision: resp.revision,
                            request_id: resp.request_id,
                        });
                    }
                    if let Some(new_endpoint) = self.follow_not_leader(store_id, group_id, &resp) {
                        endpoint = new_endpoint;
                        continue;
                    }
                    attempts = self.count_other(attempts, &resp.error)?;
                    if Self::is_unknown_leader(&resp.error) {
                        endpoint = self.wait_and_refresh_leader(store_id, group_id, &endpoint).await;
                    }
                }
                Err(status) => {
                    if !self.retry.single_attempt {
                        endpoint = self
                            .handle_transport_err(store_id, group_id, &endpoint, &mut backoff)
                            .await;
                    }
                    attempts = self.count_other(attempts, &status.to_string())?;
                }
            }
        }
    }

    /// `Get` a single key.
    ///
    /// # Errors
    /// See [`Error`].
    pub async fn get(
        &self,
        store_id: u64,
        group_id: u64,
        key: &[u8],
        read_mode: ReadMode,
        client_slot: Option<u64>,
    ) -> Result<GetOutcome> {
        let client_slot = self.resolve_client_slot(store_id, group_id, read_mode, client_slot);
        let mut endpoint = self.resolve_leader(store_id, group_id).await?;
        let mut attempts = 0u32;
        let mut backoff = self.retry.backoff_base;
        loop {
            let req = KvGetRequest {
                version: 1,
                key: key.to_vec(),
                request_id: next_request_id(),
                request_create_ms: now_ms(),
                group_id,
                read_mode: read_mode as i32,
                client_slot,
            };
            let channel = self.pool.get(&endpoint)?;
            match KvServiceClient::new(channel).get(req).await {
                Ok(resp) => {
                    let resp = resp.into_inner();
                    if resp.not_found {
                        return Ok(GetOutcome::NotFound);
                    }
                    if resp.ok {
                        return Ok(GetOutcome::Found {
                            value: resp.value,
                            revision: resp.revision,
                        });
                    }
                    if let Some(new_endpoint) = self.follow_not_leader(store_id, group_id, &resp) {
                        endpoint = new_endpoint;
                        continue;
                    }
                    attempts = self.count_other(attempts, &resp.error)?;
                    if Self::is_unknown_leader(&resp.error) {
                        endpoint = self.wait_and_refresh_leader(store_id, group_id, &endpoint).await;
                    }
                }
                Err(status) => {
                    if !self.retry.single_attempt {
                        endpoint = self
                            .handle_transport_err(store_id, group_id, &endpoint, &mut backoff)
                            .await;
                    }
                    attempts = self.count_other(attempts, &status.to_string())?;
                }
            }
        }
    }

    /// `Delete` a single key. `not_found` is reported as a benign
    /// `WriteOutcome { revision: 0,.. }`, matching `Put`'s idempotent-retry
    /// shape. `ids` overrides `(client_id, seq)`; see [`Self::put`].
    ///
    /// # Errors
    /// See [`Error`].
    pub async fn delete(
        &self,
        store_id: u64,
        group_id: u64,
        key: &[u8],
        ids: Option<(u64, u64)>,
    ) -> Result<WriteOutcome> {
        let (client_id, seq) =
            ids.unwrap_or_else(|| (self.client_id, self.next_seq.fetch_add(1, Ordering::Relaxed)));
        let mut endpoint = self.resolve_leader(store_id, group_id).await?;
        let mut attempts = 0u32;
        let mut backoff = self.retry.backoff_base;
        loop {
            let req = KvDeleteRequest {
                version: 1,
                key: key.to_vec(),
                seq,
                client_id,
                request_id: next_request_id(),
                request_create_ms: now_ms(),
                group_id,
            };
            let channel = self.pool.get(&endpoint)?;
            match KvServiceClient::new(channel).delete(req).await {
                Ok(resp) => {
                    let resp = resp.into_inner();
                    if resp.not_found {
                        return Ok(WriteOutcome {
                            revision: 0,
                            request_id: resp.request_id,
                        });
                    }
                    if resp.ok {
                        self.record_write(store_id, group_id, resp.revision);
                        return Ok(WriteOutcome {
                            revision: resp.revision,
                            request_id: resp.request_id,
                        });
                    }
                    if let Some(new_endpoint) = self.follow_not_leader(store_id, group_id, &resp) {
                        endpoint = new_endpoint;
                        continue;
                    }
                    attempts = self.count_other(attempts, &resp.error)?;
                    if Self::is_unknown_leader(&resp.error) {
                        endpoint = self.wait_and_refresh_leader(store_id, group_id, &endpoint).await;
                    }
                }
                Err(status) => {
                    if !self.retry.single_attempt {
                        endpoint = self
                            .handle_transport_err(store_id, group_id, &endpoint, &mut backoff)
                            .await;
                    }
                    attempts = self.count_other(attempts, &status.to_string())?;
                }
            }
        }
    }

    /// Atomically apply a batch of `Put`/`Delete` ops at one slot.
    ///
    /// # Errors
    /// See [`Error`].
    pub async fn batch_write(&self, store_id: u64, group_id: u64, ops: &[BatchOp]) -> Result<WriteOutcome> {
        let seq = self.next_seq.fetch_add(1, Ordering::Relaxed);
        let items: Vec<KvBatchItem> = ops
            .iter()
            .map(|op| match op {
                BatchOp::Put { key, value } => KvBatchItem {
                    key: key.clone(),
                    value: value.clone(),
                    is_delete: false,
                },
                BatchOp::Delete { key } => KvBatchItem {
                    key: key.clone(),
                    value: Vec::new(),
                    is_delete: true,
                },
            })
            .collect();
        let mut endpoint = self.resolve_leader(store_id, group_id).await?;
        let mut attempts = 0u32;
        let mut backoff = self.retry.backoff_base;
        loop {
            let req = KvBatchWriteRequest {
                version: 1,
                items: items.clone(),
                seq,
                client_id: self.client_id,
                request_id: next_request_id(),
                request_create_ms: now_ms(),
                group_id,
            };
            let channel = self.pool.get(&endpoint)?;
            match KvServiceClient::new(channel).batch_write(req).await {
                Ok(resp) => {
                    let resp = resp.into_inner();
                    if resp.ok {
                        self.record_write(store_id, group_id, resp.revision);
                        return Ok(WriteOutcome {
                            revision: resp.revision,
                            request_id: resp.request_id,
                        });
                    }
                    if let Some(new_endpoint) = self.follow_not_leader(store_id, group_id, &resp) {
                        endpoint = new_endpoint;
                        continue;
                    }
                    attempts = self.count_other(attempts, &resp.error)?;
                    if Self::is_unknown_leader(&resp.error) {
                        endpoint = self.wait_and_refresh_leader(store_id, group_id, &endpoint).await;
                    }
                }
                Err(status) => {
                    if !self.retry.single_attempt {
                        endpoint = self
                            .handle_transport_err(store_id, group_id, &endpoint, &mut backoff)
                            .await;
                    }
                    attempts = self.count_other(attempts, &status.to_string())?;
                }
            }
        }
    }

    /// Prefix-scan a group's key space.
    ///
    /// # Errors
    /// See [`Error`].
    pub async fn scan(
        &self,
        store_id: u64,
        group_id: u64,
        prefix: &[u8],
        start_after: &[u8],
        limit: u32,
        read_mode: ReadMode,
    ) -> Result<ScanOutcome> {
        let mut endpoint = self.resolve_leader(store_id, group_id).await?;
        let mut attempts = 0u32;
        let mut backoff = self.retry.backoff_base;
        loop {
            let req = KvScanRequest {
                version: 1,
                prefix: prefix.to_vec(),
                start_after: start_after.to_vec(),
                limit,
                request_id: next_request_id(),
                request_create_ms: now_ms(),
                group_id,
                read_mode: read_mode as i32,
            };
            let channel = self.pool.get(&endpoint)?;
            match KvServiceClient::new(channel).scan(req).await {
                Ok(resp) => {
                    let resp = resp.into_inner();
                    if resp.ok {
                        let items = resp.items.into_iter().map(|i| (i.key, i.value)).collect();
                        return Ok(ScanOutcome {
                            items,
                            truncated: resp.truncated,
                        });
                    }
                    // `KvScanResponse` carries no `NotLeaderHint` (server-side
                    // forwarding already handles linearizable scans; other
                    // modes never need a leader) — any `!ok` is a plain error.
                    attempts = self.count_other(attempts, &resp.error)?;
                }
                Err(status) => {
                    if !self.retry.single_attempt {
                        endpoint = self
                            .handle_transport_err(store_id, group_id, &endpoint, &mut backoff)
                            .await;
                    }
                    attempts = self.count_other(attempts, &status.to_string())?;
                }
            }
        }
    }

    /// `ReadYourWrites` auto-attaches this client's own last-write watermark
    /// for the group unless the caller already supplied a `client_slot`.
    fn resolve_client_slot(
        &self,
        store_id: u64,
        group_id: u64,
        read_mode: ReadMode,
        client_slot: Option<u64>,
    ) -> u64 {
        if let Some(slot) = client_slot {
            return slot;
        }
        if read_mode == ReadMode::ReadYourWrites {
            return self.read_your_writes_slot(store_id, group_id);
        }
        0
    }

    /// If `resp` carries a `NotLeaderHint`, follow it immediately (uncounted
    /// retry — forward progress toward the real leader) and update the
    /// topology cache. Returns `None` if `resp` did not indicate not-leader
    /// (caller should treat it as a normal application error).
    fn follow_not_leader(&self, store_id: u64, group_id: u64, resp: &KvResponse) -> Option<String> {
        if resp.not_leader_hint.is_empty() {
            return None;
        }
        self.topology
            .set_leader(store_id, group_id, resp.not_leader_hint.clone());
        if self.retry.single_attempt {
            // Still worth caching the real leader for the *next* call, but
            // this call itself must not silently redirect -- see
            // `RetryConfig::single_attempt`.
            return None;
        }
        Some(resp.not_leader_hint.clone())
    }

    /// A `not leader` failure with an empty hint (the responding replica
    /// doesn't know who its leader is either -- typically mid-election,
    /// e.g. right after a restart; a real hint would have already been
    /// handled by [`Self::follow_not_leader`] before this is checked).
    fn is_unknown_leader(error: &str) -> bool {
        error == "not leader"
    }

    /// After an [`Self::is_unknown_leader`] failure, give the election a
    /// chance to converge and pick up whatever leader the cache learns in
    /// the meantime, instead of busy-retrying the same non-answering
    /// replica ("Unknown leader: 1s-then-retry").
    async fn wait_and_refresh_leader(&self, store_id: u64, group_id: u64, endpoint: &str) -> String {
        let _ = self.topology.refresh().await;
        tokio::time::sleep(self.retry.unknown_leader_wait).await;
        self.topology
            .leader(store_id, group_id)
            .unwrap_or_else(|| endpoint.to_string())
    }

    /// Transport-level failure (connect/timeout/unavailable): best-effort
    /// topology refresh (covers "leader moved and we don't know where"),
    /// exponential backoff, then return the (possibly updated) endpoint to
    /// retry against.
    async fn handle_transport_err(
        &self,
        store_id: u64,
        group_id: u64,
        current: &str,
        backoff: &mut Duration,
    ) -> String {
        let _ = self.topology.refresh().await;
        let endpoint = self
            .topology
            .leader(store_id, group_id)
            .unwrap_or_else(|| current.to_string());
        tokio::time::sleep(*backoff).await;
        *backoff = (*backoff * 2).min(self.retry.backoff_max);
        endpoint
    }

    /// Count one non-`NotLeaderHint` retryable outcome; errors once the
    /// configured retry budget (`RetryConfig::max_retries`) is exhausted.
    ///
    /// # Errors
    /// `Error::RetriesExhausted` once `attempts` exceeds the budget.
    fn count_other(&self, attempts: u32, last: &str) -> Result<u32> {
        let attempts = attempts + 1;
        if self.retry.single_attempt || attempts > self.retry.max_retries {
            return Err(Error::RetriesExhausted {
                attempts,
                last: last.to_string(),
            });
        }
        Ok(attempts)
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

fn next_request_id() -> u64 {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    u64::try_from(nanos).unwrap_or(u64::MAX)
}

/// A `client_id` unique enough for one client session ("opaque, assigned
/// once per client session"). Derived from the
/// process start time in nanoseconds; not a cryptographic identifier.
fn new_client_id() -> u64 {
    next_request_id()
}
