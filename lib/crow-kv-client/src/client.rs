// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! [`CrowkvClient`]: the C1-C3 client library (—
//! topology cache, retry/idempotency, and `ReadMode` routing on top of
//! `crow_kv`'s generated `KvService` client.

#![allow(clippy::cast_possible_truncation)]

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use dashmap::DashMap;
use tracing::warn;

use crow_kv::rpc::kv_service_client::KvServiceClient;
use crow_kv::rpc::{
    KvBatchItem, KvBatchWriteRequest, KvDeleteRequest, KvErrorCode, KvGetRequest, KvResponse, KvScanRequest,
    KvSetRequest, ReadMode,
};

use crate::config::{ClientConfig, ReadEndpointPolicy, RetryConfig};
use crate::error::{Error, Result};
use crate::metrics::ClientMetrics;
use crate::pool::ConnectionPool;
use crate::topology::TopologyCache;

/// Outcome of a successful `put`/`delete`/`batch_write`.
#[derive(Debug, Clone)]
pub struct WriteOutcome {
    pub revision: u64,
    pub request_id: u64,
}

/// Outcome of a successful `get`. `value` is zero-copy `Bytes` from the
/// prost response frame, not a `Vec<u8>` copy.
#[derive(Debug, Clone)]
pub enum GetOutcome {
    Found { value: Bytes, revision: u64 },
    NotFound,
}

/// Outcome of a successful `scan`. Items are zero-copy `Bytes` from the
/// prost response frame, not per-entry `Vec<u8>` copies.
#[derive(Debug, Clone)]
pub struct ScanOutcome {
    pub items: Vec<(Bytes, Bytes)>,
    pub truncated: bool,
}

/// One item of a `batch_write` call.
#[derive(Debug, Clone)]
pub enum BatchOp {
    Put { key: Bytes, value: Bytes },
    Delete { key: Bytes },
}

/// Standalone `CrowKV` client: topology discovery over the HTTP management
/// API, per-group leader cache, retry loop reusing `(client_id, seq)` across
/// retries of one logical write, and `ReadMode` routing including
/// `MinSlot` client-side slot tracking.
pub struct CrowkvClient {
    topology: TopologyCache,
    pool: ConnectionPool,
    retry: RetryConfig,
    client_id: u64,
    next_seq: AtomicU64,
    metrics: Arc<ClientMetrics>,
    /// Per-`(store_id, group_id)` high-watermark of the last write's
    /// `revision`, auto-attached as `min_slot` on `MinSlot` reads.
    /// Bounded by the number of groups this client has written to, not by
    /// keyspace size.
    write_watermark: DashMap<(u64, u64), u64>,
    /// `MinSlot` read-endpoint selection policy. `Leader` (default)
    /// preserves the pre-R26 behavior; `AnyReplica` distributes `MinSlot`
    /// reads round-robin across the topology cache's replica list.
    /// Linearizable reads always target the leader regardless of this.
    read_endpoint_policy: ReadEndpointPolicy,
    /// Per-`(store_id, group_id)` round-robin cursor for the
    /// `AnyReplica` `MinSlot` selector. Lock-free `fetch_add`; one entry
    /// per group the client has read from.
    read_rr: DashMap<(u64, u64), AtomicU64>,
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
            metrics: Arc::new(ClientMetrics::default()),
            write_watermark: DashMap::new(),
            read_endpoint_policy: config.read_endpoint_policy,
            read_rr: DashMap::new(),
        }
    }

    /// This client session's opaque `client_id`.
    #[must_use]
    pub fn client_id(&self) -> u64 {
        self.client_id
    }

    /// Snapshot the client's internal metrics counters (per-op counts,
    /// leader-related retry events, topology refreshes). Values are
    /// cumulative since client creation.
    #[must_use]
    pub fn metrics(&self) -> crate::metrics::ClientMetricsSnapshot {
        self.metrics.snapshot()
    }

    /// Drain per-op-kind window latency histograms. Returns one
    /// `Histogram<u64>` per op kind. The caller is expected to
    /// accumulate these into cumulative histograms if desired.
    #[must_use]
    pub fn drain_window(&self) -> crate::metrics::WindowLatencySnapshot {
        self.metrics.drain_window()
    }

    /// Flush per-op-kind window latency histograms to `writer` in the
    /// same column-aligned format as the server `[metrics]` log.
    /// Takes a pre-drained `WindowLatencySnapshot` so the caller can
    /// also use it for cumulative accumulation.
    pub fn flush_latencies<W: std::fmt::Write>(
        &self,
        writer: &mut W,
        snap: &crate::metrics::WindowLatencySnapshot,
        window_secs: f64,
    ) {
        self.metrics.flush_latencies(writer, snap, window_secs);
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
    /// long-lived embedders (e.g. `crow-console`) whose set of known
    /// nodes can grow at runtime.
    pub fn set_mgmt_seeds(&self, seeds: Vec<String>) {
        self.topology.set_seeds(seeds);
    }

    /// Directly seed the topology cache with a known leader endpoint for a
    /// group, bypassing `/topology` discovery entirely. For callers that
    /// already resolved an endpoint through some other discovery path
    /// (e.g. `crow-console`'s own management API) and just want
    /// `CrowkvClient`'s retry/pool machinery on top of it.
    pub fn seed_leader(&self, store_id: u64, group_id: u64, endpoint: String) {
        self.topology.set_leader(store_id, group_id, endpoint);
    }

    /// Resolve the current leader endpoint for `(store_id, group_id)`,
    /// retrying an "unknown leader" outcome ("100ms-then-retry") rather
    /// than failing on the first miss. A single failed/empty `/topology`
    /// fetch is not conclusive: the group may simply be mid-election (a
    /// real, common case right after a node restart) or the seed just
    /// queried may be transiently down while others are fine. Bounded by
    /// the same `RetryConfig::max_retries` budget used for post-request
    /// retries.
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
            self.metrics.record_leader_query();
            self.metrics.record_topology_refresh();
            let _ = self.topology.refresh().await;
            if let Some(ep) = self.topology.leader(store_id, group_id) {
                return Ok(ep);
            }
            attempts += 1;
            if attempts > self.retry.max_retries {
                self.metrics.record_no_leader();
                return Err(Error::NoLeader { store_id, group_id });
            }
            self.metrics.record_unknown_leader_wait();
            tokio::time::sleep(self.retry.unknown_leader_wait).await;
        }
    }

    /// Pick the first endpoint for a read. Linearizable reads always
    /// resolve to the leader (correctness: only the leader can prove a
    /// linearizable read is fresh). `MinSlot` reads under the `Leader`
    /// policy also resolve to the leader (backward-compatible default).
    /// `MinSlot` reads under `AnyReplica` round-robin across the
    /// topology cache's replica list for the group; if no replica list
    /// is known (cache miss) the client refreshes `/topology` once and
    /// retries, falling back to the leader if still unknown — a
    /// single-replica group or a stale `/topology` never blocks reads.
    async fn resolve_read_endpoint(
        &self,
        store_id: u64,
        group_id: u64,
        read_mode: ReadMode,
    ) -> Result<String> {
        if read_mode == ReadMode::Linearizable || self.read_endpoint_policy == ReadEndpointPolicy::Leader {
            return self.resolve_leader(store_id, group_id).await;
        }
        // `MinSlot` + `AnyReplica`: pick a replica round-robin.
        if self.topology.replicas(store_id, group_id).is_none() {
            self.metrics.record_topology_refresh();
            let _ = self.topology.refresh().await;
        }
        let replicas = match self.topology.replicas(store_id, group_id) {
            Some(r) if !r.is_empty() => r,
            _ => {
                // No replica list available (single-replica group, or
                // every seed unreachable): fall back to the leader
                // rather than failing the read.
                return self.resolve_leader(store_id, group_id).await;
            }
        };
        let cursor = self
            .read_rr
            .entry((store_id, group_id))
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_add(1, Ordering::Relaxed);
        let idx = (cursor as usize) % replicas.len();
        self.metrics.record_read_endpoint_distributed();
        Ok(replicas[idx].clone())
    }

    fn record_write(&self, store_id: u64, group_id: u64, revision: u64) {
        self.write_watermark
            .entry((store_id, group_id))
            .and_modify(|w| *w = (*w).max(revision))
            .or_insert(revision);
    }

    /// Cached `min_slot` for `MinSlot` reads against this group:
    /// the highest `revision` this client has observed from its own writes,
    /// or `0` if it has never written to this group.
    #[must_use]
    pub fn read_your_writes_slot(&self, store_id: u64, group_id: u64) -> u64 {
        self.write_watermark.get(&(store_id, group_id)).map_or(0, |v| *v)
    }

    /// `Put` a single key/value.
    ///
    /// `ids`: override `(client_id, seq)` for callers that manage their own
    /// idempotency keys (e.g. `crow-console`'s HTTP API, which lets an
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
                key: Bytes::copy_from_slice(key),
                value: Bytes::copy_from_slice(value),
                seq,
                ttl_ms: 0,
                client_id,
                request_id: next_request_id(),
                request_create_ms: now_ms(),
                group_id,
            };
            let channel = self.pool.get(&endpoint)?;
            let t0 = Instant::now();
            match KvServiceClient::new(channel).put(req).await {
                Ok(resp) => {
                    let resp = resp.into_inner();
                    if resp.ok {
                        self.record_write(store_id, group_id, resp.revision);
                        self.metrics.record_put_latency(t0.elapsed().as_micros() as u64);
                        return Ok(WriteOutcome {
                            revision: resp.revision,
                            request_id: resp.request_id,
                        });
                    }
                    self.metrics.record_put_error();
                    if let Some(new_endpoint) = self.follow_not_leader(store_id, group_id, &resp) {
                        self.metrics.record_not_leader_hint();
                        self.metrics.on_leader_error(store_id, group_id, &endpoint);
                        self.metrics
                            .on_leader_resolved(store_id, group_id, &new_endpoint, "not_leader_hint");
                        endpoint = new_endpoint;
                        continue;
                    }
                    attempts = self.count_other(attempts, &resp.error)?;
                    if Self::is_unknown_leader(resp.error_code, &resp.error) {
                        self.metrics.on_leader_error(store_id, group_id, &endpoint);
                        endpoint = self.wait_and_refresh_leader(store_id, group_id, &endpoint).await;
                        self.metrics
                            .on_leader_resolved(store_id, group_id, &endpoint, "unknown_leader");
                    }
                }
                Err(status) => {
                    self.metrics.record_put_error();
                    self.metrics.record_transport_error();
                    self.metrics.on_leader_error(store_id, group_id, &endpoint);
                    endpoint = self
                        .handle_transport_err(store_id, group_id, &endpoint, &mut backoff)
                        .await;
                    self.metrics
                        .on_leader_resolved(store_id, group_id, &endpoint, "transport_error");
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
        min_slot: Option<u64>,
    ) -> Result<GetOutcome> {
        let min_slot = self.resolve_min_slot(store_id, group_id, read_mode, min_slot);
        let mut endpoint = self.resolve_read_endpoint(store_id, group_id, read_mode).await?;
        let mut attempts = 0u32;
        let mut backoff = self.retry.backoff_base;
        loop {
            let req = KvGetRequest {
                version: 1,
                key: Bytes::copy_from_slice(key),
                request_id: next_request_id(),
                request_create_ms: now_ms(),
                group_id,
                read_mode: read_mode as i32,
                min_slot,
            };
            let channel = self.pool.get(&endpoint)?;
            let t0 = Instant::now();
            match KvServiceClient::new(channel).get(req).await {
                Ok(resp) => {
                    let resp = resp.into_inner();
                    if resp.not_found {
                        self.metrics.record_get_latency(t0.elapsed().as_micros() as u64);
                        return Ok(GetOutcome::NotFound);
                    }
                    if resp.ok {
                        self.metrics.record_get_latency(t0.elapsed().as_micros() as u64);
                        return Ok(GetOutcome::Found {
                            value: resp.value,
                            revision: resp.revision,
                        });
                    }
                    self.metrics.record_get_error();
                    if let Some(new_endpoint) = self.follow_not_leader(store_id, group_id, &resp) {
                        self.metrics.record_not_leader_hint();
                        self.metrics.on_leader_error(store_id, group_id, &endpoint);
                        self.metrics
                            .on_leader_resolved(store_id, group_id, &new_endpoint, "not_leader_hint");
                        // A `MinSlot` read distributed to a follower
                        // that hasn't applied `min_slot` redirects to
                        // the leader here — count the distribution
                        // fallback so operators can confirm the rate
                        // stays low.
                        if read_mode == ReadMode::MinSlot
                            && self.read_endpoint_policy == ReadEndpointPolicy::AnyReplica
                        {
                            self.metrics.record_read_endpoint_fallback();
                        }
                        endpoint = new_endpoint;
                        continue;
                    }
                    attempts = self.count_other(attempts, &resp.error)?;
                    if Self::is_unknown_leader(resp.error_code, &resp.error) {
                        self.metrics.on_leader_error(store_id, group_id, &endpoint);
                        endpoint = self.wait_and_refresh_leader(store_id, group_id, &endpoint).await;
                        self.metrics
                            .on_leader_resolved(store_id, group_id, &endpoint, "unknown_leader");
                    }
                }
                Err(status) => {
                    self.metrics.record_get_error();
                    self.metrics.record_transport_error();
                    self.metrics.on_leader_error(store_id, group_id, &endpoint);
                    endpoint = self
                        .handle_transport_err(store_id, group_id, &endpoint, &mut backoff)
                        .await;
                    self.metrics
                        .on_leader_resolved(store_id, group_id, &endpoint, "transport_error");
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
                key: Bytes::copy_from_slice(key),
                seq,
                client_id,
                request_id: next_request_id(),
                request_create_ms: now_ms(),
                group_id,
            };
            let channel = self.pool.get(&endpoint)?;
            let t0 = Instant::now();
            match KvServiceClient::new(channel).delete(req).await {
                Ok(resp) => {
                    let resp = resp.into_inner();
                    if resp.not_found {
                        self.metrics
                            .record_delete_latency(t0.elapsed().as_micros() as u64);
                        return Ok(WriteOutcome {
                            revision: 0,
                            request_id: resp.request_id,
                        });
                    }
                    if resp.ok {
                        self.record_write(store_id, group_id, resp.revision);
                        self.metrics
                            .record_delete_latency(t0.elapsed().as_micros() as u64);
                        return Ok(WriteOutcome {
                            revision: resp.revision,
                            request_id: resp.request_id,
                        });
                    }
                    self.metrics.record_delete_error();
                    if let Some(new_endpoint) = self.follow_not_leader(store_id, group_id, &resp) {
                        self.metrics.record_not_leader_hint();
                        self.metrics.on_leader_error(store_id, group_id, &endpoint);
                        self.metrics
                            .on_leader_resolved(store_id, group_id, &new_endpoint, "not_leader_hint");
                        endpoint = new_endpoint;
                        continue;
                    }
                    attempts = self.count_other(attempts, &resp.error)?;
                    if Self::is_unknown_leader(resp.error_code, &resp.error) {
                        self.metrics.on_leader_error(store_id, group_id, &endpoint);
                        endpoint = self.wait_and_refresh_leader(store_id, group_id, &endpoint).await;
                        self.metrics
                            .on_leader_resolved(store_id, group_id, &endpoint, "unknown_leader");
                    }
                }
                Err(status) => {
                    self.metrics.record_delete_error();
                    self.metrics.record_transport_error();
                    self.metrics.on_leader_error(store_id, group_id, &endpoint);
                    endpoint = self
                        .handle_transport_err(store_id, group_id, &endpoint, &mut backoff)
                        .await;
                    self.metrics
                        .on_leader_resolved(store_id, group_id, &endpoint, "transport_error");
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
                    value: Bytes::new(),
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
            let t0 = Instant::now();
            match KvServiceClient::new(channel).batch_write(req).await {
                Ok(resp) => {
                    let resp = resp.into_inner();
                    if resp.ok {
                        self.record_write(store_id, group_id, resp.revision);
                        self.metrics
                            .record_batch_write_latency(t0.elapsed().as_micros() as u64);
                        return Ok(WriteOutcome {
                            revision: resp.revision,
                            request_id: resp.request_id,
                        });
                    }
                    self.metrics.record_batch_write_error();
                    if let Some(new_endpoint) = self.follow_not_leader(store_id, group_id, &resp) {
                        self.metrics.record_not_leader_hint();
                        self.metrics.on_leader_error(store_id, group_id, &endpoint);
                        self.metrics
                            .on_leader_resolved(store_id, group_id, &new_endpoint, "not_leader_hint");
                        endpoint = new_endpoint;
                        continue;
                    }
                    attempts = self.count_other(attempts, &resp.error)?;
                    if Self::is_unknown_leader(resp.error_code, &resp.error) {
                        self.metrics.on_leader_error(store_id, group_id, &endpoint);
                        endpoint = self.wait_and_refresh_leader(store_id, group_id, &endpoint).await;
                        self.metrics
                            .on_leader_resolved(store_id, group_id, &endpoint, "unknown_leader");
                    }
                }
                Err(status) => {
                    self.metrics.record_batch_write_error();
                    self.metrics.record_transport_error();
                    self.metrics.on_leader_error(store_id, group_id, &endpoint);
                    endpoint = self
                        .handle_transport_err(store_id, group_id, &endpoint, &mut backoff)
                        .await;
                    self.metrics
                        .on_leader_resolved(store_id, group_id, &endpoint, "transport_error");
                    attempts = self.count_other(attempts, &status.to_string())?;
                }
            }
        }
    }

    /// Prefix-scan a group's key space.
    ///
    /// # Errors
    /// See [`Error`].
    #[allow(clippy::too_many_arguments)]
    pub async fn scan(
        &self,
        store_id: u64,
        group_id: u64,
        prefix: &[u8],
        start_after: &[u8],
        limit: u32,
        read_mode: ReadMode,
        min_slot: Option<u64>,
    ) -> Result<ScanOutcome> {
        let min_slot = self.resolve_min_slot(store_id, group_id, read_mode, min_slot);
        let mut endpoint = self.resolve_read_endpoint(store_id, group_id, read_mode).await?;
        let mut attempts = 0u32;
        let mut backoff = self.retry.backoff_base;
        loop {
            let req = KvScanRequest {
                version: 1,
                prefix: Bytes::copy_from_slice(prefix),
                start_after: Bytes::copy_from_slice(start_after),
                limit,
                request_id: next_request_id(),
                request_create_ms: now_ms(),
                group_id,
                read_mode: read_mode as i32,
                min_slot,
            };
            let channel = self.pool.get(&endpoint)?;
            let t0 = Instant::now();
            match KvServiceClient::new(channel).scan(req).await {
                Ok(resp) => {
                    let resp = resp.into_inner();
                    if resp.ok {
                        let items = resp.items.into_iter().map(|i| (i.key, i.value)).collect();
                        self.metrics.record_scan_latency(t0.elapsed().as_micros() as u64);
                        return Ok(ScanOutcome {
                            items,
                            truncated: resp.truncated,
                        });
                    }
                    self.metrics.record_scan_error();
                    // Follow a `not_leader_hint` redirect (uncounted,
                    // mirroring the `get` path) so a `MinSlot` scan
                    // against a follower that hasn't applied `min_slot`
                    // falls back to the leader rather than being treated
                    // as a plain error.
                    if !resp.not_leader_hint.is_empty() {
                        if read_mode == ReadMode::MinSlot
                            && self.read_endpoint_policy == ReadEndpointPolicy::AnyReplica
                        {
                            self.metrics.record_read_endpoint_fallback();
                        }
                        self.topology
                            .set_leader(store_id, group_id, resp.not_leader_hint.clone());
                        endpoint = resp.not_leader_hint;
                        continue;
                    }
                    attempts = self.count_other(attempts, &resp.error)?;
                }
                Err(status) => {
                    self.metrics.record_scan_error();
                    self.metrics.record_transport_error();
                    self.metrics.on_leader_error(store_id, group_id, &endpoint);
                    endpoint = self
                        .handle_transport_err(store_id, group_id, &endpoint, &mut backoff)
                        .await;
                    self.metrics
                        .on_leader_resolved(store_id, group_id, &endpoint, "transport_error");
                    attempts = self.count_other(attempts, &status.to_string())?;
                }
            }
        }
    }

    /// `MinSlot` auto-attaches this client's own last-write watermark
    /// for the group unless the caller already supplied a `min_slot`.
    fn resolve_min_slot(
        &self,
        store_id: u64,
        group_id: u64,
        read_mode: ReadMode,
        min_slot: Option<u64>,
    ) -> u64 {
        if let Some(slot) = min_slot {
            return slot;
        }
        if read_mode == ReadMode::MinSlot {
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
        Some(resp.not_leader_hint.clone())
    }

    /// A `not leader` failure with an empty hint (the responding replica
    /// doesn't know who its leader is either -- typically mid-election,
    /// e.g. right after a restart; a real hint would have already been
    /// handled by [`Self::follow_not_leader`] before this is checked).
    /// Checks the structured `error_code` first, falling back to the
    /// string for old servers that don't set the code (default 0 =
    /// `KvErrorNone`).
    fn is_unknown_leader(error_code: i32, error: &str) -> bool {
        error_code == KvErrorCode::KvErrorNotLeader as i32 || error == "not leader"
    }

    /// After an [`Self::is_unknown_leader`] failure, give the election a
    /// chance to converge and pick up whatever leader the cache learns in
    /// the meantime, instead of busy-retrying the same non-answering
    /// replica ("100ms-then-retry"). Logs refresh failures instead of
    /// silently swallowing them; the caller's `count_other` surfaces
    /// `RetriesExhausted` if the endpoint stays stale.
    async fn wait_and_refresh_leader(&self, store_id: u64, group_id: u64, endpoint: &str) -> String {
        self.metrics.record_unknown_leader_wait();
        self.metrics.record_leader_query();
        self.metrics.record_topology_refresh();
        if let Err(e) = self.topology.refresh().await {
            warn!(error = %e, "topology refresh failed in wait_and_refresh_leader");
        }
        tokio::time::sleep(self.retry.unknown_leader_wait).await;
        self.topology
            .leader(store_id, group_id)
            .unwrap_or_else(|| endpoint.to_string())
    }

    /// Transport-level failure (connect/timeout/unavailable): best-effort
    /// topology refresh (covers "leader moved and we don't know where"),
    /// exponential backoff, then return the (possibly updated) endpoint to
    /// retry against. Logs refresh failures instead of silently
    /// swallowing them; the caller's `count_other` surfaces
    /// `RetriesExhausted` if the endpoint stays stale.
    async fn handle_transport_err(
        &self,
        store_id: u64,
        group_id: u64,
        current: &str,
        backoff: &mut Duration,
    ) -> String {
        self.metrics.record_topology_refresh();
        if let Err(e) = self.topology.refresh().await {
            warn!(error = %e, "topology refresh failed in handle_transport_err");
        }
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
        if attempts > self.retry.max_retries {
            self.metrics.record_retries_exhausted();
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
