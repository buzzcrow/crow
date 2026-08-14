// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! [`CrowkvClient`]: the C1-C3 client library (—
//! topology cache, retry/idempotency, and `ReadMode` routing on top of
//! `crow_kv`'s generated `KvService` client.

#![allow(clippy::cast_possible_truncation)]

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use dashmap::DashMap;

use crow_kv::rpc::kv_service_client::KvServiceClient;
use crow_kv::rpc::{
    KvBatchItem, KvBatchWriteRequest, KvDeleteRequest, KvGetRequest, KvJournalScanRequest, KvScanRequest,
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
    pub timed_out: bool,
    /// The applied frontier when the scan ran (page 1's `read_slot`).
    /// Used by `get_applied_slot` to read the data group's frontier via
    /// a linearizable scan.
    pub read_slot: u64,
}

/// One op from a `journal_scan` — a Put or Delete at a specific commit
/// slot. `value` is empty for Delete. Zero-copy `Bytes` from the prost
/// response frame.
#[derive(Debug, Clone)]
pub struct JournalOp {
    pub key: Bytes,
    pub value: Bytes,
    pub is_delete: bool,
    pub slot: u64,
}

/// Outcome of a successful `journal_scan`. Ops are in slot order
/// (within a slot, in batch order). `truncated` means the caller's
/// `limit` was reached and more ops exist beyond it.
#[derive(Debug, Clone)]
pub struct JournalScanOutcome {
    pub ops: Vec<JournalOp>,
    pub truncated: bool,
    pub read_slot: u64,
}

/// One item of a `batch_write` call.
#[derive(Debug, Clone)]
pub enum BatchOp {
    Put { key: Bytes, value: Bytes },
    Delete { key: Bytes },
}

/// Per-endpoint statistics for `LeastConnections` / `Latency` read
/// routing. Stored in a `DashMap<String, EndpointStats>` keyed by
/// endpoint string. All fields are lock-free atomics — updated on the
/// hot path with `Relaxed` ordering (no locks, no allocation).
#[derive(Debug, Default)]
struct EndpointStats {
    /// In-flight read count for this endpoint. Incremented before the
    /// gRPC send, decremented when the response arrives (via
    /// [`InFlightGuard`] drop). Used by `LeastConnections` selection.
    in_flight: AtomicI64,
    /// EWMA of get RTT in microseconds, updated on each `Ok` response.
    /// `0` means no history yet (treated as a tie by `Latency`
    /// selection). Updated via CAS loop with `alpha = 0.25`.
    rtt_ewma_us: AtomicU64,
}

impl EndpointStats {
    /// Update the RTT EWMA with a new sample. `alpha = 0.25`: the new
    /// sample gets a quarter weight, so a single spike moves the EWMA
    /// by 25% and decays over ~4 samples. The first sample initializes
    /// the EWMA directly.
    fn record_rtt(&self, rtt_us: u64) {
        let mut old = self.rtt_ewma_us.load(Ordering::Relaxed);
        loop {
            let new = if old == 0 {
                rtt_us
            } else {
                old / 4 * 3 + rtt_us / 4
            };
            match self
                .rtt_ewma_us
                .compare_exchange_weak(old, new, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => break,
                Err(actual) => old = actual,
            }
        }
    }

    /// Current in-flight count, loaded `Relaxed` — used only for
    /// selection comparison, not for ordering guarantees.
    #[must_use]
    fn in_flight_count(&self) -> i64 {
        self.in_flight.load(Ordering::Relaxed)
    }

    /// Current RTT EWMA in micros. `0` means no history.
    #[must_use]
    fn rtt_ewma(&self) -> u64 {
        self.rtt_ewma_us.load(Ordering::Relaxed)
    }
}

/// RAII guard that decrements the endpoint's in-flight count on drop.
/// Created before the gRPC send; dropped at the end of the retry-loop
/// iteration (covers all exit paths: success, error, redirect, `?`).
/// Holds an `Arc<EndpointStats>` so it can live across `.await` points
/// (a `DashMap` entry guard is not `Send`).
pub(crate) struct InFlightGuard {
    stats: Arc<EndpointStats>,
}

impl InFlightGuard {
    fn new(stats: Arc<EndpointStats>) -> Self {
        stats.in_flight.fetch_add(1, Ordering::Relaxed);
        Self { stats }
    }
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.stats.in_flight.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Standalone `CrowKV` client: topology discovery over the HTTP management
/// API, per-group leader cache, retry loop reusing `(client_id, seq)` across
/// retries of one logical write, and `ReadMode` routing including
/// `MinSlot` client-side slot tracking.
pub struct CrowkvClient {
    pub(crate) topology: TopologyCache,
    pub(crate) pool: ConnectionPool,
    pub(crate) retry: RetryConfig,
    client_id: u64,
    next_seq: AtomicU64,
    pub(crate) metrics: Arc<ClientMetrics>,
    /// Per-`(store_id, group_id)` high-watermark of the last write's
    /// paxos slot, auto-attached as `min_slot` on `MinSlot` reads.
    /// Bounded by the number of groups this client has written to, not by
    /// keyspace size. Evicted when a group disappears from the topology
    /// (via `TopologyCache`'s eviction hook) — a stale `min_slot`
    /// high-watermark does not self-heal and causes silent empty reads
    /// on a reused group ID.
    write_slot_highwater: Arc<DashMap<(u64, u64), u64>>,
    /// `MinSlot` read-endpoint selection policy. `Leader` (default)
    /// preserves the pre-R26 behavior; `AnyReplica` distributes `MinSlot`
    /// reads round-robin across the topology cache's replica list.
    /// Linearizable reads always target the leader regardless of this.
    read_endpoint_policy: ReadEndpointPolicy,
    /// Per-`(store_id, group_id)` round-robin cursor for the
    /// `AnyReplica` `MinSlot` selector. Lock-free `fetch_add`; one entry
    /// per group the client has read from.
    read_rr: DashMap<(u64, u64), AtomicU64>,
    /// Per-endpoint statistics for `LeastConnections` / `Latency`
    /// selection. Keyed by endpoint string (same keys as the topology
    /// cache's replica list). Entries are created lazily on first
    /// selection and never evicted — stale entries (replica removed
    /// from topology) simply accumulate zero in-flight and zero RTT,
    /// never selected again. `Arc` values so `InFlightGuard` can hold
    /// a clone across `.await` points.
    endpoint_stats: DashMap<String, Arc<EndpointStats>>,
}

impl CrowkvClient {
    #[must_use]
    pub fn new(config: ClientConfig) -> Self {
        // The eviction hook removes stale `write_slot_highwater` entries
        // when a group disappears from the topology. The hook captures a
        // raw pointer pattern via `Arc<DashMap>` — we create the DashMap
        // first, then build the hook referencing it, then the cache.
        let write_slot_highwater: Arc<DashMap<(u64, u64), u64>> = Arc::new(DashMap::new());
        let eviction_hook_map = Arc::clone(&write_slot_highwater);
        let eviction_hook: crate::topology::EvictionHook = Arc::new(move |evicted| {
            for key in evicted {
                eviction_hook_map.remove(key);
            }
        });
        Self {
            topology: TopologyCache::with_eviction_hook(
                config.mgmt_seeds,
                config.topology_min_refresh_interval,
                Some(eviction_hook),
            ),
            pool: ConnectionPool::new(config.pool_size_per_endpoint),
            retry: config.retry,
            client_id: new_client_id(),
            next_seq: AtomicU64::new(1),
            metrics: Arc::new(ClientMetrics::default()),
            write_slot_highwater,
            read_endpoint_policy: config.read_endpoint_policy,
            read_rr: DashMap::new(),
            endpoint_stats: DashMap::new(),
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

    /// The system KV group's store id (always 0). Group 0 of store 0
    /// is the fixed directory holding hardware/service-registry/
    /// KV-cluster-topology records.
    pub const SYSTEM_STORE: u64 = 0;
    /// The system KV group's group id (always 0).
    pub const SYSTEM_GROUP: u64 = 0;

    /// The system KV group `(store_id, group_id)` — group 0 of store
    /// 0, the fixed directory holding hardware/service-registry/
    /// KV-cluster-topology records. Group-0 service classes
    /// (`HardwareClient`, `ServiceRegistryClient`,
    /// `KVClusterMetaClient`) target this group; callers can use this
    /// instead of hardcoding `(0, 0)`.
    #[must_use]
    pub fn system_group(&self) -> (u64, u64) {
        (Self::SYSTEM_STORE, Self::SYSTEM_GROUP)
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
    /// `MinSlot` reads under a distributed policy (`AnyReplica`,
    /// `LeastConnections`, `Latency`) pick from the topology cache's
    /// replica list; if no replica list is known (cache miss) the client
    /// refreshes `/topology` once and retries, falling back to the
    /// leader if still unknown — a single-replica group or a stale
    /// `/topology` never blocks reads.
    pub(crate) async fn resolve_read_endpoint(
        &self,
        store_id: u64,
        group_id: u64,
        read_mode: ReadMode,
    ) -> Result<String> {
        if read_mode == ReadMode::Linearizable || self.read_endpoint_policy == ReadEndpointPolicy::Leader {
            return self.resolve_leader(store_id, group_id).await;
        }
        // `MinSlot` + distributed policy: pick from the replica list.
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
        let idx = self.select_replica_index(store_id, group_id, &replicas);
        self.metrics.record_read_endpoint_distributed();
        Ok(replicas[idx].clone())
    }

    /// Select a replica index from the list according to the active
    /// distributed policy. `AnyReplica` → round-robin;
    /// `LeastConnections` → min in-flight (ties → round-robin);
    /// `Latency` → min RTT EWMA (no history / ties → round-robin).
    /// The round-robin cursor (`read_rr`) is always advanced so tie-
    /// breaks are evenly distributed.
    fn select_replica_index(&self, store_id: u64, group_id: u64, replicas: &[String]) -> usize {
        let cursor = self
            .read_rr
            .entry((store_id, group_id))
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_add(1, Ordering::Relaxed);
        let rr_idx = (cursor as usize) % replicas.len();
        match self.read_endpoint_policy {
            ReadEndpointPolicy::Leader | ReadEndpointPolicy::AnyReplica => rr_idx,
            ReadEndpointPolicy::LeastConnections => {
                // Start with the round-robin candidate's count so ties
                // keep the round-robin index (even distribution).
                let mut best_idx = rr_idx;
                let mut best_count = self
                    .endpoint_stats
                    .entry(replicas[rr_idx].clone())
                    .or_default()
                    .in_flight_count();
                for (i, ep) in replicas.iter().enumerate() {
                    if i == rr_idx {
                        continue;
                    }
                    let count = self
                        .endpoint_stats
                        .entry(ep.clone())
                        .or_default()
                        .in_flight_count();
                    if count < best_count {
                        best_count = count;
                        best_idx = i;
                    }
                }
                best_idx
            }
            ReadEndpointPolicy::Latency => {
                // Start with the round-robin candidate's RTT so ties
                // (including all-zero / no history) keep the round-robin
                // index. A non-zero RTT only wins over another non-zero
                // RTT that is higher — `0` (no history) is never
                // preferred over the round-robin candidate.
                let mut best_idx = rr_idx;
                let mut best_rtt = self
                    .endpoint_stats
                    .entry(replicas[rr_idx].clone())
                    .or_default()
                    .rtt_ewma();
                for (i, ep) in replicas.iter().enumerate() {
                    if i == rr_idx {
                        continue;
                    }
                    let rtt = self.endpoint_stats.entry(ep.clone()).or_default().rtt_ewma();
                    if rtt > 0 && best_rtt > 0 && rtt < best_rtt {
                        best_rtt = rtt;
                        best_idx = i;
                    }
                }
                best_idx
            }
        }
    }

    /// Get or create `EndpointStats` for `endpoint` and return an
    /// `InFlightGuard` that decrements the in-flight count on drop.
    /// Used in the get/scan retry loops to track per-endpoint load for
    /// `LeastConnections` selection.
    pub(crate) fn incr_in_flight(&self, endpoint: &str) -> InFlightGuard {
        let entry = self
            .endpoint_stats
            .entry(endpoint.to_string())
            .or_insert_with(|| Arc::new(EndpointStats::default()))
            .clone();
        InFlightGuard::new(entry)
    }

    /// Record the RTT for `endpoint` into its EWMA. Called on every
    /// `Ok` response (success, not-found, `NotLeader` redirect); not
    /// called on transport errors (a timeout doesn't reflect the
    /// endpoint's serving latency). Used by `Latency` selection.
    fn record_endpoint_rtt(&self, endpoint: &str, rtt_us: u64) {
        if let Some(entry) = self.endpoint_stats.get(endpoint) {
            entry.record_rtt(rtt_us);
        }
    }

    fn record_write(&self, store_id: u64, group_id: u64, revision: u64) {
        self.write_slot_highwater
            .entry((store_id, group_id))
            .and_modify(|w| *w = (*w).max(revision))
            .or_insert(revision);
    }

    /// Cached `min_slot` for `MinSlot` reads against this group:
    /// the highest paxos slot this client has observed from its own writes,
    /// or `0` if it has never written to this group.
    #[must_use]
    pub fn read_your_writes_slot(&self, store_id: u64, group_id: u64) -> u64 {
        self.write_slot_highwater
            .get(&(store_id, group_id))
            .map_or(0, |v| *v)
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
            let _in_flight = self.incr_in_flight(&endpoint);
            match KvServiceClient::new(channel).get(req).await {
                Ok(resp) => {
                    self.record_endpoint_rtt(&endpoint, t0.elapsed().as_micros() as u64);
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
                        if read_mode == ReadMode::MinSlot && self.read_endpoint_policy.is_distributed() {
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

    /// Prefix-scan a group's key space. Uses S3-style pagination
    /// (`start_after` + `truncated`): the server applies a byte budget to
    /// each unary response so every page is provably bounded regardless of
    /// value sizes, and this method transparently pages until `!truncated`
    /// or the caller's `limit` is reached. The returned `ScanOutcome.truncated`
    /// flag means "more entries exist beyond the caller's `limit`". When
    /// `keys_only` is true, items carry empty values (no value materialization
    /// on the server); pagination is unchanged.
    ///
    /// # Panics
    /// Panics if the server returns a truncated page with zero items — an
    /// impossible state (truncated implies items were returned but more
    /// remain). The `page_len > 0` guard prevents this.
    ///
    /// # Errors
    /// See [`Error`].
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    pub async fn scan(
        &self,
        store_id: u64,
        group_id: u64,
        prefix: &[u8],
        start_after: &[u8],
        end_key: &[u8],
        limit: u32,
        read_mode: ReadMode,
        min_slot: Option<u64>,
        keys_only: bool,
        deadline: Option<u64>,
    ) -> Result<ScanOutcome> {
        let min_slot = self.resolve_min_slot(store_id, group_id, read_mode, min_slot);
        let mut endpoint = self.resolve_read_endpoint(store_id, group_id, read_mode).await?;
        let mut attempts = 0u32;
        let mut backoff = self.retry.backoff_base;
        // Inner pagination state: collect pages until !truncated or limit reached.
        let mut all_items: Vec<(Bytes, Bytes)> = Vec::new();
        let mut page_start_after: Vec<u8> = start_after.to_vec();
        // After page 1 of a Linearizable scan returns read_slot = S, switch
        // subsequent pages to MinSlot with min_slot = S. Later pages only need
        // to be at least as fresh as page 1 (a paginated scan was never a
        // single snapshot), so MinSlot with the page-1 floor skips the
        // per-page leader barrier. The leader has S applied and serves locally
        // without the barrier; a redirect mid-scan lands on the leader, which
        // also has S applied.
        let mut page_read_mode = read_mode;
        let mut page_min_slot = min_slot;
        let mut page1_read_slot: Option<u64> = None;
        loop {
            // Remaining entry-count budget for this page. The server's byte
            // budget may stop the page before this limit is reached; that's
            // fine — `truncated` tells us to fetch the next page.
            let remaining_limit = if limit == 0 {
                0 // unlimited: let the server's byte budget page
            } else {
                limit.saturating_sub(u32::try_from(all_items.len()).unwrap_or(u32::MAX))
            };
            let req = KvScanRequest {
                version: 1,
                prefix: Bytes::copy_from_slice(prefix),
                start_after: Bytes::copy_from_slice(&page_start_after),
                end_key: Bytes::copy_from_slice(end_key),
                limit: remaining_limit,
                request_id: next_request_id(),
                request_create_ms: now_ms(),
                group_id,
                read_mode: page_read_mode as i32,
                min_slot: page_min_slot,
                keys_only,
                count_only: false,
                deadline_ms: deadline.unwrap_or(0),
            };
            let channel = self.pool.get(&endpoint)?;
            let t0 = Instant::now();
            let _in_flight = self.incr_in_flight(&endpoint);
            match KvServiceClient::new(channel).scan(req).await {
                Ok(resp) => {
                    self.record_endpoint_rtt(&endpoint, t0.elapsed().as_micros() as u64);
                    let resp = resp.into_inner();
                    if resp.ok {
                        let page_len = resp.items.len();
                        let resp_truncated = resp.truncated;
                        // Capture page-1 read_slot and switch subsequent
                        // pages to MinSlot with that slot as the freshness
                        // floor, skipping the per-page leader barrier.
                        if page1_read_slot.is_none() && read_mode == ReadMode::Linearizable {
                            page1_read_slot = Some(resp.read_slot);
                            page_read_mode = ReadMode::MinSlot;
                            page_min_slot = resp.read_slot;
                        }
                        for item in resp.items {
                            all_items.push((item.key, item.value));
                        }
                        // If the page was truncated (server hit byte budget or
                        // entry limit) and we haven't reached the caller's
                        // limit yet, fetch the next page using the last key as
                        // the new start_after. A zero-item page with
                        // truncated=true is a safety stop (avoid infinite loop).
                        if resp_truncated && page_len > 0 && (limit == 0 || all_items.len() < limit as usize)
                        {
                            // Deadline check before fetching the next page: if
                            // the deadline has fired (either the server set
                            // timed_out on this page, or the client-side
                            // deadline has elapsed), stop with a partial result.
                            let server_timed_out = resp.timed_out;
                            let client_timed_out = deadline.is_some_and(|dl| now_ms() >= dl);
                            if server_timed_out || client_timed_out {
                                self.metrics.record_scan_latency(t0.elapsed().as_micros() as u64);
                                return Ok(ScanOutcome {
                                    items: all_items,
                                    truncated: true,
                                    timed_out: true,
                                    read_slot: page1_read_slot.unwrap_or(0),
                                });
                            }
                            page_start_after = all_items.last().expect("non-empty page").0.to_vec();
                            continue;
                        }
                        self.metrics.record_scan_latency(t0.elapsed().as_micros() as u64);
                        // `truncated` in the outcome means "more exist beyond
                        // the caller's limit", not "this page was truncated".
                        let outcome_truncated = if limit == 0 {
                            resp_truncated
                        } else {
                            resp_truncated && all_items.len() >= limit as usize
                        };
                        // If the caller's limit was reached, truncate.
                        if limit != 0 && all_items.len() > limit as usize {
                            all_items.truncate(limit as usize);
                        }
                        return Ok(ScanOutcome {
                            items: all_items,
                            truncated: outcome_truncated,
                            timed_out: resp.timed_out,
                            read_slot: page1_read_slot.unwrap_or(0),
                        });
                    }
                    self.metrics.record_scan_error();
                    // Follow a `not_leader_hint` redirect (uncounted,
                    // mirroring the `get` path) so a `MinSlot` scan
                    // against a follower that hasn't applied `min_slot`
                    // falls back to the leader rather than being treated
                    // as a plain error. Resume pagination from the last
                    // received key on the new endpoint (see below).
                    if !resp.not_leader_hint.is_empty() {
                        if page_read_mode == ReadMode::MinSlot && self.read_endpoint_policy.is_distributed() {
                            self.metrics.record_read_endpoint_fallback();
                        }
                        self.topology
                            .set_leader(store_id, group_id, resp.not_leader_hint.clone());
                        endpoint = resp.not_leader_hint;
                        // Resume from the last received key (S3-style
                        // pagination is keyed on `start_after`, so no
                        // duplicates or gaps). Only reset to the caller's
                        // `start_after` when nothing has been received yet.
                        page_start_after = all_items
                            .last()
                            .map_or_else(|| start_after.to_vec(), |(k, _)| k.to_vec());
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
                    // Resume from the last received key on the (possibly new)
                    // endpoint — S3-style pagination is keyed on `start_after`,
                    // so no duplicates or gaps in key order. Only reset to the
                    // caller's `start_after` when nothing has been received yet.
                    page_start_after = all_items
                        .last()
                        .map_or_else(|| start_after.to_vec(), |(k, _)| k.to_vec());
                    attempts = self.count_other(attempts, &status.to_string())?;
                }
            }
        }
    }

    /// Count the live keys matching `prefix` (empty = whole keyspace) in a
    /// group. A single `count_only` RPC asks the server to count all matching
    /// keys in one pass (no value materialization, no items shipped — only the
    /// count crosses the network). `start_after`/`end_key` bound the counted
    /// range like [`Self::scan`]. No pagination: the server counts the whole
    /// range in one response. `limit` (`0` = count all) caps the count; when
    /// it is reached the result is exact up to `limit` (the server does not
    /// distinguish "exactly N" from "N or more" in that case — pass `limit =
    /// 0` for a true total).
    ///
    /// # Errors
    /// See [`Error`].
    #[allow(clippy::too_many_arguments)]
    pub async fn scan_count(
        &self,
        store_id: u64,
        group_id: u64,
        prefix: &[u8],
        start_after: &[u8],
        end_key: &[u8],
        limit: u32,
        read_mode: ReadMode,
        min_slot: Option<u64>,
        deadline: Option<u64>,
    ) -> Result<u64> {
        let min_slot = self.resolve_min_slot(store_id, group_id, read_mode, min_slot);
        let mut endpoint = self.resolve_read_endpoint(store_id, group_id, read_mode).await?;
        let mut attempts = 0u32;
        let mut backoff = self.retry.backoff_base;
        loop {
            let req = KvScanRequest {
                version: 1,
                prefix: Bytes::copy_from_slice(prefix),
                start_after: Bytes::copy_from_slice(start_after),
                end_key: Bytes::copy_from_slice(end_key),
                limit,
                request_id: next_request_id(),
                request_create_ms: now_ms(),
                group_id,
                read_mode: read_mode as i32,
                min_slot,
                keys_only: false,
                count_only: true,
                deadline_ms: deadline.unwrap_or(0),
            };
            let channel = self.pool.get(&endpoint)?;
            let t0 = Instant::now();
            let _in_flight = self.incr_in_flight(&endpoint);
            match KvServiceClient::new(channel).scan(req).await {
                Ok(resp) => {
                    let resp = resp.into_inner();
                    self.record_endpoint_rtt(&endpoint, t0.elapsed().as_micros() as u64);
                    if resp.ok {
                        self.metrics.record_scan_latency(t0.elapsed().as_micros() as u64);
                        return Ok(resp.count);
                    }
                    self.metrics.record_scan_error();
                    if !resp.not_leader_hint.is_empty() {
                        if read_mode == ReadMode::MinSlot && self.read_endpoint_policy.is_distributed() {
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

    /// Slot-ordered scan over the chosen log. Returns individual KV ops
    /// (Put / Delete) in commit (slot) order within `[min_slot,
    /// max_slot]` (`max_slot = 0` means "up to the current applied
    /// frontier"), filtered by `key_prefix` (empty = all keys). Used by
    /// diskdb strategy 2 (journal scan replay) — [`Self::scan`]
    /// returns key order, not slot order, so it cannot drive a correct
    /// replay.
    ///
    /// Transparent pagination: sends the first request, if `truncated`
    /// resends with `min_slot = last_op_slot + 1`, repeats until all
    /// ops in the range are collected or the caller's `limit` is
    /// reached. `limit = 0` means "no caller limit" (still pages via
    /// the server's per-page `page_limit`). Returns the full op list
    /// in slot order.
    ///
    /// # Errors
    /// See [`Error`]. A server `KV_ERROR_JOURNAL_SCAN_GC_GAP` (asked
    /// for slots already GC'd below the WAL trim point) is surfaced as
    /// [`Error::Server`] — the caller (diskdb recovery) falls back to
    /// a full-scan rebuild.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    pub async fn journal_scan(
        &self,
        store_id: u64,
        group_id: u64,
        min_slot: u64,
        max_slot: u64,
        key_prefix: &[u8],
        limit: u32,
        page_limit: u32,
        read_mode: ReadMode,
        deadline: Option<u64>,
    ) -> Result<JournalScanOutcome> {
        let min_slot_floor = self.resolve_min_slot(store_id, group_id, read_mode, None);
        let mut endpoint = self.resolve_read_endpoint(store_id, group_id, read_mode).await?;
        let mut attempts = 0u32;
        let mut backoff = self.retry.backoff_base;
        let mut all_ops: Vec<JournalOp> = Vec::new();
        let mut page_min_slot = min_slot.max(min_slot_floor);
        let mut page1_read_slot: Option<u64> = None;
        loop {
            let remaining_page_limit = if page_limit == 0 { 0 } else { page_limit };
            let req = KvJournalScanRequest {
                version: 1,
                group_id,
                min_slot: page_min_slot,
                max_slot,
                key_prefix: Bytes::copy_from_slice(key_prefix),
                limit: remaining_page_limit,
                request_id: next_request_id(),
                request_create_ms: now_ms(),
                read_mode: read_mode as i32,
            };
            let channel = self.pool.get(&endpoint)?;
            let t0 = Instant::now();
            let _in_flight = self.incr_in_flight(&endpoint);
            match KvServiceClient::new(channel).journal_scan(req).await {
                Ok(resp) => {
                    let resp = resp.into_inner();
                    self.record_endpoint_rtt(&endpoint, t0.elapsed().as_micros() as u64);
                    if resp.ok {
                        self.metrics.record_scan_latency(t0.elapsed().as_micros() as u64);
                        if page1_read_slot.is_none() {
                            page1_read_slot = Some(resp.read_slot);
                        }
                        let page_len = resp.ops.len();
                        let resp_truncated = resp.truncated;
                        for op in resp.ops {
                            all_ops.push(JournalOp {
                                key: op.key,
                                value: op.value,
                                is_delete: op.is_delete,
                                slot: op.slot,
                            });
                        }
                        // Caller's limit reached?
                        if limit != 0 && all_ops.len() >= limit as usize {
                            all_ops.truncate(limit as usize);
                            self.metrics.record_scan_latency(t0.elapsed().as_micros() as u64);
                            return Ok(JournalScanOutcome {
                                ops: all_ops,
                                truncated: true,
                                read_slot: page1_read_slot.unwrap_or(0),
                            });
                        }
                        // Server page truncated → fetch the next page
                        // from `last_op_slot + 1`. A zero-op truncated
                        // page is a safety stop (avoid infinite loop).
                        if resp_truncated && page_len > 0 {
                            let server_timed_out = deadline.is_some_and(|dl| now_ms() >= dl);
                            if server_timed_out {
                                return Ok(JournalScanOutcome {
                                    ops: all_ops,
                                    truncated: true,
                                    read_slot: page1_read_slot.unwrap_or(0),
                                });
                            }
                            page_min_slot = resp.last_op_slot.saturating_add(1);
                            continue;
                        }
                        return Ok(JournalScanOutcome {
                            ops: all_ops,
                            truncated: false,
                            read_slot: page1_read_slot.unwrap_or(0),
                        });
                    }
                    self.metrics.record_scan_error();
                    if !resp.not_leader_hint.is_empty() {
                        if read_mode == ReadMode::MinSlot && self.read_endpoint_policy.is_distributed() {
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
    pub(crate) fn resolve_min_slot(
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
#[must_use]
pub fn new_client_id() -> u64 {
    next_request_id()
}
