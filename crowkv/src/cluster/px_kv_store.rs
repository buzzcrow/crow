// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#![allow(clippy::cast_possible_truncation)]

use crate::cluster::group::{ProposeResult, PxGroup};
use crate::cluster::group_election::{LeaderElection, ReadBarrierOutcome};
use crate::cluster::kv_server::GrpcTaskState;
use crate::cluster::kv_store::KvStore;
use crate::cluster::status::{StatusLevel, StoreStatus};
use crate::common::optional_u64;
use crate::common::report::OperationReport;
use crate::metrics::MetricsRegistry;
use crate::rpc::ReadMode;
use dashmap::DashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use tracing::{debug, info};

pub struct PxKvStore {
    pub store_id: u64,
    pub(crate) groups: DashMap<u64, Arc<PxGroup>>,
    pub(crate) server_state: Mutex<GrpcTaskState>,
    pub(crate) listen_addr: SocketAddr,
    /// Set the first time `shutdown()` is invoked. Subsequent calls are no-ops.
    shutdown_started: AtomicBool,
    /// Metrics registry for KV service instrumentation. `None` when
    /// metrics are disabled (`--metrics-interval 0`).
    pub(crate) metrics_registry: Option<Arc<std::sync::Mutex<MetricsRegistry>>>,
}

impl KvStore for PxKvStore {
    async fn kv_get(
        &self,
        group_id: u64,
        key: &[u8],
        read_mode: i32,
        min_slot: u64,
        request_id: u64,
        request_create_ms: u64,
    ) -> crate::rpc::KvResponse {
        let Some(group) = self.get_group(group_id) else {
            return missing_group_response(request_id, request_create_ms);
        };

        match self.resolve_read_point(&group, read_mode, min_slot).await {
            ReadDecision::Serve { read_slot, safe_slot } => {
                let value = group.local_replica().learner.engine_get_bytes(key).await;
                match value {
                    Some((slot, v)) => {
                        crate::rpc::KvResponse::ok_value_with_revision(v, slot, request_id, request_create_ms)
                            .with_read_slots(read_slot, safe_slot)
                    }
                    None => crate::rpc::KvResponse::not_found(request_id, request_create_ms)
                        .with_read_slots(read_slot, safe_slot),
                }
            }
            ReadDecision::NotLeader { hint } => {
                crate::rpc::KvResponse::not_leader(hint, request_id, request_create_ms)
            }
            ReadDecision::Unavailable { msg } => {
                crate::rpc::KvResponse::err(msg, request_id, request_create_ms)
            }
        }
    }

    async fn kv_put(
        &self,
        group_id: u64,
        key: &[u8],
        value: &[u8],
        client_id: u64,
        seq: u64,
        request_id: u64,
        request_create_ms: u64,
    ) -> crate::rpc::KvResponse {
        let payload = Self::encode_kv_payload(&[(key, Some(value))]);
        self.propose_and_respond(
            group_id,
            payload,
            optional_u64(client_id),
            Some(seq),
            request_id,
            request_create_ms,
        )
        .await
    }
    async fn kv_delete(
        &self,
        group_id: u64,
        key: &[u8],
        client_id: u64,
        seq: u64,
        request_id: u64,
        request_create_ms: u64,
    ) -> crate::rpc::KvResponse {
        let payload = Self::encode_kv_payload(&[(key, None)]);
        self.propose_and_respond(
            group_id,
            payload,
            optional_u64(client_id),
            Some(seq),
            request_id,
            request_create_ms,
        )
        .await
    }
    async fn kv_batch_write(
        &self,
        group_id: u64,
        items: Vec<crate::rpc::KvBatchItem>,
        client_id: u64,
        seq: u64,
        request_id: u64,
        request_create_ms: u64,
    ) -> crate::rpc::KvResponse {
        let payload = Self::encode_kv_batch_items(&items);
        self.propose_and_respond(
            group_id,
            payload,
            optional_u64(client_id),
            Some(seq),
            request_id,
            request_create_ms,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn kv_scan(
        &self,
        group_id: u64,
        prefix: &[u8],
        start_after: &[u8],
        limit: u32,
        read_mode: i32,
        min_slot: u64,
        request_id: u64,
        request_create_ms: u64,
    ) -> crate::rpc::KvScanResponse {
        let Some(group) = self.get_group(group_id) else {
            return scan_err(
                format!("group {group_id} not found in store {}", self.store_id),
                request_id,
                request_create_ms,
            );
        };

        // Scans pass `min_slot` through to the read resolver; for
        // linearizable scans it is ignored, for MinSlot scans it sets
        // the freshness floor.
        let read_slot = match self.resolve_read_point(&group, read_mode, min_slot).await {
            ReadDecision::Serve { read_slot, .. } => read_slot,
            ReadDecision::NotLeader { hint } => {
                return scan_err(
                    format!("not leader; retry scan at {hint}"),
                    request_id,
                    request_create_ms,
                );
            }
            ReadDecision::Unavailable { msg } => {
                return scan_err(msg, request_id, request_create_ms);
            }
        };

        // Ordered prefix scan from the engine. The engine returns the
        // `limit` smallest matching live keys (no tombstones) in key order
        // plus a `truncated` flag; `limit == 0` means unlimited. Sorted
        // output keeps pagination via `prefix` extension predictable.
        let (scanned, truncated) = group
            .local_replica()
            .learner
            .engine_scan(prefix, start_after, limit as usize)
            .await;
        let mut items: Vec<crate::rpc::KvScanItem> = Vec::with_capacity(scanned.len());
        for (key, _slot, value) in scanned {
            items.push(crate::rpc::KvScanItem {
                key: bytes::Bytes::from(key),
                value: bytes::Bytes::from(value),
            });
        }

        debug!(
            store_id = self.store_id,
            group_id,
            prefix_len = prefix.len(),
            limit,
            returned = items.len(),
            truncated,
            "kv_scan local-replica read"
        );

        crate::rpc::KvScanResponse {
            version: 1,
            ok: true,
            error: String::new(),
            truncated,
            items,
            request_id,
            request_create_ms,
            read_slot,
        }
    }
}

impl PxKvStore {
    #[must_use]
    pub fn new(store_id: u64, listen_addr: SocketAddr) -> Self {
        Self {
            store_id,
            groups: DashMap::new(),
            server_state: Mutex::new(GrpcTaskState::default()),
            listen_addr,
            shutdown_started: AtomicBool::new(false),
            metrics_registry: None,
        }
    }

    /// Attach a metrics registry so the KV service can register and
    /// record metrics. Called before `start()`.
    pub fn set_metrics_registry(&mut self, registry: Arc<std::sync::Mutex<MetricsRegistry>>) {
        self.metrics_registry = Some(registry);
    }

    /// Cascade shutdown: stop gRPC server (with timeout), then shut down each
    /// group, cascading into every replica layer.
    ///
    /// The shutdown contract across layers (`PxKvStore` → `PxGroup` →
    /// `PxLocalReplica` / `PxRemoteReplica` → `acceptor` / `learner` / `slot_list`
    /// / `kv_store`) is:
    ///
    /// 1. Stops accepting new work for that layer.
    /// 2. Cascades into children, **continuing on errors** (never aborts the chain).
    /// 3. Force-cleans the resource it owns (abort task, close channel, drain
    ///    retired pointers, …) when graceful join times out.
    /// 4. Returns an [`OperationReport`](crate::common::report::OperationReport)
    ///    with aggregated `critical:` errors.
    ///
    /// Calls are **idempotent** — second and later calls return an empty clean
    /// report and log at `debug`. Layers are responsible for their own
    /// `AtomicBool` "already-shutdown" gate.
    ///
    /// ## Why this shape
    ///
    /// - Caller decides what to do with errors (retry, surface to operator, panic
    ///   in tests). Mirrors how Rust idiomatic shutdown is usually expressed via
    ///   `Result`-aggregation.
    /// - Per-layer timeout guarantees the chain returns even if a child hangs;
    ///   the timed-out layer is force-cleaned and a `critical:` line tells the
    ///   operator which resource leaked.
    /// - Sub-shutdowns are awaited (not spawned) so the report accurately
    ///   reflects the state of every owned resource at return time.
    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(store_id = self.store_id, timeout_ms = per_layer_timeout.as_millis() as u64)
    )]
    pub async fn shutdown(&self, per_layer_timeout: Duration) -> OperationReport {
        // Idempotency gate: only the first caller proceeds.
        if self.shutdown_started.swap(true, Ordering::AcqRel) {
            debug!(
                store_id = self.store_id,
                "PxKvStore::shutdown is a no-op (already shut down)"
            );
            return OperationReport::new();
        }

        info!(
            store_id = self.store_id,
            group_count = self.groups.len(),
            timeout_ms = per_layer_timeout.as_millis() as u64,
            "PxKvStore shutdown starting"
        );

        let mut report = OperationReport::new();

        // 1. Stop gRPC server first so no new requests reach the groups.
        if let Err(msg) = self.shutdown_server(per_layer_timeout).await {
            report.push_error(msg);
        }

        // 2. Cascade into each group. Continue on errors.
        for entry in &self.groups {
            let group_id = *entry.key();
            let group = entry.value();
            info!(store_id = self.store_id, group_id, "shutting down PxGroup");
            let sub = group.shutdown(per_layer_timeout).await;
            if !sub.is_clean() {
                debug!(
                    store_id = self.store_id,
                    group_id,
                    error_count = sub.errors.len(),
                    "PxGroup shutdown reported errors"
                );
            }
            report.merge(sub);
        }

        if report.is_clean() {
            info!(store_id = self.store_id, "PxKvStore shutdown complete");
        } else {
            info!(
                store_id = self.store_id,
                error_count = report.errors.len(),
                "PxKvStore shutdown complete with errors (see critical: logs above)"
            );
        }
        report
    }

    /// Hierarchical point-in-time status for `/topology` and `/health`.
    /// Composes group statuses from cached state (no RPC).
    #[must_use]
    pub fn status(&self) -> StoreStatus {
        let mut status = StatusLevel::Ok;
        let mut messages = Vec::new();

        if self.shutdown_started.load(Ordering::Acquire) {
            status = StatusLevel::Unhealthy;
            messages.push(format!("store {} has been shut down", self.store_id));
        } else {
            // gRPC server liveness — listen_addr is set by start() and cleared by
            // shutdown_server() taking the JoinHandle.
            let server_running = self.server_state.lock().handle.is_some();
            if !server_running {
                status = StatusLevel::Unhealthy;
                messages.push(format!("store {}: gRPC server not running", self.store_id));
            }
        }

        let groups = self
            .groups
            .iter()
            .map(|entry| {
                let group_id = *entry.key();
                let group = entry.value().status();
                status = StatusLevel::worst(status, group.status);
                messages.extend(
                    group
                        .messages
                        .iter()
                        .map(|msg| format!("group#{group_id}: {msg}")),
                );
                group
            })
            .collect();

        StoreStatus {
            store_id: self.store_id,
            listen_addr: self.server_state.lock().listen_addr.map(|a| a.to_string()),
            status,
            messages,
            groups,
        }
    }

    pub fn add_group(&self, group: PxGroup) {
        self.add_group_inner(group, true);
    }

    /// Add a group **without** spawning its election driver.
    ///
    /// Used by the restore / multi-replica-creation orchestration: a group
    /// must not self-elect leader at `quorum == 1` (no remotes wired yet) and
    /// then run `bulk_phase1` / `repair_once` against only itself, which can
    /// `NoOp`-fill or finalize a committed slot the node is personally missing
    /// and thereby **erase** committed data. The
    /// caller wires the full configured membership first; the subsequent
    /// remote-wiring rebuild (`add_remote_replicas` → [`Self::add_group`])
    /// starts the driver with a correct quorum.
    pub fn add_group_without_election(&self, group: PxGroup) {
        self.add_group_inner(group, false);
    }

    fn add_group_inner(&self, group: PxGroup, spawn_driver: bool) {
        let group_id = group.group_id;
        let mut group = group;
        if let Some(prior) = self.groups.get(&group_id) {
            group.inherit_local_state_from(prior.value());
        }
        // Set the local replica's endpoint from the store's actual bound
        // address (if the server is running) or the configured listen addr,
        // so persist_config writes the correct endpoint for all nodes.
        let endpoint = self
            .server_state
            .lock()
            .listen_addr
            .map_or_else(|| self.listen_addr.to_string(), |a| a.to_string());
        group.local_replica().set_endpoint(endpoint);
        info!(
            store_id = self.store_id,
            group_id,
            replicas = group.remote_replica_count(),
            spawn_driver,
            "added group to kv store"
        );
        let arc = Arc::new(group);
        // Wire metrics registry into local + remote replicas when available.
        if let Some(ref registry) = self.metrics_registry {
            arc.set_metrics_registry(registry, self.store_id);
        }
        // Spawn the per-group election driver (no-op when
        // `election_driver_disabled`). Driver holds a `Weak<PxGroup>` so
        // dropping the store's `Arc` does not leak the task. Skip when no
        // tokio runtime is active (structural / non-async unit tests), or when
        // the caller deferred the driver (`spawn_driver == false`).
        if spawn_driver && tokio::runtime::Handle::try_current().is_ok() {
            let arc_for_spawn = arc.clone();
            tokio::spawn(async move {
                arc_for_spawn.start_election_loop().await;
            });
            let arc_for_maintenance = arc.clone();
            tokio::spawn(async move {
                arc_for_maintenance.start_engine_maintenance_loop().await;
            });
        }
        // Atomically replace any prior group entry with the new arc and
        // cancel the prior group's driver synchronously. Without the
        // synchronous cancel, the old driver keeps running until its
        // next loop iteration discovers `Weak::upgrade` failed, which
        // creates a window where two drivers (old and new) race for
        // leadership of the same `(store_id, group_id)`. Common path:
        // `add_store` lands the group with 0 remotes, the old driver
        // self-elects leader at `quorum=1`, then `add_remote_replicas` rebuilds
        // and the new driver re-elects, producing split-brain at
        // `term=1` until both drivers eventually step down via
        // heartbeats and the cluster re-races.
        if let Some(old_arc) = self.groups.insert(group_id, arc) {
            old_arc.tenure_cancel().cancel();
        }
    }

    pub fn get_group(&self, group_id: u64) -> Option<Arc<PxGroup>> {
        self.groups.get(&group_id).map(|r| r.clone())
    }

    /// Decide whether a KV read on `group_id` should be forwarded to the
    /// group's leader. Returns `Some(endpoint)` only when **all** of:
    ///
    /// * the group exists locally,
    /// * the local replica is **not** the current leader, and
    /// * the leader's gRPC endpoint is known (one of the group's
    ///   `remote_replicas` carries it).
    ///
    /// Returns `None` when local is the leader, the group is missing,
    /// or the leader endpoint is unknown. In those cases callers serve
    /// the read from the local learner store as a best-effort fallback.
    /// Used by `KvStoreService::{get, scan}` for transparent
    /// leader-forwarding of reads.
    #[must_use]
    pub fn forward_target_for(&self, group_id: u64) -> Option<String> {
        let group = self.get_group(group_id)?;
        group.leader_endpoint()
    }

    /// Resolve the consistency discipline for a read on `group` and, when the
    /// read may be served locally, the slot it is served at. Mirrors
    /// the consistency model:
    ///
    /// * **Linearizable** — on the leader, run the lease / `ReadIndex` barrier
    ///   ([`PxGroup::linearizable_read_barrier`]). A non-leader (or a leader
    ///   that loses the barrier) must **never** serve local state: doing so
    ///   would silently return a stale value for a read the caller asked to be
    ///   fresh. Instead it redirects (`NotLeader`) so the caller retries at the
    ///   real leader, or fails (`Unavailable`) when no quorum can confirm
    ///   freshness.
    /// * **`MinSlot`** — serve locally once the applied frontier has caught up
    ///   to `min_slot`; otherwise point the client at the leader. `min_slot = 0`
    ///   accepts any staleness.
    async fn resolve_read_point(&self, group: &Arc<PxGroup>, read_mode: i32, min_slot: u64) -> ReadDecision {
        let replica = group.local_replica();
        let safe_slot = group.group_safe_slot();
        let mode = ReadMode::try_from(read_mode).unwrap_or(ReadMode::Linearizable);
        match mode {
            ReadMode::Linearizable => {
                if replica.is_leader() {
                    match group.linearizable_read_barrier().await {
                        ReadBarrierOutcome::Ready { read_slot } => {
                            ReadDecision::Serve { read_slot, safe_slot }
                        }
                        // Lost leadership during the barrier: redirect to the
                        // current leader rather than serving stale local state.
                        ReadBarrierOutcome::NotLeader => ReadDecision::NotLeader {
                            hint: self.forward_target_for(group.group_id()).unwrap_or_default(),
                        },
                        ReadBarrierOutcome::NoQuorum => ReadDecision::Unavailable {
                            msg: "linearizable read: leadership quorum unavailable".to_string(),
                        },
                    }
                } else {
                    // Non-leader: a linearizable read cannot be proven fresh
                    // here. `kv_service` forwards linearizable reads to the
                    // leader before reaching the store; arriving here means
                    // forwarding was unavailable or the loop-guard is set.
                    // Redirect instead of serving a stale local value.
                    ReadDecision::NotLeader {
                        hint: self.forward_target_for(group.group_id()).unwrap_or_default(),
                    }
                }
            }
            ReadMode::MinSlot => {
                if replica.contiguous_applied() >= min_slot {
                    ReadDecision::Serve {
                        read_slot: replica.contiguous_applied(),
                        safe_slot,
                    }
                } else {
                    ReadDecision::NotLeader {
                        hint: self.forward_target_for(group.group_id()).unwrap_or_default(),
                    }
                }
            }
        }
    }

    pub fn remove_group(&self, group_id: u64) -> bool {
        // Cancel the removed group's per-tenure token so its election
        // driver (and, if it is the leader, its heartbeat loop) stops.
        // Dropping the `DashMap` entry alone is not enough: the running
        // `run_leader_state` / `run_election_driver` task holds its own
        // strong `Arc<PxGroup>` for the duration of the tenure, so the
        // group is not dropped and a removed leader would keep sending
        // heartbeats forever — starving the surviving replicas' election
        // deadline so they can never re-elect. Mirror `add_group`'s
        // synchronous cancel on replacement.
        if let Some((_, group)) = self.groups.remove(&group_id) {
            group.tenure_cancel().cancel();
            true
        } else {
            false
        }
    }

    pub fn group_count(&self) -> usize {
        self.groups.len()
    }

    /// Iterate all groups, calling `f` with each `Arc<PxGroup>`.
    /// Used by the engine stats collector to poll crowtree counters.
    pub fn for_each_group<F>(&self, mut f: F)
    where
        F: FnMut(&Arc<PxGroup>),
    {
        for entry in &self.groups {
            f(entry.value());
        }
    }

    /// Return `(group_id, local_replica_id, leader_id, remote_count)` for all groups.
    pub fn group_summaries(&self) -> Vec<(u64, u64, u64, usize)> {
        self.groups
            .iter()
            .map(|entry| {
                let group = entry.value();
                (
                    group.group_id,
                    group.local_replica().id,
                    group.leader_id(),
                    group.remote_replica_info().len(),
                )
            })
            .collect()
    }

    // ── KV operations ─────────────────────────────────────────

    async fn propose_and_respond(
        &self,
        group_id: u64,
        payload: Vec<u8>,
        client_id: Option<u64>,
        seq: Option<u64>,
        request_id: u64,
        request_create_ms: u64,
    ) -> crate::rpc::KvResponse {
        let Some(group) = self.get_group(group_id) else {
            return missing_group_response(request_id, request_create_ms);
        };

        match group.propose(payload, client_id, seq).await {
            ProposeResult::Chosen { slot } => {
                crate::rpc::KvResponse::ok_chosen(slot, request_id, request_create_ms)
            }
            ProposeResult::NotLeader { leader_hint } => {
                crate::rpc::KvResponse::not_leader(leader_hint, request_id, request_create_ms)
            }
            // Window-full: surface a retryable error keyword so clients back
            // off and retry rather than treating it as a hard failure.
            ProposeResult::Busy => crate::rpc::KvResponse::err(
                crate::paxos::error::PxPaxosError::Busy.keyword().to_string(),
                request_id,
                request_create_ms,
            ),
            ProposeResult::Err(msg) => crate::rpc::KvResponse::err(msg, request_id, request_create_ms),
        }
    }

    // ── KV payload encoding ───────────────────────────────────

    fn encode_kv_payload(ops: &[(&[u8], Option<&[u8]>)]) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.push(ops.len() as u8);
        for (key, value_opt) in ops {
            buf.push(u8::from(value_opt.is_none()));
            buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
            buf.extend_from_slice(key);
            let value_len = value_opt.map_or(0, <[u8]>::len) as u32;
            buf.extend_from_slice(&value_len.to_le_bytes());
            if let Some(value) = value_opt {
                buf.extend_from_slice(value);
            }
        }
        buf
    }

    fn encode_kv_batch_items(items: &[crate::rpc::KvBatchItem]) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.push(items.len() as u8);
        for item in items {
            buf.push(u8::from(item.is_delete));
            buf.extend_from_slice(&(item.key.len() as u32).to_le_bytes());
            buf.extend_from_slice(&item.key);
            let value_len = if item.is_delete {
                0
            } else {
                item.value.len() as u32
            };
            buf.extend_from_slice(&value_len.to_le_bytes());
            if !item.is_delete {
                buf.extend_from_slice(&item.value);
            }
        }
        buf
    }
}

fn missing_group_response(request_id: u64, request_create_ms: u64) -> crate::rpc::KvResponse {
    crate::rpc::KvResponse::err(
        "no kv group configured for request group_id".to_string(),
        request_id,
        request_create_ms,
    )
}

/// Build a failed [`crate::rpc::KvScanResponse`] carrying `error`. Scans have
/// no dedicated `not_leader_hint`/`not_found` channel, so all failure shapes
/// collapse to `ok = false` with a descriptive message.
fn scan_err(error: String, request_id: u64, request_create_ms: u64) -> crate::rpc::KvScanResponse {
    crate::rpc::KvScanResponse {
        version: 1,
        ok: false,
        error,
        truncated: false,
        items: Vec::new(),
        request_id,
        request_create_ms,
        read_slot: 0,
    }
}

/// Outcome of [`PxKvStore::resolve_read_point`]: whether a read may be served
/// from local state (and at which slots) or must be redirected / failed.
enum ReadDecision {
    /// Serve from local applied state. `read_slot` is the serving frontier;
    /// `safe_slot` is the group safe-slot for bounded-stale reporting.
    Serve { read_slot: u64, safe_slot: u64 },
    /// Redirect the client to the leader (`hint` may be empty if unknown).
    NotLeader { hint: String },
    /// The read cannot currently be served with the requested consistency.
    Unavailable { msg: String },
}
