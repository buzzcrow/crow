// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `PxRemoteReplica` — crowdb-rpc adapter for a peer replica in a Paxos group.
//!
//! Wraps a shared `PxRpcTransport` (the only RPC path) and a small layer
//! of cached status/metrics state. Exposes `ReplicaClient` so callers
//! talk to peers through a uniform RPC surface.
//!
//! Key work: Prepare/Accept/PreVote/RequestVote/Heartbeat/StepDown RPC
//! bridges over crowdb-rpc, fire-and-forget `ChosenNotification` /
//! `BatchChosenNotification`, `FetchGap`, cached status snapshots for the
//! management API.

use crate::cluster::replica::{
    FetchGapReply, HeartbeatReply, HeartbeatRequestPayload, PxReplicaError, Replica, ReplicaClient,
    StepDownReply, StepDownRequestPayload, VoteReply, VoteRequestPayload,
};
use crate::cluster::status::{RemoteStatus, StatusLevel};
use crate::common::config::PxElectionConfig;
use crate::common::metrics::LayerMetrics;
use crate::common::report::OperationReport;
use crate::metrics::{Counter, LatencySummary, MetricsRegistry};
use crate::paxos::roles::{DedupTag, PxAcceptReply, PxBallot, PxLogEntry, PxPrepareReply};
use crate::paxos::PxNodeId;
use crate::rpc::PxRpcTransport;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use tracing::{debug, info};

#[derive(Debug)]
pub struct PxRemoteReplica {
    pub(crate) node_id: PxNodeId,
    pub(crate) endpoint: String,
    /// Per-RPC deadline for the crowdb-rpc unary calls (`Prepare`, `Accept`,
    /// `PreVote`, `RequestVote`, `Heartbeat`, `StepDown`, `FetchGap`). Snapshot of
    /// `PxElectionConfig::learner_stream_rpc_timeout_ms`.
    rpc_timeout: Duration,
    pub(crate) voting: bool,
    /// Per-remote RPC counters consumed by `/topology`.
    metrics: LayerMetrics,
    /// Optional registry handles mirroring RPC stats to the metrics log.
    /// Set via [`Self::set_metrics_registry`] when a registry is wired.
    rpc_handles: OnceLock<RpcRegistryHandles>,
    /// Idempotency gate for [`Self::shutdown`].
    shutdown_started: AtomicBool,
    /// crowdb-rpc transport — the only RPC path. Set via
    /// [`Self::with_rpc_transport`] or [`Self::set_rpc_transport`] when
    /// the store wires the shared transport. All Prepare/Accept/PreVote/
    /// RequestVote/Heartbeat/`StepDown`/`ChosenNotification`/
    /// `BatchChosenNotification`/`FetchGap` calls route through it.
    rpc_transport: OnceLock<Arc<PxRpcTransport>>,
}

/// Registry-based metric handles for per-peer RPC stats.
#[derive(Debug)]
pub(crate) struct RpcRegistryHandles {
    pub(crate) latency: Arc<LatencySummary>,
    pub(crate) errors: Arc<Counter>,
}

impl Replica for PxRemoteReplica {
    fn id(&self) -> u64 {
        self.node_id
    }

    fn endpoint(&self) -> Option<&str> {
        Some(&self.endpoint)
    }

    fn voting(&self) -> bool {
        self.voting
    }
}

impl ReplicaClient for PxRemoteReplica {
    async fn send_prepare(
        &self,
        slot: u64,
        ballot: PxBallot,
        term: crate::paxos::PxTerm,
        group_id: u64,
        membership_epoch: u64,
    ) -> Result<PxPrepareReply, PxReplicaError> {
        let transport = self.transport_or_err()?;
        let started = Instant::now();
        let result = tokio::time::timeout(
            self.rpc_timeout,
            transport.send_prepare(&self.endpoint, slot, ballot, term, group_id, membership_epoch),
        )
        .await;
        self.finish_rpc(started, "prepare", result)
    }

    async fn send_accept(
        &self,
        entry: &PxLogEntry,
        dedup_tags: &[DedupTag],
        group_id: u64,
        membership_epoch: u64,
    ) -> Result<PxAcceptReply, PxReplicaError> {
        let transport = self.transport_or_err()?;
        let started = Instant::now();
        let result = tokio::time::timeout(
            self.rpc_timeout,
            transport.send_accept(&self.endpoint, entry, dedup_tags, group_id, membership_epoch),
        )
        .await;
        self.finish_rpc(started, "accept", result)
    }

    async fn send_pre_vote(
        &self,
        req: VoteRequestPayload,
        group_id: u64,
    ) -> Result<VoteReply, PxReplicaError> {
        let transport = self.transport_or_err()?;
        let started = Instant::now();
        let result = tokio::time::timeout(
            self.rpc_timeout,
            transport.send_pre_vote(&self.endpoint, req, group_id),
        )
        .await;
        self.finish_rpc(started, "pre_vote", result)
    }

    async fn send_request_vote(
        &self,
        req: VoteRequestPayload,
        group_id: u64,
    ) -> Result<VoteReply, PxReplicaError> {
        let transport = self.transport_or_err()?;
        let started = Instant::now();
        let result = tokio::time::timeout(
            self.rpc_timeout,
            transport.send_request_vote(&self.endpoint, req, group_id),
        )
        .await;
        self.finish_rpc(started, "request_vote", result)
    }

    async fn send_heartbeat(
        &self,
        req: HeartbeatRequestPayload,
        group_id: u64,
    ) -> Result<HeartbeatReply, PxReplicaError> {
        let transport = self.transport_or_err()?;
        let started = Instant::now();
        let result = tokio::time::timeout(
            self.rpc_timeout,
            transport.send_heartbeat(&self.endpoint, req, group_id),
        )
        .await;
        self.finish_rpc(started, "heartbeat", result)
    }

    async fn send_step_down(
        &self,
        req: &StepDownRequestPayload,
        group_id: u64,
    ) -> Result<StepDownReply, PxReplicaError> {
        let transport = self.transport_or_err()?;
        let started = Instant::now();
        let result = tokio::time::timeout(
            self.rpc_timeout,
            transport.send_step_down(&self.endpoint, req, group_id),
        )
        .await;
        self.finish_rpc(started, "step_down", result)
    }
}

impl PxRemoteReplica {
    #[must_use]
    pub fn new(node_id: PxNodeId, endpoint: String) -> Self {
        Self {
            node_id,
            endpoint,
            rpc_timeout: Duration::from_millis(PxElectionConfig::DEFAULT.learner_stream_rpc_timeout_ms),
            voting: true,
            metrics: LayerMetrics::new(),
            rpc_handles: OnceLock::new(),
            shutdown_started: AtomicBool::new(false),
            rpc_transport: OnceLock::new(),
        }
    }

    /// Construct a remote replica with the given election config snapshot.
    /// Consumes `learner_stream_rpc_timeout_ms` (per-RPC deadline); other
    /// fields stay configurable per-call.
    #[must_use]
    #[allow(dead_code)]
    pub(crate) fn with_config(node_id: PxNodeId, endpoint: String, cfg: &PxElectionConfig) -> Self {
        let mut r = Self::new(node_id, endpoint);
        r.rpc_timeout = Duration::from_millis(cfg.learner_stream_rpc_timeout_ms);
        r
    }

    /// Enable crowdb-rpc transport for all RPCs to this peer. The transport
    /// is shared across all remote replicas in the store.
    #[must_use]
    pub fn with_rpc_transport(self, transport: Arc<PxRpcTransport>) -> Self {
        let _ = self.rpc_transport.set(transport);
        self
    }

    /// Set the crowdb-rpc transport on an already-constructed replica.
    /// Used by the server after `start_rpc_server` to wire the shared
    /// transport into existing remote replicas.
    pub fn set_rpc_transport(&self, transport: Arc<PxRpcTransport>) {
        let _ = self.rpc_transport.set(transport);
    }

    /// Fire-and-forget peer notification that `(slot, term)` is chosen
    /// in `group_id`. Sent via crowdb-rpc with no reply awaited; failures
    /// (peer down, transport error) are returned for caller-side
    /// observability but the proposer treats them as best-effort and
    /// swallows them since the next heartbeat re-converges frontiers
    /// anyway.
    ///
    /// # Errors
    /// Returns [`PxReplicaError::Internal`] if the transport is not set
    /// or the fire-and-forget send fails.
    pub fn send_chosen_notice(
        &self,
        slot: u64,
        term: crate::paxos::PxTerm,
        leader_id: PxNodeId,
        group_id: u64,
        ballot_round: u64,
    ) -> Result<(), PxReplicaError> {
        let transport = self.transport_or_err_sync()?;
        transport.send_chosen(&self.endpoint, group_id, slot, term, leader_id, ballot_round)
    }

    /// R63: fire-and-forget batch chosen notice covering `[start_slot,
    /// end_slot]`. Sent via crowdb-rpc before the full-accept catch-up loop
    /// so the follower's chosen frontier advances for present slots
    /// without waiting for the full-accept round-trip.
    ///
    /// # Errors
    /// Returns [`PxReplicaError::Internal`] if the transport is not set
    /// or the fire-and-forget send fails.
    #[allow(dead_code)]
    pub(crate) fn send_batch_chosen_notice(
        &self,
        start_slot: u64,
        end_slot: u64,
        term: crate::paxos::PxTerm,
        leader_id: PxNodeId,
        group_id: u64,
        ballot_round: u64,
    ) -> Result<(), PxReplicaError> {
        let transport = self.transport_or_err_sync()?;
        transport.send_batch_chosen(
            &self.endpoint,
            group_id,
            start_slot,
            end_slot,
            term,
            leader_id,
            ballot_round,
        )
    }

    /// R65: Send a `FetchGapRequest` to the leader for a missing or stale
    /// slot. The leader replies with the chosen value + ballot. This is
    /// the follower-driven catch-up path — the follower detects a gap
    /// (`ChosenNotice` for a slot it doesn't have, or apply loop finds a
    /// missing slot in the committed range) and proactively fetches the
    /// value from the leader.
    ///
    /// # Errors
    /// Returns [`PxReplicaError`] on transport failure or timeout.
    #[allow(dead_code)]
    pub async fn send_fetch_gap(
        &self,
        slot: u64,
        term: crate::paxos::PxTerm,
        leader_id: PxNodeId,
        group_id: u64,
    ) -> Result<FetchGapReply, PxReplicaError> {
        let transport = self.transport_or_err()?;
        let started = Instant::now();
        let result = tokio::time::timeout(
            self.rpc_timeout,
            transport.send_fetch_gap(&self.endpoint, group_id, slot, term, leader_id),
        )
        .await;
        self.finish_rpc(started, "fetch_gap", result)
    }

    /// Get the crowdb-rpc transport, cloning the `Arc` for use in an async
    /// context. Records an error and returns `Err` if the transport was
    /// never wired (e.g. in unit tests that construct via `new`).
    fn transport_or_err(&self) -> Result<Arc<PxRpcTransport>, PxReplicaError> {
        if let Some(t) = self.rpc_transport.get() {
            Ok(t.clone())
        } else {
            self.record_err();
            Err(PxReplicaError::Internal(format!(
                "crowdb-rpc transport unavailable: not set for peer {} ({})",
                self.node_id, self.endpoint
            )))
        }
    }

    /// Sync variant of [`Self::transport_or_err`] for fire-and-forget
    /// methods (`send_chosen_notice`, `send_batch_chosen_notice`).
    /// Returns a borrowed `&Arc` so the caller can dispatch without
    /// cloning.
    fn transport_or_err_sync(&self) -> Result<&Arc<PxRpcTransport>, PxReplicaError> {
        if let Some(t) = self.rpc_transport.get() {
            Ok(t)
        } else {
            self.record_err();
            Err(PxReplicaError::Internal(format!(
                "crowdb-rpc transport unavailable: not set for peer {} ({})",
                self.node_id, self.endpoint
            )))
        }
    }

    /// Generic timeout/result handler shared by all unary RPCs. Records
    /// latency on success, an error on failure or timeout, and maps the
    /// `tokio::time::Elapsed` into a descriptive `PxReplicaError`.
    fn finish_rpc<T>(
        &self,
        started: Instant,
        rpc_name: &str,
        result: Result<Result<T, PxReplicaError>, tokio::time::error::Elapsed>,
    ) -> Result<T, PxReplicaError> {
        match result {
            Ok(Ok(reply)) => {
                self.record_ok(u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX));
                Ok(reply)
            }
            Ok(Err(e)) => {
                self.record_err();
                Err(e)
            }
            Err(_) => {
                self.record_err();
                Err(PxReplicaError::Internal(format!(
                    "{} rpc timeout after {} ms at peer {}",
                    rpc_name,
                    self.rpc_timeout.as_millis(),
                    self.endpoint
                )))
            }
        }
    }

    /// Record a successful RPC to both legacy and registry handles.
    fn record_ok(&self, rtt_ns: u64) {
        self.metrics.record_ok(rtt_ns);
        if let Some(h) = self.rpc_handles.get() {
            h.latency.observe(rtt_ns);
        }
    }

    /// Record a failed RPC to both legacy and registry handles.
    fn record_err(&self) {
        self.metrics.record_err();
        if let Some(h) = self.rpc_handles.get() {
            h.errors.inc();
        }
    }

    /// Get the crowdb-rpc transport if set. Used by `group_fetchgap.rs`
    /// to route `FetchGap` through the transport when available.
    pub fn rpc_transport(&self) -> Option<&Arc<PxRpcTransport>> {
        self.rpc_transport.get()
    }

    /// Get the endpoint string (for spawned tasks that need to call
    /// the transport directly).
    pub(crate) fn endpoint_str(&self) -> &str {
        &self.endpoint
    }

    /// Register per-peer RPC latency summary and error counter with the
    /// metrics registry. Called once during group creation when a
    /// registry is wired.
    ///
    /// # Panics
    ///
    /// Panics if the metrics registry mutex is poisoned.
    pub(crate) fn set_metrics_registry(
        &self,
        registry: &Arc<std::sync::Mutex<MetricsRegistry>>,
        store_id: u64,
        group_id: u64,
    ) {
        let mut r = registry.lock().expect("metrics registry poisoned");
        let prefix = format!("s.{store_id}.g.{group_id}");
        let peer = self.node_id;
        let handles = RpcRegistryHandles {
            latency: r.register_summary(format!("{prefix}.rpc.l@{peer}")),
            errors: r.register_counter(format!("{prefix}.rpc.errors.c@{peer}")),
        };
        let _ = self.rpc_handles.set(handles);
    }

    /// Read this remote's RPC metrics for the topology endpoint.
    #[must_use]
    pub(crate) fn status(&self) -> RemoteStatus {
        let mut status = StatusLevel::Ok;
        let mut messages = Vec::new();
        if self.rpc_transport.get().is_none() {
            status = StatusLevel::Degraded;
            messages.push(format!(
                "remote {} ({}): crowdb-rpc transport not wired",
                self.node_id, self.endpoint
            ));
        }
        RemoteStatus {
            id: self.node_id,
            endpoint: self.endpoint.clone(),
            voting: self.voting,
            status,
            messages,
            metrics: self.metrics.snapshot(),
        }
    }

    /// Cascade shutdown: stop the legacy `PxLearnerStream` background task
    /// (if it was ever initialized). The crowdb-rpc transport is shared and
    /// owned by the store, so it is not torn down here. Idempotent.
    #[tracing::instrument(level = "debug", skip_all, fields(replica_r_id = self.node_id))]
    #[allow(clippy::unused_async)] // async kept for cascade uniformity
    pub(crate) async fn shutdown(&self, _per_layer_timeout: Duration) -> OperationReport {
        if self.shutdown_started.swap(true, Ordering::AcqRel) {
            debug!(
                replica_r_id = self.node_id,
                "PxRemoteReplica::shutdown is a no-op (already shut down)"
            );
            return OperationReport::new();
        }

        info!(
            replica_r_id = self.node_id,
            endpoint = %self.endpoint,
            "PxRemoteReplica shutdown (transport ref dropped on drop)"
        );
        OperationReport::new()
    }

    #[must_use]
    pub fn with_voting(mut self, voting: bool) -> Self {
        self.voting = voting;
        self
    }
}
