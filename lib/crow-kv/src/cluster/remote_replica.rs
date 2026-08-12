// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! `PxRemoteReplica` — gRPC adapter for a peer replica in a Paxos group.
//!
//! Wraps a lazy gRPC client, a per-peer bidi `PxLearnerStream` (for
//! Accept / `ChosenNotification` fan-out), a dedicated heartbeat channel
//! (separate TCP connection for steady-state heartbeats so liveness
//! messages are never blocked behind data on the `LearnerStream`), and a
//! small layer of cached status/metrics state. Exposes `ReplicaClient` so
//! callers talk to peers through a uniform RPC surface.
//!
//! Key work: lazy gRPC client init, peer-stream construction & lifecycle,
//! Prepare/Accept/PreVote/RequestVote/Heartbeat/StepDown RPC bridges,
//! cached status snapshots for the management API.

use crate::cluster::learner_stream::PxLearnerStream;
use crate::cluster::replica::{
    HeartbeatReply, HeartbeatRequestPayload, PxReplicaError, Replica, ReplicaClient, StepDownReply,
    StepDownRequestPayload, VoteReply, VoteRequestPayload,
};
use crate::cluster::status::{RemoteStatus, StatusLevel};
use crate::common::config::PxElectionConfig;
use crate::common::metrics::LayerMetrics;
use crate::common::report::OperationReport;
use crate::metrics::{Counter, LatencySummary, MetricsRegistry};
use crate::paxos::roles::{DedupTag, PxAcceptReply, PxBallot, PxLogEntry, PxPrepareReply};
use crate::paxos::PxNodeId;
use crate::rpc::px_service_client::PxServiceClient;
use crate::rpc::{
    AcceptRequest, AcceptedValue, BatchChosenNotification as RpcBatchChosenNotification,
    ChosenNotification as RpcChosenNotification, HeartbeatRequest as RpcHeartbeatRequest,
    PreVoteRequest as RpcPreVoteRequest, PrepareRequest, RequestVoteRequest as RpcRequestVoteRequest,
    StepDownRequest as RpcStepDownRequest,
};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use tokio::sync::OnceCell;
use tonic::transport::{Channel, Endpoint};
use tracing::{debug, info};

#[derive(Debug)]
pub struct PxRemoteReplica {
    pub(crate) node_id: PxNodeId,
    pub(crate) endpoint: String,
    // Boxed to reduce inline size - PxServiceClient is large (~272 bytes),
    // which would make RemoteReplicaKind::Real large and trigger large_enum_variant warning.
    // Heap allocation is deferred via OnceCell until first use.
    grpc_client: OnceCell<Box<PxServiceClient<Channel>>>,
    /// Dedicated gRPC client for steady-state heartbeats. Established
    /// lazily on first heartbeat (mirroring the `learner_stream`
    /// connect-on-first-use pattern) and reused for the peer's lifetime.
    /// Separate TCP connection from `grpc_client` and `learner_stream` so
    /// liveness messages are never blocked behind data traffic.
    heartbeat_client: OnceCell<Box<PxServiceClient<Channel>>>,
    /// Lazy-initialized per-peer bidi stream. Carries `Accept` and
    /// `ChosenNotification` frames. Constructed on first call to
    /// [`Self::learner_stream`].
    learner_stream: OnceLock<Arc<PxLearnerStream>>,
    /// Window size used for the lazy `learner_stream` mpsc. Snapshot of
    /// `PxElectionConfig::learner_stream_window_frames` taken at the time
    /// `with_config` is called; defaults to `64` for tests / callsites
    /// that construct via [`Self::new`] without a config.
    learner_stream_window_frames: usize,
    /// Per-RPC deadline for `send_prepare` (unary), the bidi
    /// `learner_stream` accept call, and the unary `heartbeat` RPC.
    /// Snapshot of `PxElectionConfig::learner_stream_rpc_timeout_ms`.
    rpc_timeout: Duration,
    /// Monotonic correlation id allocator for `Accept` frames sent over
    /// [`Self::learner_stream`]. Starts at 1; 0 is reserved as the "no
    /// correlation" sentinel used by fire-and-forget frames such as
    /// `ChosenNotification`.
    next_request_id: AtomicU64,
    pub(crate) voting: bool,
    /// Per-remote RPC counters consumed by `/topology`.
    metrics: LayerMetrics,
    /// Optional registry handles mirroring RPC stats to the metrics log.
    /// Set via [`Self::set_metrics_registry`] when a registry is wired.
    rpc_handles: OnceLock<RpcRegistryHandles>,
    /// Idempotency gate for [`Self::shutdown`].
    shutdown_started: AtomicBool,
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

fn status_to_err(status: &tonic::Status) -> PxReplicaError {
    // Preserve the gRPC code in the message so logs / observability can still
    // distinguish Unavailable / NotFound / Internal at the network boundary.
    PxReplicaError::Internal(format!("grpc {}: {}", status.code(), status.message()))
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
        let mut client = match self.get_client().await {
            Ok(c) => c.clone(),
            Err(status) => {
                self.record_err();
                return Err(status_to_err(&status));
            }
        };
        let started = Instant::now();
        let resp = match tokio::time::timeout(
            self.rpc_timeout,
            client.prepare(PrepareRequest {
                version: 1,
                slot,
                round: ballot.round,
                leader_id: ballot.leader_id,
                request_id: 0,
                request_create_ms: 0,
                group_id,
                term,
                membership_epoch,
            }),
        )
        .await
        {
            Ok(Ok(r)) => r.into_inner(),
            Ok(Err(status)) => {
                self.record_err();
                return Err(status_to_err(&status));
            }
            Err(_) => {
                self.record_err();
                return Err(PxReplicaError::Internal(format!(
                    "prepare rpc timeout after {} ms at peer {}",
                    self.rpc_timeout.as_millis(),
                    self.endpoint
                )));
            }
        };
        #[allow(clippy::cast_possible_truncation)]
        self.record_ok(started.elapsed().as_millis() as u64);

        if resp.epoch_mismatch {
            Ok(PxPrepareReply::EpochMismatch {
                responder_epoch: resp.membership_epoch,
            })
        } else if resp.term_stale {
            Ok(PxPrepareReply::TermStale {
                slot: resp.slot,
                new_term: resp.term,
            })
        } else if resp.rejected {
            Ok(PxPrepareReply::Rejected {
                slot: resp.slot,
                current_promised: PxBallot::new(resp.rejected_round, resp.rejected_leader_id),
            })
        } else {
            Ok(PxPrepareReply::Promised {
                slot: resp.slot,
                accepted: resp.previously_accepted.map(Self::accepted_value_to_log_entry),
            })
        }
    }

    async fn send_accept(
        &self,
        entry: &PxLogEntry,
        dedup_tags: &[DedupTag],
        group_id: u64,
        membership_epoch: u64,
    ) -> Result<PxAcceptReply, PxReplicaError> {
        // Route `Accept` through the per-peer bidi PxLearnerStream rather
        // than a one-shot unary RPC. The wire-level conversion
        // (PxLogEntry -> AcceptRequest, AcceptedResponse -> PxAcceptReply)
        // is identical; only the transport changes.
        let request_id = self.alloc_request_id();
        // Legacy single-tag fields: the first coalesced tag (or 0) so
        // older followers during a rolling upgrade still record one dedup
        // entry. New followers prefer `dedup_tags`.
        let (legacy_client_id, legacy_seq) = dedup_tags.first().map_or((0, 0), |t| (t.client_id, t.seq));
        let req = AcceptRequest {
            version: 1,
            slot: entry.slot,
            round: entry.ballot.round,
            leader_id: entry.ballot.leader_id,
            term: entry.term,
            value: Some(AcceptedValue {
                slot: entry.slot,
                round: entry.ballot.round,
                leader_id: entry.ballot.leader_id,
                term: entry.term,
                // `Bytes::clone` is an `O(1)` ref-count bump; the
                // underlying buffer is shared with the local entry.
                payload: entry.payload.clone(),
            }),
            request_id,
            request_create_ms: 0,
            client_id: legacy_client_id,
            seq: legacy_seq,
            group_id,
            membership_epoch,
            dedup_tags: dedup_tags
                .iter()
                .map(|t| crate::rpc::DedupTag {
                    client_id: t.client_id,
                    seq: t.seq,
                })
                .collect(),
        };

        let started = Instant::now();
        let resp = match self.learner_stream().send_accept(req).await {
            Ok(r) => r,
            Err(err) => {
                self.record_err();
                return Err(err);
            }
        };
        #[allow(clippy::cast_possible_truncation)]
        self.record_ok(started.elapsed().as_millis() as u64);

        if resp.epoch_mismatch {
            Ok(PxAcceptReply::EpochMismatch {
                responder_epoch: resp.membership_epoch,
            })
        } else if resp.term_stale {
            Ok(PxAcceptReply::TermStale {
                slot: resp.slot,
                new_term: resp.term,
            })
        } else if resp.rejected {
            Ok(PxAcceptReply::Rejected {
                slot: resp.slot,
                current_promised: PxBallot::new(resp.rejected_round, resp.rejected_leader_id),
            })
        } else {
            Ok(PxAcceptReply::Accepted {
                slot: resp.slot,
                ballot: PxBallot::new(resp.round, resp.leader_id),
            })
        }
    }

    async fn send_pre_vote(
        &self,
        req: VoteRequestPayload,
        group_id: u64,
    ) -> Result<VoteReply, PxReplicaError> {
        let mut client = self
            .get_client()
            .await
            .map_err(|s| {
                self.record_err();
                status_to_err(&s)
            })?
            .clone();
        let started = Instant::now();
        let resp = client
            .pre_vote(RpcPreVoteRequest {
                version: 1,
                group_id,
                term: req.term,
                candidate_id: req.candidate_id,
                accepted_log_tip_slot: req.accepted_log_tip_slot,
                accepted_log_tip_term: req.accepted_log_tip_term,
                request_id: 0,
                request_create_ms: 0,
            })
            .await
            .map_err(|s| {
                self.record_err();
                status_to_err(&s)
            })?
            .into_inner();
        #[allow(clippy::cast_possible_truncation)]
        self.record_ok(started.elapsed().as_millis() as u64);
        Ok(VoteReply {
            term: resp.term,
            granted: resp.granted,
            contiguous_chosen: resp.contiguous_chosen,
            last_chosen_term: resp.last_chosen_term,
            highest_seen_slot: resp.highest_seen_slot,
        })
    }

    async fn send_request_vote(
        &self,
        req: VoteRequestPayload,
        group_id: u64,
    ) -> Result<VoteReply, PxReplicaError> {
        let mut client = self
            .get_client()
            .await
            .map_err(|s| {
                self.record_err();
                status_to_err(&s)
            })?
            .clone();
        let started = Instant::now();
        let resp = client
            .request_vote(RpcRequestVoteRequest {
                version: 1,
                group_id,
                term: req.term,
                candidate_id: req.candidate_id,
                accepted_log_tip_slot: req.accepted_log_tip_slot,
                accepted_log_tip_term: req.accepted_log_tip_term,
                request_id: 0,
                request_create_ms: 0,
            })
            .await
            .map_err(|s| {
                self.record_err();
                status_to_err(&s)
            })?
            .into_inner();
        #[allow(clippy::cast_possible_truncation)]
        self.record_ok(started.elapsed().as_millis() as u64);
        Ok(VoteReply {
            term: resp.term,
            granted: resp.granted,
            contiguous_chosen: resp.contiguous_chosen,
            last_chosen_term: resp.last_chosen_term,
            highest_seen_slot: resp.highest_seen_slot,
        })
    }

    async fn send_heartbeat(
        &self,
        req: HeartbeatRequestPayload,
        group_id: u64,
    ) -> Result<HeartbeatReply, PxReplicaError> {
        // Route Heartbeat over a dedicated gRPC Channel (separate TCP
        // connection) via the unary `heartbeat` RPC so liveness messages
        // are never blocked behind data on the `LearnerStream`. The FIFO
        // ordering invariant with `Accept` is not a hard safety
        // requirement — the term fence handles cross-term reordering and
        // same-term heartbeat/accept mutate independent state. See
        // `design-crow-kv-rpc.md` §3.
        let request_id = self.alloc_request_id();
        let rpc_req = RpcHeartbeatRequest {
            version: 1,
            group_id,
            term: req.term,
            leader_id: req.leader_id,
            prev_log_slot: req.prev_log_slot,
            prev_log_term: req.prev_log_term,
            committed_safe_slot: req.committed_safe_slot,
            lease_grant_until_ms_mono: req.lease_grant_until_ms_mono,
            t_send_ms_mono: req.t_send_ms_mono,
            request_id,
            request_create_ms: 0,
        };
        let mut client = match self.get_heartbeat_client().await {
            Ok(c) => c.clone(),
            Err(status) => {
                self.record_err();
                return Err(status_to_err(&status));
            }
        };
        let started = Instant::now();
        let resp = match tokio::time::timeout(self.rpc_timeout, client.heartbeat(rpc_req)).await {
            Ok(Ok(r)) => r.into_inner(),
            Ok(Err(status)) => {
                self.record_err();
                return Err(status_to_err(&status));
            }
            Err(_) => {
                self.record_err();
                return Err(PxReplicaError::Internal(format!(
                    "heartbeat rpc timeout after {} ms at peer {}",
                    self.rpc_timeout.as_millis(),
                    self.endpoint
                )));
            }
        };
        #[allow(clippy::cast_possible_truncation)]
        self.record_ok(started.elapsed().as_millis() as u64);
        Ok(HeartbeatReply {
            term: resp.term,
            success: resp.success,
            contiguous_chosen: resp.contiguous_chosen,
            last_chosen_term: resp.last_chosen_term,
            contiguous_applied: resp.contiguous_applied,
            highest_seen_slot: resp.highest_seen_slot,
            durable_snapshot_slot: resp.durable_snapshot_slot,
        })
    }

    async fn send_step_down(
        &self,
        req: &StepDownRequestPayload,
        group_id: u64,
    ) -> Result<StepDownReply, PxReplicaError> {
        let mut client = self
            .get_client()
            .await
            .map_err(|s| {
                self.record_err();
                status_to_err(&s)
            })?
            .clone();
        let started = Instant::now();
        let resp = client
            .step_down(RpcStepDownRequest {
                version: 1,
                group_id,
                term: req.term,
                target_leader_id: req.target_leader_id,
                reason: req.reason.clone(),
                request_id: 0,
                request_create_ms: 0,
            })
            .await
            .map_err(|s| {
                self.record_err();
                status_to_err(&s)
            })?
            .into_inner();
        #[allow(clippy::cast_possible_truncation)]
        self.record_ok(started.elapsed().as_millis() as u64);
        Ok(StepDownReply {
            accepted: resp.accepted,
            current_term: resp.current_term,
            current_leader_id: resp.current_leader_id,
        })
    }
}

impl PxRemoteReplica {
    async fn get_client(&self) -> Result<&PxServiceClient<Channel>, tonic::Status> {
        self.grpc_client
            .get_or_try_init(|| async {
                let ch = Endpoint::from_shared(format!("http://{}", self.endpoint))
                    .map_err(|e| tonic::Status::unavailable(e.to_string()))?
                    .tcp_nodelay(true)
                    .http2_keep_alive_interval(Duration::from_secs(5))
                    .keep_alive_while_idle(true)
                    .connect()
                    .await
                    .map_err(|e| tonic::Status::unavailable(e.to_string()))?;
                Ok(Box::new(PxServiceClient::new(ch)))
            })
            .await
            .map(std::convert::AsRef::as_ref)
    }

    /// Lazily establish (and reuse) the dedicated heartbeat channel.
    /// Separate TCP connection from `get_client` and `learner_stream` so
    /// steady-state heartbeats are never blocked behind data traffic.
    async fn get_heartbeat_client(&self) -> Result<&PxServiceClient<Channel>, tonic::Status> {
        self.heartbeat_client
            .get_or_try_init(|| async {
                let ch = Endpoint::from_shared(format!("http://{}", self.endpoint))
                    .map_err(|e| tonic::Status::unavailable(e.to_string()))?
                    .tcp_nodelay(true)
                    .http2_keep_alive_interval(Duration::from_secs(5))
                    .keep_alive_while_idle(true)
                    .connect()
                    .await
                    .map_err(|e| tonic::Status::unavailable(e.to_string()))?;
                Ok(Box::new(PxServiceClient::new(ch)))
            })
            .await
            .map(std::convert::AsRef::as_ref)
    }

    #[must_use]
    pub fn new(node_id: PxNodeId, endpoint: String) -> Self {
        Self {
            node_id,
            endpoint,
            grpc_client: OnceCell::new(),
            heartbeat_client: OnceCell::new(),
            learner_stream: OnceLock::new(),
            learner_stream_window_frames: PxElectionConfig::DEFAULT.learner_stream_window_frames,
            rpc_timeout: Duration::from_millis(PxElectionConfig::DEFAULT.learner_stream_rpc_timeout_ms),
            next_request_id: AtomicU64::new(1),
            voting: true,
            metrics: LayerMetrics::new(),
            rpc_handles: OnceLock::new(),
            shutdown_started: AtomicBool::new(false),
        }
    }

    /// Construct a remote replica with the given election config snapshot.
    /// Consumes `learner_stream_window_frames` (learner stream window) and
    /// `learner_stream_rpc_timeout_ms` (per-RPC deadline); other fields stay
    /// configurable per-call.
    #[must_use]
    #[allow(dead_code)]
    pub(crate) fn with_config(node_id: PxNodeId, endpoint: String, cfg: &PxElectionConfig) -> Self {
        let mut r = Self::new(node_id, endpoint);
        r.learner_stream_window_frames = cfg.learner_stream_window_frames;
        r.rpc_timeout = Duration::from_millis(cfg.learner_stream_rpc_timeout_ms);
        r
    }

    /// Lazily construct (and reuse) the per-peer bidi stream client.
    /// Returns the shared `Arc<PxLearnerStream>`; the underlying background
    /// task is spawned on first call and lives until [`Self::shutdown`].
    pub(crate) fn learner_stream(&self) -> &Arc<PxLearnerStream> {
        self.learner_stream.get_or_init(|| {
            let cfg = PxElectionConfig {
                learner_stream_window_frames: self.learner_stream_window_frames,
                learner_stream_rpc_timeout_ms: self.rpc_timeout.as_millis().try_into().unwrap_or(u64::MAX),
                ..PxElectionConfig::DEFAULT
            };
            PxLearnerStream::new(self.endpoint.clone(), &cfg)
        })
    }

    /// Fire-and-forget peer notification that `(slot, term)` is chosen
    /// in `group_id`. Sent over the per-peer bidi `PxLearnerStream`. No
    /// response is awaited; failures (peer down,
    /// stream reset) are returned for caller-side observability but
    /// the proposer treats them as best-effort and swallows them
    /// since the next heartbeat re-converges frontiers anyway.
    ///
    /// # Errors
    /// Returns [`PxReplicaError::Internal`] if the per-peer stream is
    /// shut down or its reconnect loop is currently failing fast.
    pub(crate) fn send_chosen_notice(
        &self,
        slot: u64,
        term: crate::paxos::PxTerm,
        leader_id: PxNodeId,
        group_id: u64,
        ballot_round: u64,
    ) -> Result<(), PxReplicaError> {
        let notice = RpcChosenNotification {
            version: 1,
            group_id,
            slot,
            term,
            leader_id,
            request_id: 0,
            request_create_ms: 0,
            ballot_round,
        };
        self.learner_stream().send_chosen(notice)
    }

    /// R63: fire-and-forget batch chosen notice covering `[start_slot,
    /// end_slot]`. Sent over the per-peer bidi `PxLearnerStream` before the
    /// full-accept catch-up loop so the follower's chosen frontier advances
    /// for present slots without waiting for the full-accept round-trip.
    ///
    /// # Errors
    /// Returns [`PxReplicaError::Internal`] if the per-peer stream is
    /// shut down or its reconnect loop is currently failing fast.
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
        let batch = RpcBatchChosenNotification {
            version: 1,
            group_id,
            start_slot,
            end_slot,
            term,
            leader_id,
            ballot_round,
        };
        self.learner_stream().send_batch_chosen(batch)
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
    pub(crate) async fn send_fetch_gap(
        &self,
        slot: u64,
        term: crate::paxos::PxTerm,
        leader_id: PxNodeId,
        group_id: u64,
    ) -> Result<crate::rpc::FetchGapResponse, PxReplicaError> {
        let req = crate::rpc::FetchGapRequest {
            version: 1,
            group_id,
            slot,
            term,
            leader_id,
        };
        self.learner_stream().send_fetch_gap(req).await
    }

    /// Allocate the next correlation id for an `Accept` frame or unary
    /// `Heartbeat` RPC. Never returns 0 (that value is reserved for
    /// fire-and-forget frames like `ChosenNotification`).
    fn alloc_request_id(&self) -> u64 {
        let id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        if id == 0 {
            self.next_request_id.fetch_add(1, Ordering::Relaxed)
        } else {
            id
        }
    }

    /// Record a successful RPC to both legacy and registry handles.
    fn record_ok(&self, rtt_ms: u64) {
        self.metrics.record_ok(rtt_ms);
        if let Some(h) = self.rpc_handles.get() {
            h.latency.observe(rtt_ms.saturating_mul(1_000_000));
        }
    }

    /// Record a failed RPC to both legacy and registry handles.
    fn record_err(&self) {
        self.metrics.record_err();
        if let Some(h) = self.rpc_handles.get() {
            h.errors.inc();
        }
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
        if !self.grpc_client.initialized() {
            status = StatusLevel::Degraded;
            messages.push(format!(
                "remote {} ({}): channel not yet established",
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

    /// Cascade shutdown: take and drop the gRPC client (closes the channel).
    ///
    /// `tonic::transport::Channel` cleans up its connection on drop, so taking
    /// the `OnceCell`'s value is sufficient. This is fast (no real I/O) so
    /// the timeout is informational only — we still pass it through for
    /// uniformity with the cascade contract.
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

        // `take` requires `&mut`; OnceCell doesn't expose that on `&self`.
        // The Channel inside Box will be dropped when the OnceCell itself is
        // dropped (when PxGroup is dropped). For now, we rely on Drop. If
        // explicit close is needed earlier (e.g. tearing down a remote without
        // tearing down the group), expose `OnceCell` mutation through a
        // `&mut self` API.
        // Stop the PxLearnerStream background task (idempotent;
        // safe to call even if the stream was never initialized).
        if let Some(stream) = self.learner_stream.get() {
            stream.shutdown();
        }

        info!(
            replica_r_id = self.node_id,
            endpoint = %self.endpoint,
            "PxRemoteReplica shutdown (channel will close on drop)"
        );
        OperationReport::new()
    }

    #[must_use]
    pub fn with_voting(mut self, voting: bool) -> Self {
        self.voting = voting;
        self
    }

    fn accepted_value_to_log_entry(value: AcceptedValue) -> PxLogEntry {
        PxLogEntry {
            slot: value.slot,
            ballot: PxBallot::new(value.round, value.leader_id),
            term: value.term,
            payload: value.payload,
        }
    }
}
