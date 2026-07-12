//! `PxRemoteReplica` — gRPC adapter for a peer replica in a Paxos group.
//!
//! Wraps a lazy gRPC client, a per-peer bidi `PxPeerStream` (for
//! Accept / Heartbeat / `ChosenNotification` fan-out), and a small layer
//! of cached status/metrics state. Exposes `ReplicaClient` so callers
//! talk to peers through a uniform RPC surface.
//!
//! Key work: lazy gRPC client init, peer-stream construction & lifecycle,
//! Prepare/Accept/PreVote/RequestVote/Heartbeat/StepDown RPC bridges,
//! cached status snapshots for the management API.

use crate::cluster::peer_stream::PxPeerStream;
use crate::cluster::replica::{
    HeartbeatReply, HeartbeatRequestPayload, PxReplicaError, Replica, ReplicaClient, StepDownReply,
    StepDownRequestPayload, VoteReply, VoteRequestPayload,
};
use crate::cluster::status::{RemoteStatus, StatusLevel};
use crate::common::config::PxElectionConfig;
use crate::common::metrics::LayerMetrics;
use crate::common::report::OperationReport;
use crate::paxos::roles::{PxAcceptReply, PxBallot, PxLogEntry, PxLogEntryKind, PxPrepareReply};
use crate::paxos::PxNodeId;
use crate::rpc::px_service_client::PxServiceClient;
use crate::rpc::{
    AcceptRequest, AcceptedValue, ChosenNotification as RpcChosenNotification,
    HeartbeatRequest as RpcHeartbeatRequest, PreVoteRequest as RpcPreVoteRequest, PrepareRequest,
    RequestVoteRequest as RpcRequestVoteRequest, StepDownRequest as RpcStepDownRequest,
};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::{Duration, Instant};
use tokio::sync::OnceCell;
use tonic::transport::Channel;
use tracing::{debug, info};

#[derive(Debug)]
pub struct PxRemoteReplica {
    pub(crate) node_id: PxNodeId,
    pub(crate) endpoint: String,
    // Boxed to reduce inline size - PxServiceClient is large (~272 bytes),
    // which would make RemoteReplicaKind::Real large and trigger large_enum_variant warning.
    // Heap allocation is deferred via OnceCell until first use.
    grpc_client: OnceCell<Box<PxServiceClient<Channel>>>,
    /// Lazy-initialized per-peer bidi stream. Carries `Accept`,
    /// `Heartbeat`, and `ChosenNotification` frames. Constructed on
    /// first call to [`Self::peer_stream`].
    peer_stream: OnceLock<Arc<PxPeerStream>>,
    /// Window size used for the lazy `peer_stream` mpsc. Snapshot of
    /// `PxElectionConfig::peer_stream_window_frames` taken at the time
    /// `with_config` is called; defaults to `64` for tests / callsites
    /// that construct via [`Self::new`] without a config.
    peer_stream_window_frames: usize,
    /// Monotonic correlation id allocator for `Accept` / `Heartbeat`
    /// frames sent over [`Self::peer_stream`]. Starts at 1; 0 is
    /// reserved as the "no correlation" sentinel used by fire-and-forget
    /// frames such as `ChosenNotification`.
    next_request_id: AtomicU64,
    pub(crate) voting: bool,
    /// Per-remote RPC counters consumed by `/topology`.
    metrics: LayerMetrics,
    /// Idempotency gate for [`Self::shutdown`].
    shutdown_started: AtomicBool,
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
    ) -> Result<PxPrepareReply, PxReplicaError> {
        let mut client = match self.get_client().await {
            Ok(c) => c.clone(),
            Err(status) => {
                self.metrics.record_err();
                return Err(status_to_err(&status));
            }
        };
        let started = Instant::now();
        let resp = match client
            .prepare(PrepareRequest {
                version: 1,
                slot,
                round: ballot.round,
                leader_id: ballot.leader_id,
                request_id: 0,
                request_create_ms: 0,
                group_id,
                term,
            })
            .await
        {
            Ok(r) => r.into_inner(),
            Err(status) => {
                self.metrics.record_err();
                return Err(status_to_err(&status));
            }
        };
        #[allow(clippy::cast_possible_truncation)]
        self.metrics.record_ok(started.elapsed().as_millis() as u64);

        if resp.term_stale {
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

    async fn send_accept(&self, entry: &PxLogEntry, group_id: u64) -> Result<PxAcceptReply, PxReplicaError> {
        // Route `Accept` through the per-peer bidi PxPeerStream rather
        // than a one-shot unary RPC. The wire-level conversion
        // (PxLogEntry -> AcceptRequest, AcceptedResponse -> PxAcceptReply)
        // is identical; only the transport changes.
        let request_id = self.alloc_request_id();
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
            client_id: entry.client_id.unwrap_or(0),
            seq: entry.seq.unwrap_or(0),
            group_id,
        };

        let started = Instant::now();
        let resp = match self.peer_stream().send_accept(req).await {
            Ok(r) => r,
            Err(err) => {
                self.metrics.record_err();
                return Err(err);
            }
        };
        #[allow(clippy::cast_possible_truncation)]
        self.metrics.record_ok(started.elapsed().as_millis() as u64);

        if resp.term_stale {
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
                self.metrics.record_err();
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
                self.metrics.record_err();
                status_to_err(&s)
            })?
            .into_inner();
        #[allow(clippy::cast_possible_truncation)]
        self.metrics.record_ok(started.elapsed().as_millis() as u64);
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
                self.metrics.record_err();
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
                self.metrics.record_err();
                status_to_err(&s)
            })?
            .into_inner();
        #[allow(clippy::cast_possible_truncation)]
        self.metrics.record_ok(started.elapsed().as_millis() as u64);
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
        // Route Heartbeat through the per-peer bidi PxPeerStream so it
        // shares ordering with `Accept` (no heartbeat can race ahead of
        // an in-flight Accept on the same peer). Wire-format conversion
        // is unchanged.
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
        let started = Instant::now();
        let resp = match self.peer_stream().send_heartbeat(rpc_req).await {
            Ok(r) => r,
            Err(err) => {
                self.metrics.record_err();
                return Err(err);
            }
        };
        #[allow(clippy::cast_possible_truncation)]
        self.metrics.record_ok(started.elapsed().as_millis() as u64);
        Ok(HeartbeatReply {
            term: resp.term,
            success: resp.success,
            contiguous_chosen: resp.contiguous_chosen,
            last_chosen_term: resp.last_chosen_term,
            contiguous_applied: resp.contiguous_applied,
            highest_seen_slot: resp.highest_seen_slot,
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
                self.metrics.record_err();
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
                self.metrics.record_err();
                status_to_err(&s)
            })?
            .into_inner();
        #[allow(clippy::cast_possible_truncation)]
        self.metrics.record_ok(started.elapsed().as_millis() as u64);
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
                PxServiceClient::connect(format!("http://{}", self.endpoint))
                    .await
                    .map_err(|e| tonic::Status::unavailable(e.to_string()))
                    .map(Box::new)
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
            peer_stream: OnceLock::new(),
            peer_stream_window_frames: PxElectionConfig::DEFAULT.peer_stream_window_frames,
            next_request_id: AtomicU64::new(1),
            voting: true,
            metrics: LayerMetrics::new(),
            shutdown_started: AtomicBool::new(false),
        }
    }

    /// Construct a remote replica with the given election config snapshot.
    /// The only field consumed today is `peer_stream_window_frames`
    /// (peer stream window); other fields stay configurable per-call.
    #[must_use]
    pub fn with_config(node_id: PxNodeId, endpoint: String, cfg: &PxElectionConfig) -> Self {
        let mut r = Self::new(node_id, endpoint);
        r.peer_stream_window_frames = cfg.peer_stream_window_frames;
        r
    }

    /// Lazily construct (and reuse) the per-peer bidi stream client.
    /// Returns the shared `Arc<PxPeerStream>`; the underlying background
    /// task is spawned on first call and lives until [`Self::shutdown`].
    fn peer_stream(&self) -> &Arc<PxPeerStream> {
        self.peer_stream.get_or_init(|| {
            let cfg = PxElectionConfig {
                peer_stream_window_frames: self.peer_stream_window_frames,
                ..PxElectionConfig::DEFAULT
            };
            PxPeerStream::new(self.endpoint.clone(), &cfg)
        })
    }

    /// Fire-and-forget peer notification that `(slot, term)` is chosen
    /// in `group_id`. Sent over the per-peer bidi `PxPeerStream`. No
    /// response is awaited; failures (peer down,
    /// stream reset) are returned for caller-side observability but
    /// the proposer treats them as best-effort and swallows them
    /// since the next heartbeat re-converges frontiers anyway.
    ///
    /// # Errors
    /// Returns [`PxReplicaError::Internal`] if the per-peer stream is
    /// shut down or its reconnect loop is currently failing fast.
    pub fn send_chosen_notice(
        &self,
        slot: u64,
        term: crate::paxos::PxTerm,
        leader_id: PxNodeId,
        group_id: u64,
    ) -> Result<(), PxReplicaError> {
        let notice = RpcChosenNotification {
            version: 1,
            group_id,
            slot,
            term,
            leader_id,
            request_id: 0,
            request_create_ms: 0,
        };
        self.peer_stream().send_chosen(notice)
    }

    /// Allocate the next correlation id for an `Accept` / `Heartbeat`
    /// frame. Never returns 0 (that value is reserved for fire-and-forget
    /// frames like `ChosenNotification`).
    fn alloc_request_id(&self) -> u64 {
        let id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        if id == 0 {
            self.next_request_id.fetch_add(1, Ordering::Relaxed)
        } else {
            id
        }
    }

    /// Read this remote's RPC metrics for the topology endpoint.
    #[must_use]
    pub fn status(&self) -> RemoteStatus {
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
    pub async fn shutdown(&self, _per_layer_timeout: Duration) -> OperationReport {
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
        // Stop the PxPeerStream background task (idempotent;
        // safe to call even if the stream was never initialized).
        if let Some(stream) = self.peer_stream.get() {
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
            kind: PxLogEntryKind::Write,
            payload: value.payload,
            client_id: None,
            seq: None,
        }
    }
}
