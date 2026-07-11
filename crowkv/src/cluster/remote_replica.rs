use crate::cluster::health::HealthReport;
use crate::cluster::replica::{
    HeartbeatReply, HeartbeatRequestPayload, PxReplicaError, Replica, ReplicaClient, StepDownReply, StepDownRequestPayload, VoteReply, VoteRequestPayload,
};
use crate::cluster::shutdown::ShutdownReport;
use crate::cluster::snapshot::RemoteSnapshot;
use crate::common::metrics::LayerMetrics;
use crate::paxos::roles::{PxAcceptReply, PxBallot, PxLogEntry, PxLogEntryKind, PxPrepareReply};
use crate::paxos::PxNodeId;
use crate::rpc::px_service_client::PxServiceClient;
use crate::rpc::{
    AcceptRequest, AcceptedValue, HeartbeatRequest as RpcHeartbeatRequest, PreVoteRequest as RpcPreVoteRequest, PrepareRequest, RequestVoteRequest as RpcRequestVoteRequest,
    StepDownRequest as RpcStepDownRequest,
};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
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
    async fn send_prepare(&self, slot: u64, ballot: PxBallot, term: crate::paxos::PxTerm, group_id: u64) -> Result<PxPrepareReply, PxReplicaError> {
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
        let mut client = match self.get_client().await {
            Ok(c) => c.clone(),
            Err(status) => {
                self.metrics.record_err();
                return Err(status_to_err(&status));
            }
        };
        let started = Instant::now();
        let resp = match client
            .accept(AcceptRequest {
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
                    payload: (*entry.payload).clone(),
                }),
                request_id: 0,
                request_create_ms: 0,
                client_id: entry.client_id.unwrap_or(0),
                seq: entry.seq.unwrap_or(0),
                group_id,
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

    async fn send_pre_vote(&self, req: VoteRequestPayload, group_id: u64) -> Result<VoteReply, PxReplicaError> {
        let mut client = self.get_client().await.map_err(|s| {
            self.metrics.record_err();
            status_to_err(&s)
        })?.clone();
        let started = Instant::now();
        let resp = client
            .pre_vote(RpcPreVoteRequest {
                version: 1,
                group_id,
                term: req.term,
                candidate_id: req.candidate_id,
                last_chosen_slot: req.last_chosen_slot,
                last_chosen_term: req.last_chosen_term,
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

    async fn send_request_vote(&self, req: VoteRequestPayload, group_id: u64) -> Result<VoteReply, PxReplicaError> {
        let mut client = self.get_client().await.map_err(|s| {
            self.metrics.record_err();
            status_to_err(&s)
        })?.clone();
        let started = Instant::now();
        let resp = client
            .request_vote(RpcRequestVoteRequest {
                version: 1,
                group_id,
                term: req.term,
                candidate_id: req.candidate_id,
                last_chosen_slot: req.last_chosen_slot,
                last_chosen_term: req.last_chosen_term,
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

    async fn send_heartbeat(&self, req: HeartbeatRequestPayload, group_id: u64) -> Result<HeartbeatReply, PxReplicaError> {
        let mut client = self.get_client().await.map_err(|s| {
            self.metrics.record_err();
            status_to_err(&s)
        })?.clone();
        let started = Instant::now();
        let resp = client
            .heartbeat(RpcHeartbeatRequest {
                version: 1,
                group_id,
                term: req.term,
                leader_id: req.leader_id,
                prev_log_slot: req.prev_log_slot,
                prev_log_term: req.prev_log_term,
                committed_safe_slot: req.committed_safe_slot,
                lease_grant_until_ms_mono: req.lease_grant_until_ms_mono,
                t_send_ms_mono: req.t_send_ms_mono,
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
        Ok(HeartbeatReply {
            term: resp.term,
            success: resp.success,
            contiguous_chosen: resp.contiguous_chosen,
            last_chosen_term: resp.last_chosen_term,
            contiguous_applied: resp.contiguous_applied,
            highest_seen_slot: resp.highest_seen_slot,
        })
    }

    async fn send_step_down(&self, req: StepDownRequestPayload, group_id: u64) -> Result<StepDownReply, PxReplicaError> {
        let mut client = self.get_client().await.map_err(|s| {
            self.metrics.record_err();
            status_to_err(&s)
        })?.clone();
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
            voting: true,
            metrics: LayerMetrics::new(),
            shutdown_started: AtomicBool::new(false),
        }
    }

    /// Read this remote's RPC metrics for the topology endpoint.
    #[must_use]
    pub fn snapshot(&self) -> RemoteSnapshot {
        RemoteSnapshot {
            id: self.node_id,
            endpoint: self.endpoint.clone(),
            voting: self.voting,
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
    pub async fn shutdown(&self, _per_layer_timeout: Duration) -> ShutdownReport {
        if self.shutdown_started.swap(true, Ordering::AcqRel) {
            debug!(replica_r_id = self.node_id, "PxRemoteReplica::shutdown is a no-op (already shut down)");
            return ShutdownReport::new();
        }

        // `take` requires `&mut`; OnceCell doesn't expose that on `&self`.
        // The Channel inside Box will be dropped when the OnceCell itself is
        // dropped (when PxGroup is dropped). For now, we rely on Drop. If
        // explicit close is needed earlier (e.g. tearing down a remote without
        // tearing down the group), expose `OnceCell` mutation through a
        // `&mut self` API.
        info!(
            replica_r_id = self.node_id,
            endpoint = %self.endpoint,
            "PxRemoteReplica shutdown (channel will close on drop)"
        );
        ShutdownReport::new()
    }

    #[must_use]
    pub fn with_voting(mut self, voting: bool) -> Self {
        self.voting = voting;
        self
    }

    pub fn id(&self) -> PxNodeId {
        self.node_id
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Best-effort cached health.
    ///
    /// V1: no active probe. We only know:
    /// - if the `OnceCell` is initialized, we connected at least once and the
    ///   tonic `Channel` is auto-reconnecting; report `Ok`.
    /// - if not initialized, no traffic has been sent; report `Degraded`
    ///   with a message so operators see "haven't talked yet".
    ///
    /// Once §3 metrics expose `err_count` / `last_rtt_ms`, this method should
    /// downgrade to `Degraded`/`Unhealthy` based on those counters.
    #[must_use]
    pub fn health(&self) -> HealthReport {
        if self.grpc_client.initialized() {
            HealthReport::ok()
        } else {
            HealthReport::degraded(format!("remote {} ({}): channel not yet established", self.node_id, self.endpoint))
        }
    }

    fn accepted_value_to_log_entry(value: AcceptedValue) -> PxLogEntry {
        PxLogEntry {
            slot: value.slot,
            ballot: PxBallot::new(value.round, value.leader_id),
            term: value.term,
            kind: PxLogEntryKind::Write,
            payload: Arc::new(value.payload),
            client_id: None,
            seq: None,
        }
    }
}
