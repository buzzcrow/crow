//! Tonic `PxService` implementation that delegates to `PxLocalReplica`.
//!
//! This module contains the wire-format handler (`PxReplicaService`) that
//! converts between protobuf messages and the in-memory Paxos types,
//! then forwards to the node so that all real logic lives in one place.

use std::pin::Pin;
use std::sync::Arc;
use tokio_stream::{Stream, StreamExt};
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, warn};

use crate::cluster::px_kv_store::PxKvStore;
use crate::cluster::replica::{
    HeartbeatRequestPayload, PxReplicaError, ReplicaHandler, StepDownRequestPayload, VoteRequestPayload,
};
use crate::common::optional_u64;
use crate::paxos::roles::{PxAcceptReply, PxBallot, PxLogEntry, PxPrepareReply};
use crate::paxos::PxTerm;

use crate::rpc::px_service_server::PxService;
use crate::rpc::{
    learner_stream_request, learner_stream_response, AcceptRequest, AcceptedResponse, AcceptedValue,
    HeartbeatRequest, HeartbeatResponse, LearnerStreamRequest, LearnerStreamResponse, PreVoteRequest,
    PreVoteResponse, PrepareRequest, PromiseResponse, RequestVoteRequest, RequestVoteResponse,
    StepDownRequest, StepDownResponse,
};

/// Build an epoch-mismatch `PromiseResponse` for the Prepare fence.
fn epoch_mismatch_prepare_response(
    req: &PrepareRequest,
    term: PxTerm,
    responder_epoch: u64,
) -> PromiseResponse {
    PromiseResponse {
        version: 1,
        slot: req.slot,
        round: req.round,
        leader_id: req.leader_id,
        previously_accepted: None,
        rejected: false,
        rejected_round: 0,
        rejected_leader_id: 0,
        request_id: req.request_id,
        request_create_ms: req.request_create_ms,
        term,
        term_stale: false,
        membership_epoch: responder_epoch,
        epoch_mismatch: true,
    }
}

/// Build an epoch-mismatch `AcceptedResponse` for the Accept fence.
fn epoch_mismatch_accept_response(
    slot: u64,
    round: u64,
    leader_id: u64,
    request_id: u64,
    request_create_ms: u64,
    term: PxTerm,
    responder_epoch: u64,
) -> AcceptedResponse {
    AcceptedResponse {
        version: 1,
        slot,
        round,
        leader_id,
        rejected: false,
        rejected_round: 0,
        rejected_leader_id: 0,
        request_id,
        request_create_ms,
        term,
        term_stale: false,
        membership_epoch: responder_epoch,
        epoch_mismatch: true,
    }
}

/// gRPC adapter for the in-process [`PxReplicaError`] enum.
impl From<PxReplicaError> for Status {
    fn from(e: PxReplicaError) -> Self {
        match &e {
            PxReplicaError::GroupNotFound(_) => Status::not_found(e.to_string()),
            PxReplicaError::ShuttingDown => Status::unavailable(e.to_string()),
            PxReplicaError::Internal(_) => Status::internal(e.to_string()),
        }
    }
}

/// gRPC service wrapper that delegates `Prepare`/`Accept` to `PxLocalReplica`.
#[derive(Clone)]
pub struct PxReplicaService {
    store: Arc<PxKvStore>,
}

impl PxReplicaService {
    pub fn new(store: Arc<PxKvStore>) -> Self {
        Self { store }
    }
}

#[allow(clippy::too_many_lines)]
#[tonic::async_trait]
impl PxService for PxReplicaService {
    async fn prepare(&self, request: Request<PrepareRequest>) -> Result<Response<PromiseResponse>, Status> {
        let req = request.into_inner();
        debug!(
            store_id = self.store.store_id,
            group_id = req.group_id,
            request_id = req.request_id,
            slot = req.slot,
            round = req.round,
            leader_id = req.leader_id,
            "received paxos prepare rpc"
        );
        let ballot = PxBallot {
            round: req.round,
            leader_id: req.leader_id,
        };
        let group = self
            .store
            .get_group(req.group_id)
            .ok_or_else(|| Status::not_found("px group not found"))?;
        let replica = group.local_replica();

        let responder_epoch = group.membership_epoch();
        if req.membership_epoch != responder_epoch {
            let converged_epoch = group.adopt_membership_epoch(req.membership_epoch);
            warn!(
                store_id = self.store.store_id,
                group_id = req.group_id,
                request_id = req.request_id,
                proposer_epoch = req.membership_epoch,
                responder_epoch,
                converged_epoch,
                "prepare rejected by membership-epoch fence; adopting higher epoch from proposer"
            );
            return Ok(Response::new(epoch_mismatch_prepare_response(
                &req,
                replica.current_term_snapshot(),
                responder_epoch,
            )));
        }

        let reply = <crate::cluster::local_replica::PxLocalReplica as ReplicaHandler>::on_prepare(
            replica,
            req.slot,
            ballot,
            req.term,
            req.group_id,
        )
        .await?;

        let response = match reply {
            PxPrepareReply::Promised { slot, accepted } => PromiseResponse {
                version: 1,
                slot,
                round: req.round,
                leader_id: req.leader_id,
                previously_accepted: accepted.as_ref().map(log_entry_to_proto),
                rejected: false,
                rejected_round: 0,
                rejected_leader_id: 0,
                request_id: req.request_id,
                request_create_ms: req.request_create_ms,
                term: replica.current_term_snapshot(),
                term_stale: false,
                membership_epoch: responder_epoch,
                epoch_mismatch: false,
            },
            PxPrepareReply::TermStale { slot, new_term } => PromiseResponse {
                version: 1,
                slot,
                round: req.round,
                leader_id: req.leader_id,
                previously_accepted: None,
                rejected: false,
                rejected_round: 0,
                rejected_leader_id: 0,
                request_id: req.request_id,
                request_create_ms: req.request_create_ms,
                term: new_term,
                term_stale: true,
                membership_epoch: responder_epoch,
                epoch_mismatch: false,
            },
            PxPrepareReply::Rejected {
                slot,
                current_promised,
            } => {
                warn!(
                    store_id = self.store.store_id,
                    group_id = req.group_id,
                    request_id = req.request_id,
                    slot,
                    current_round = current_promised.round,
                    current_leader_id = current_promised.leader_id,
                    "prepare rejected; next step: proposer should retry with a higher ballot"
                );
                PromiseResponse {
                    version: 1,
                    slot,
                    round: req.round,
                    leader_id: req.leader_id,
                    previously_accepted: None,
                    rejected: true,
                    rejected_round: current_promised.round,
                    rejected_leader_id: current_promised.leader_id,
                    request_id: req.request_id,
                    request_create_ms: req.request_create_ms,
                    term: replica.current_term_snapshot(),
                    term_stale: false,
                    membership_epoch: responder_epoch,
                    epoch_mismatch: false,
                }
            }
            PxPrepareReply::EpochMismatch { .. } => {
                // on_prepare (the in-process acceptor path) never produces
                // this variant itself; it only exists on the wire-response
                // side, constructed by the early-return fence check above.
                unreachable!("on_prepare does not produce EpochMismatch")
            }
        };

        Ok(Response::new(response))
    }

    async fn accept(&self, _request: Request<AcceptRequest>) -> Result<Response<AcceptedResponse>, Status> {
        // Unary `Accept` RPC is retired — all proposers route Accept
        // frames over the per-peer bidi `LearnerStream`. The proto method
        // is kept for one release for binary-compat, but the handler
        // now refuses calls. New clients must open a `LearnerStream`.
        debug!(
            store_id = self.store.store_id,
            "unary Accept RPC is deprecated; use LearnerStream",
        );
        Err(Status::unimplemented(
            "unary Accept is deprecated in P1 M3; use LearnerStream",
        ))
    }

    // ---------------- P1 M3 leader-election stubs ----------------
    //
    // Full handlers land in election / heartbeat / prepare phases.
    // Until then these reject with
    // Unimplemented so callers see a clear gap and tests cannot accidentally
    // depend on partial behavior.

    async fn pre_vote(&self, request: Request<PreVoteRequest>) -> Result<Response<PreVoteResponse>, Status> {
        let req = request.into_inner();
        let group = self
            .store
            .get_group(req.group_id)
            .ok_or_else(|| Status::not_found("px group not found"))?;
        let replica = group.local_replica();
        let payload = VoteRequestPayload {
            term: req.term,
            candidate_id: req.candidate_id,
            accepted_log_tip_slot: req.accepted_log_tip_slot,
            accepted_log_tip_term: req.accepted_log_tip_term,
        };
        let reply = <crate::cluster::local_replica::PxLocalReplica as ReplicaHandler>::on_pre_vote(
            replica,
            payload,
            req.group_id,
        )
        .await?;
        Ok(Response::new(PreVoteResponse {
            version: 1,
            group_id: req.group_id,
            term: reply.term,
            granted: reply.granted,
            contiguous_chosen: reply.contiguous_chosen,
            last_chosen_term: reply.last_chosen_term,
            highest_seen_slot: reply.highest_seen_slot,
            request_id: req.request_id,
            request_create_ms: req.request_create_ms,
        }))
    }

    async fn request_vote(
        &self,
        request: Request<RequestVoteRequest>,
    ) -> Result<Response<RequestVoteResponse>, Status> {
        let req = request.into_inner();
        let group = self
            .store
            .get_group(req.group_id)
            .ok_or_else(|| Status::not_found("px group not found"))?;
        let replica = group.local_replica();
        let payload = VoteRequestPayload {
            term: req.term,
            candidate_id: req.candidate_id,
            accepted_log_tip_slot: req.accepted_log_tip_slot,
            accepted_log_tip_term: req.accepted_log_tip_term,
        };
        let reply = <crate::cluster::local_replica::PxLocalReplica as ReplicaHandler>::on_request_vote(
            replica,
            payload,
            req.group_id,
        )
        .await?;
        Ok(Response::new(RequestVoteResponse {
            version: 1,
            group_id: req.group_id,
            term: reply.term,
            granted: reply.granted,
            contiguous_chosen: reply.contiguous_chosen,
            last_chosen_term: reply.last_chosen_term,
            highest_seen_slot: reply.highest_seen_slot,
            request_id: req.request_id,
            request_create_ms: req.request_create_ms,
        }))
    }

    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let req = request.into_inner();
        let group = self
            .store
            .get_group(req.group_id)
            .ok_or_else(|| Status::not_found("px group not found"))?;
        let replica = group.local_replica();
        let payload = HeartbeatRequestPayload {
            term: req.term,
            leader_id: req.leader_id,
            prev_log_slot: req.prev_log_slot,
            prev_log_term: req.prev_log_term,
            committed_safe_slot: req.committed_safe_slot,
            lease_grant_until_ms_mono: req.lease_grant_until_ms_mono,
            t_send_ms_mono: req.t_send_ms_mono,
        };
        let reply = <crate::cluster::local_replica::PxLocalReplica as ReplicaHandler>::on_heartbeat(
            replica,
            payload,
            req.group_id,
        )
        .await?;
        Ok(Response::new(HeartbeatResponse {
            version: 1,
            group_id: req.group_id,
            term: reply.term,
            success: reply.success,
            contiguous_chosen: reply.contiguous_chosen,
            last_chosen_term: reply.last_chosen_term,
            contiguous_applied: reply.contiguous_applied,
            highest_seen_slot: reply.highest_seen_slot,
            request_id: req.request_id,
            request_create_ms: req.request_create_ms,
            durable_snapshot_slot: reply.durable_snapshot_slot,
        }))
    }

    async fn step_down(
        &self,
        request: Request<StepDownRequest>,
    ) -> Result<Response<StepDownResponse>, Status> {
        let req = request.into_inner();
        let group = self
            .store
            .get_group(req.group_id)
            .ok_or_else(|| Status::not_found("px group not found"))?;
        let replica = group.local_replica();
        let payload = StepDownRequestPayload {
            term: req.term,
            target_leader_id: req.target_leader_id,
            reason: req.reason,
        };
        let reply = <crate::cluster::local_replica::PxLocalReplica as ReplicaHandler>::on_step_down(
            replica,
            &payload,
            req.group_id,
        )
        .await?;
        Ok(Response::new(StepDownResponse {
            version: 1,
            group_id: req.group_id,
            accepted: reply.accepted,
            current_term: reply.current_term,
            current_leader_id: reply.current_leader_id,
            request_id: req.request_id,
            request_create_ms: req.request_create_ms,
        }))
    }

    type LearnerStreamStream =
        Pin<Box<dyn Stream<Item = Result<LearnerStreamResponse, Status>> + Send + 'static>>;

    async fn learner_stream(
        &self,
        request: Request<Streaming<LearnerStreamRequest>>,
    ) -> Result<Response<Self::LearnerStreamStream>, Status> {
        // Route inbound frames through the existing `ReplicaHandler`
        // methods and ship matching responses back on the outbound half
        // of the same bidi stream. This shares the helper bodies with
        // the unary `Accept` / `Heartbeat` RPCs.
        let mut inbound = request.into_inner();
        let store = self.store.clone();
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<LearnerStreamResponse, Status>>(64);

        tokio::spawn(async move {
            while let Some(item) = inbound.next().await {
                let frame = match item {
                    Ok(req) => req.frame,
                    Err(status) => {
                        let _ = tx.send(Err(status)).await;
                        return;
                    }
                };
                let Some(frame) = frame else { continue };
                let response_frame = match frame {
                    learner_stream_request::Frame::Accept(accept_req) => {
                        match handle_accept_inner(&store, accept_req).await {
                            Ok(resp) => Some(learner_stream_response::Frame::Accepted(resp)),
                            Err(status) => {
                                let _ = tx.send(Err(status)).await;
                                return;
                            }
                        }
                    }
                    learner_stream_request::Frame::Heartbeat(hb_req) => {
                        match handle_heartbeat_inner(&store, hb_req).await {
                            Ok(resp) => Some(learner_stream_response::Frame::Heartbeat(resp)),
                            Err(status) => {
                                let _ = tx.send(Err(status)).await;
                                return;
                            }
                        }
                    }
                    learner_stream_request::Frame::Chosen(notice) => {
                        // Route into the local replica's learner via
                        // PxLocalReplica::note_chosen, which applies the
                        // term fence and only updates the
                        // (last_chosen_slot, last_chosen_term) high-water
                        // mark — no payload is shipped, so the
                        // contiguous-chosen / contiguous-applied
                        // watermarks are intentionally untouched.
                        if let Some(group) = store.get_group(notice.group_id) {
                            let advanced = group.local_replica().note_chosen(notice.slot, notice.term);
                            debug!(
                                store_id = store.store_id,
                                group_id = notice.group_id,
                                slot = notice.slot,
                                term = notice.term,
                                leader_id = notice.leader_id,
                                advanced,
                                "LearnerStream: chosen notification applied"
                            );
                        } else {
                            debug!(
                                store_id = store.store_id,
                                group_id = notice.group_id,
                                slot = notice.slot,
                                term = notice.term,
                                "LearnerStream: chosen notification dropped (group not found)"
                            );
                        }
                        None
                    }
                };
                if let Some(frame) = response_frame {
                    if tx
                        .send(Ok(LearnerStreamResponse { frame: Some(frame) }))
                        .await
                        .is_err()
                    {
                        // Receiver dropped (peer disconnected); stop processing.
                        return;
                    }
                }
            }
        });

        let out_stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(out_stream) as Self::LearnerStreamStream))
    }
}

/// Inner `Accept` handler shared by the unary `Accept` RPC and the
/// `LearnerStream` bidi. Wire-format → in-memory conversion + delegation
/// to [`ReplicaHandler::on_accept`] + response build.
#[allow(clippy::too_many_lines)]
async fn handle_accept_inner(store: &Arc<PxKvStore>, req: AcceptRequest) -> Result<AcceptedResponse, Status> {
    let value = req.value.ok_or_else(|| {
        warn!(
            store_id = store.store_id,
            group_id = req.group_id,
            request_id = req.request_id,
            slot = req.slot,
            "accept rpc missing value; next step: check caller/protobuf conversion"
        );
        Status::invalid_argument("missing value")
    })?;
    let client_id = optional_u64(req.client_id);
    let seq = optional_u64(req.seq);
    let entry = PxLogEntry {
        slot: req.slot,
        ballot: PxBallot {
            round: req.round,
            leader_id: req.leader_id,
        },
        term: req.term,
        payload: value.payload,
    };

    let group = store
        .get_group(req.group_id)
        .ok_or_else(|| Status::not_found("px group not found"))?;
    let replica = group.local_replica();

    let responder_epoch = group.membership_epoch();
    if req.membership_epoch != responder_epoch {
        let converged_epoch = group.adopt_membership_epoch(req.membership_epoch);
        warn!(
            store_id = store.store_id,
            group_id = req.group_id,
            request_id = req.request_id,
            proposer_epoch = req.membership_epoch,
            responder_epoch,
            converged_epoch,
            "accept rejected by membership-epoch fence; adopting higher epoch from proposer"
        );
        return Ok(epoch_mismatch_accept_response(
            req.slot,
            req.round,
            req.leader_id,
            req.request_id,
            req.request_create_ms,
            replica.current_term_snapshot(),
            responder_epoch,
        ));
    }

    let reply = <crate::cluster::local_replica::PxLocalReplica as ReplicaHandler>::on_accept(
        replica,
        entry.clone(),
        req.group_id,
    )
    .await?;
    if matches!(reply, PxAcceptReply::Accepted { .. }) {
        replica.learn_chosen(&entry, client_id, seq).await;
    }

    let (rejected, rejected_round, rejected_leader_id, term_stale, reply_term) = match reply {
        PxAcceptReply::Accepted { .. } => (false, 0, 0, false, replica.current_term_snapshot()),
        PxAcceptReply::Rejected { current_promised, .. } => {
            warn!(
                store_id = store.store_id,
                group_id = req.group_id,
                request_id = req.request_id,
                slot = req.slot,
                current_round = current_promised.round,
                current_leader_id = current_promised.leader_id,
                "accept rejected; next step: proposer should run prepare with a higher ballot"
            );
            (
                true,
                current_promised.round,
                current_promised.leader_id,
                false,
                replica.current_term_snapshot(),
            )
        }
        PxAcceptReply::TermStale { new_term, .. } => {
            warn!(
                store_id = store.store_id,
                group_id = req.group_id,
                request_id = req.request_id,
                slot = req.slot,
                new_term,
                "accept rejected by term fence; proposer should step down"
            );
            (false, 0, 0, true, new_term)
        }
        PxAcceptReply::EpochMismatch { .. } => {
            // on_accept (the in-process acceptor path) never produces this
            // variant itself; it only exists on the wire-response side,
            // constructed by the early-return fence check above.
            unreachable!("on_accept does not produce EpochMismatch")
        }
    };

    Ok(AcceptedResponse {
        version: 1,
        slot: req.slot,
        round: req.round,
        leader_id: req.leader_id,
        rejected,
        rejected_round,
        rejected_leader_id,
        request_id: req.request_id,
        request_create_ms: req.request_create_ms,
        term: reply_term,
        term_stale,
        membership_epoch: responder_epoch,
        epoch_mismatch: false,
    })
}

/// Inner `Heartbeat` handler shared by the unary `Heartbeat` RPC and the
/// `LearnerStream` bidi.
async fn handle_heartbeat_inner(
    store: &Arc<PxKvStore>,
    req: HeartbeatRequest,
) -> Result<HeartbeatResponse, Status> {
    let group = store
        .get_group(req.group_id)
        .ok_or_else(|| Status::not_found("px group not found"))?;
    let replica = group.local_replica();
    let payload = HeartbeatRequestPayload {
        term: req.term,
        leader_id: req.leader_id,
        prev_log_slot: req.prev_log_slot,
        prev_log_term: req.prev_log_term,
        committed_safe_slot: req.committed_safe_slot,
        lease_grant_until_ms_mono: req.lease_grant_until_ms_mono,
        t_send_ms_mono: req.t_send_ms_mono,
    };
    let reply = <crate::cluster::local_replica::PxLocalReplica as ReplicaHandler>::on_heartbeat(
        replica,
        payload,
        req.group_id,
    )
    .await?;
    Ok(HeartbeatResponse {
        version: 1,
        group_id: req.group_id,
        term: reply.term,
        success: reply.success,
        contiguous_chosen: reply.contiguous_chosen,
        last_chosen_term: reply.last_chosen_term,
        contiguous_applied: reply.contiguous_applied,
        highest_seen_slot: reply.highest_seen_slot,
        request_id: req.request_id,
        request_create_ms: req.request_create_ms,
        durable_snapshot_slot: reply.durable_snapshot_slot,
    })
}

fn log_entry_to_proto(entry: &PxLogEntry) -> AcceptedValue {
    AcceptedValue {
        slot: entry.slot,
        round: entry.ballot.round,
        leader_id: entry.ballot.leader_id,
        term: entry.term,
        payload: entry.payload.clone(),
    }
}
