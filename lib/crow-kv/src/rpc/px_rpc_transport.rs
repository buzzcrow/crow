// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#![allow(clippy::missing_errors_doc)]
#![allow(dead_code)] // Wired in Phase 6 (LearnerStream + RemoteReplica)

//! crow-rpc client transport for the KV consensus service (R32
//! migration). Builds flatbuffer requests, sends via `RpcClient::call`,
//! awaits `CallFuture`, and parses flatbuffer responses via the
//! zero-copy `Ref` wrappers. Runs alongside the gRPC transport during
//! the mixed-rollout window; `PxRemoteReplica` selects the transport
//! based on whether `with_rpc_transport` was called.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use dashmap::DashMap;
use flatbuffers::FlatBufferBuilder;

use crow_protocol::fb::FBMsgType;
use crow_protocol::fb_wrappers::kv_consensus::{
    FBAcceptedResponseRef, FBFetchGapResponseRef, FBHeartbeatResponseRef, FBPreVoteResponseRef,
    FBPromiseResponseRef, FBRequestVoteResponseRef, FBSnapshotResponseRef, FBStepDownResponseRef,
};
use crow_protocol::kv_consensus_fb::{
    FBAcceptRequest, FBAcceptRequestArgs, FBAcceptedValue, FBAcceptedValueArgs, FBBatchChosenNotification,
    FBBatchChosenNotificationArgs, FBChosenNotification, FBChosenNotificationArgs, FBFetchGapRequest,
    FBFetchGapRequestArgs, FBHeartbeatRequest, FBHeartbeatRequestArgs, FBKvRetCode, FBPreVoteRequest,
    FBPreVoteRequestArgs, FBPrepareRequest, FBPrepareRequestArgs, FBRequestVoteRequest,
    FBRequestVoteRequestArgs, FBSnapshotRequest, FBSnapshotRequestArgs, FBStepDownRequest,
    FBStepDownRequestArgs,
};
use crow_rpc_ffi::{noop_completion, Buffer, Connection, RpcClient, RpcError, RpcServer};

use crate::cluster::replica::{
    FetchGapReply, HeartbeatReply, HeartbeatRequestPayload, PxReplicaError, StepDownReply,
    StepDownRequestPayload, VoteReply, VoteRequestPayload,
};
use crate::paxos::roles::{DedupTag, PxAcceptReply, PxBallot, PxLogEntry, PxPrepareReply};

/// crow-rpc transport for the KV consensus service. Holds the
/// client-side `RpcServer` (manages connections), `RpcClient`
/// (request/response correlation), and a `Connection` cache per
/// endpoint.
pub struct PxRpcTransport {
    server: Arc<RpcServer>,
    rpc: Arc<RpcClient>,
    connections: DashMap<String, Connection>,
    next_req_id: AtomicU64,
}

impl std::fmt::Debug for PxRpcTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PxRpcTransport")
            .field("next_req_id", &self.next_req_id.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl PxRpcTransport {
    /// Create a new crow-rpc transport. The `RpcServer` is the
    /// client-side transport — it does not listen but is used to
    /// establish connections to remote endpoints.
    #[must_use]
    pub fn new() -> Self {
        let server = Arc::new(RpcServer::new(None));
        server.start();
        let rpc = Arc::new(RpcClient::new());
        rpc.set_completion_pool_size(1024);
        Self {
            server,
            rpc,
            connections: DashMap::new(),
            next_req_id: AtomicU64::new(1),
        }
    }

    fn next_id(&self) -> u64 {
        self.next_req_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Get or create a `Connection` for the given endpoint. The
    /// crow-rpc server listens on the same port as the gRPC endpoint
    /// (no port derivation).
    fn conn_for(&self, rpc_endpoint: &str) -> Result<Connection, PxReplicaError> {
        let normalized = normalize_endpoint(rpc_endpoint);
        if let Some(conn) = self.connections.get(&normalized) {
            return Ok(conn.clone());
        }
        let (host, port) = parse_endpoint(&normalized)
            .map_err(|e| PxReplicaError::Internal(format!("rpc connect parse endpoint: {e}")))?;
        let conn = self
            .server
            .connect(&host, port)
            .map_err(|e| PxReplicaError::Internal(format!("rpc connect to {host}:{port}: {e:?}")))?;
        self.rpc.attach(&conn);
        self.connections.insert(normalized, conn.clone());
        Ok(conn)
    }

    /// Send a `Prepare` request via crow-rpc.
    pub async fn send_prepare(
        &self,
        rpc_endpoint: &str,
        slot: u64,
        ballot: PxBallot,
        term: u64,
        group_id: u64,
        membership_epoch: u64,
    ) -> Result<PxPrepareReply, PxReplicaError> {
        let req_id = self.next_id();
        let conn = self.conn_for(rpc_endpoint)?;
        let mut builder = FlatBufferBuilder::new();
        let args = FBPrepareRequestArgs {
            id: req_id,
            rpc_create_nano: 0,
            version: 1,
            slot,
            round: ballot.round,
            leader_id: ballot.leader_id,
            term,
            group_id,
            membership_epoch,
        };
        let req = FBPrepareRequest::create(&mut builder, &args);
        builder.finish(req, None);
        let control = Buffer::from_bytes(builder.finished_data());
        let msg_type = FBMsgType::EPrepareRequest.0 as u16;
        let fut = self
            .rpc
            .call(&self.server, &conn, req_id, control, None, msg_type)
            .map_err(rpc_error_to_px)?;
        let resp = fut.await.map_err(rpc_error_to_px)?;
        let ctrl = resp
            .control
            .ok_or_else(|| PxReplicaError::Internal("prepare response missing control buffer".into()))?;
        let r = FBPromiseResponseRef::new(ctrl.bytes());
        if !r.valid() {
            return Err(PxReplicaError::Internal("prepare response malformed".into()));
        }
        check_ret_code(r.ret_code(), r.error_msg())?;
        if r.epoch_mismatch() {
            Ok(PxPrepareReply::EpochMismatch {
                responder_epoch: r.membership_epoch(),
            })
        } else if r.term_stale() {
            Ok(PxPrepareReply::TermStale {
                slot: r.slot(),
                new_term: r.term(),
            })
        } else if r.rejected() {
            Ok(PxPrepareReply::Rejected {
                slot: r.slot(),
                current_promised: PxBallot::new(r.rejected_round(), r.rejected_leader_id()),
            })
        } else {
            // Promised — read the previously accepted value from the
            // nested FBAcceptedValue table so the proposer can adopt it.
            let accepted = r.previously_accepted().map(|av| PxLogEntry {
                slot: av.slot,
                ballot: PxBallot::new(av.round, av.leader_id),
                term: av.term,
                payload: av.payload,
            });
            Ok(PxPrepareReply::Promised {
                slot: r.slot(),
                accepted,
            })
        }
    }

    /// Send an `Accept` request via crow-rpc (unary — the `LearnerStream`
    /// rewrite in Phase 6 routes through `send` for fire-and-forget
    /// frames, but Accept is request-response).
    pub async fn send_accept(
        &self,
        rpc_endpoint: &str,
        entry: &PxLogEntry,
        dedup_tags: &[DedupTag],
        group_id: u64,
        membership_epoch: u64,
    ) -> Result<PxAcceptReply, PxReplicaError> {
        let req_id = self.next_id();
        let conn = self.conn_for(rpc_endpoint)?;
        let mut builder = FlatBufferBuilder::new();
        let payload_vec = entry.payload.to_vec();
        let payload = builder.create_vector(&payload_vec);
        let value = FBAcceptedValue::create(
            &mut builder,
            &FBAcceptedValueArgs {
                slot: entry.slot,
                round: entry.ballot.round,
                leader_id: entry.ballot.leader_id,
                term: entry.term,
                payload: Some(payload),
            },
        );
        let (legacy_client_id, legacy_seq) = dedup_tags.first().map_or((0, 0), |t| (t.client_id, t.seq));
        let args = FBAcceptRequestArgs {
            id: req_id,
            rpc_create_nano: 0,
            version: 1,
            slot: entry.slot,
            round: entry.ballot.round,
            leader_id: entry.ballot.leader_id,
            term: entry.term,
            value: Some(value),
            client_id: legacy_client_id,
            seq: legacy_seq,
            group_id,
            membership_epoch,
            dedup_tags: None, // TODO: build dedup_tags vector
        };
        let req = FBAcceptRequest::create(&mut builder, &args);
        builder.finish(req, None);
        let control = Buffer::from_bytes(builder.finished_data());
        let msg_type = FBMsgType::EAcceptRequest.0 as u16;
        let fut = self
            .rpc
            .call(&self.server, &conn, req_id, control, None, msg_type)
            .map_err(rpc_error_to_px)?;
        let resp = fut.await.map_err(rpc_error_to_px)?;
        let ctrl = resp
            .control
            .ok_or_else(|| PxReplicaError::Internal("accept response missing control buffer".into()))?;
        let r = FBAcceptedResponseRef::new(ctrl.bytes());
        if !r.valid() {
            return Err(PxReplicaError::Internal("accept response malformed".into()));
        }
        check_ret_code(r.ret_code(), r.error_msg())?;
        if r.epoch_mismatch() {
            Ok(PxAcceptReply::EpochMismatch {
                responder_epoch: r.membership_epoch(),
            })
        } else if r.term_stale() {
            Ok(PxAcceptReply::TermStale {
                slot: r.slot(),
                new_term: r.term(),
            })
        } else if r.rejected() {
            Ok(PxAcceptReply::Rejected {
                slot: r.slot(),
                current_promised: PxBallot::new(r.rejected_round(), r.rejected_leader_id()),
            })
        } else {
            Ok(PxAcceptReply::Accepted {
                slot: r.slot(),
                ballot: PxBallot::new(r.round(), r.leader_id()),
            })
        }
    }

    /// Send a `PreVote` request via crow-rpc.
    pub async fn send_pre_vote(
        &self,
        rpc_endpoint: &str,
        req: VoteRequestPayload,
        group_id: u64,
    ) -> Result<VoteReply, PxReplicaError> {
        let req_id = self.next_id();
        let conn = self.conn_for(rpc_endpoint)?;
        let mut builder = FlatBufferBuilder::new();
        let args = FBPreVoteRequestArgs {
            id: req_id,
            rpc_create_nano: 0,
            version: 1,
            group_id,
            term: req.term,
            candidate_id: req.candidate_id,
            accepted_log_tip_slot: req.accepted_log_tip_slot,
            accepted_log_tip_term: req.accepted_log_tip_term,
        };
        let fb_req = FBPreVoteRequest::create(&mut builder, &args);
        builder.finish(fb_req, None);
        let control = Buffer::from_bytes(builder.finished_data());
        let msg_type = FBMsgType::EPreVoteRequest.0 as u16;
        let fut = self
            .rpc
            .call(&self.server, &conn, req_id, control, None, msg_type)
            .map_err(rpc_error_to_px)?;
        let resp = fut.await.map_err(rpc_error_to_px)?;
        let ctrl = resp
            .control
            .ok_or_else(|| PxReplicaError::Internal("pre_vote response missing control buffer".into()))?;
        let r = FBPreVoteResponseRef::new(ctrl.bytes());
        if !r.valid() {
            return Err(PxReplicaError::Internal("pre_vote response malformed".into()));
        }
        check_ret_code(r.ret_code(), r.error_msg())?;
        Ok(VoteReply {
            term: r.term(),
            granted: r.granted(),
            contiguous_chosen: r.contiguous_chosen(),
            last_chosen_term: r.last_chosen_term(),
            highest_seen_slot: r.highest_seen_slot(),
        })
    }

    /// Send a `RequestVote` request via crow-rpc.
    pub async fn send_request_vote(
        &self,
        rpc_endpoint: &str,
        req: VoteRequestPayload,
        group_id: u64,
    ) -> Result<VoteReply, PxReplicaError> {
        let req_id = self.next_id();
        let conn = self.conn_for(rpc_endpoint)?;
        let mut builder = FlatBufferBuilder::new();
        let args = FBRequestVoteRequestArgs {
            id: req_id,
            rpc_create_nano: 0,
            version: 1,
            group_id,
            term: req.term,
            candidate_id: req.candidate_id,
            accepted_log_tip_slot: req.accepted_log_tip_slot,
            accepted_log_tip_term: req.accepted_log_tip_term,
        };
        let fb_req = FBRequestVoteRequest::create(&mut builder, &args);
        builder.finish(fb_req, None);
        let control = Buffer::from_bytes(builder.finished_data());
        let msg_type = FBMsgType::ERequestVoteRequest.0 as u16;
        let fut = self
            .rpc
            .call(&self.server, &conn, req_id, control, None, msg_type)
            .map_err(rpc_error_to_px)?;
        let resp = fut.await.map_err(rpc_error_to_px)?;
        let ctrl = resp
            .control
            .ok_or_else(|| PxReplicaError::Internal("request_vote response missing control buffer".into()))?;
        let r = FBRequestVoteResponseRef::new(ctrl.bytes());
        if !r.valid() {
            return Err(PxReplicaError::Internal("request_vote response malformed".into()));
        }
        check_ret_code(r.ret_code(), r.error_msg())?;
        Ok(VoteReply {
            term: r.term(),
            granted: r.granted(),
            contiguous_chosen: r.contiguous_chosen(),
            last_chosen_term: r.last_chosen_term(),
            highest_seen_slot: r.highest_seen_slot(),
        })
    }

    /// Send a `Heartbeat` request via crow-rpc.
    pub async fn send_heartbeat(
        &self,
        rpc_endpoint: &str,
        req: HeartbeatRequestPayload,
        group_id: u64,
    ) -> Result<HeartbeatReply, PxReplicaError> {
        let req_id = self.next_id();
        let conn = self.conn_for(rpc_endpoint)?;
        let mut builder = FlatBufferBuilder::new();
        let args = FBHeartbeatRequestArgs {
            id: req_id,
            rpc_create_nano: 0,
            version: 1,
            group_id,
            term: req.term,
            leader_id: req.leader_id,
            prev_log_slot: req.prev_log_slot,
            prev_log_term: req.prev_log_term,
            committed_safe_slot: req.committed_safe_slot,
            lease_grant_until_ms_mono: req.lease_grant_until_ms_mono,
            t_send_ms_mono: req.t_send_ms_mono,
        };
        let fb_req = FBHeartbeatRequest::create(&mut builder, &args);
        builder.finish(fb_req, None);
        let control = Buffer::from_bytes(builder.finished_data());
        let msg_type = FBMsgType::EHeartbeatRequest.0 as u16;
        let fut = self
            .rpc
            .call(&self.server, &conn, req_id, control, None, msg_type)
            .map_err(rpc_error_to_px)?;
        let resp = fut.await.map_err(rpc_error_to_px)?;
        let ctrl = resp
            .control
            .ok_or_else(|| PxReplicaError::Internal("heartbeat response missing control buffer".into()))?;
        let r = FBHeartbeatResponseRef::new(ctrl.bytes());
        if !r.valid() {
            return Err(PxReplicaError::Internal("heartbeat response malformed".into()));
        }
        check_ret_code(r.ret_code(), r.error_msg())?;
        Ok(HeartbeatReply {
            term: r.term(),
            success: r.success(),
            contiguous_chosen: r.contiguous_chosen(),
            last_chosen_term: r.last_chosen_term(),
            contiguous_applied: r.contiguous_applied(),
            highest_seen_slot: r.highest_seen_slot(),
            durable_snapshot_slot: r.durable_snapshot_slot(),
        })
    }

    /// Send a `StepDown` request via crow-rpc.
    pub async fn send_step_down(
        &self,
        rpc_endpoint: &str,
        req: &StepDownRequestPayload,
        group_id: u64,
    ) -> Result<StepDownReply, PxReplicaError> {
        let req_id = self.next_id();
        let conn = self.conn_for(rpc_endpoint)?;
        let mut builder = FlatBufferBuilder::new();
        let reason = builder.create_string(&req.reason);
        let args = FBStepDownRequestArgs {
            id: req_id,
            rpc_create_nano: 0,
            version: 1,
            group_id,
            term: req.term,
            target_leader_id: req.target_leader_id,
            reason: Some(reason),
        };
        let fb_req = FBStepDownRequest::create(&mut builder, &args);
        builder.finish(fb_req, None);
        let control = Buffer::from_bytes(builder.finished_data());
        let msg_type = FBMsgType::EStepDownRequest.0 as u16;
        let fut = self
            .rpc
            .call(&self.server, &conn, req_id, control, None, msg_type)
            .map_err(rpc_error_to_px)?;
        let resp = fut.await.map_err(rpc_error_to_px)?;
        let ctrl = resp
            .control
            .ok_or_else(|| PxReplicaError::Internal("step_down response missing control buffer".into()))?;
        let r = FBStepDownResponseRef::new(ctrl.bytes());
        if !r.valid() {
            return Err(PxReplicaError::Internal("step_down response malformed".into()));
        }
        check_ret_code(r.ret_code(), r.error_msg())?;
        Ok(StepDownReply {
            accepted: r.accepted(),
            current_term: r.current_term(),
            current_leader_id: r.current_leader_id(),
        })
    }

    /// Send a `FetchGap` request via crow-rpc.
    pub async fn send_fetch_gap(
        &self,
        rpc_endpoint: &str,
        group_id: u64,
        slot: u64,
        term: u64,
        leader_id: u64,
    ) -> Result<FetchGapReply, PxReplicaError> {
        let req_id = self.next_id();
        let conn = self.conn_for(rpc_endpoint)?;
        let mut builder = FlatBufferBuilder::new();
        let args = FBFetchGapRequestArgs {
            id: req_id,
            rpc_create_nano: 0,
            version: 1,
            group_id,
            slot,
            term,
            leader_id,
        };
        let fb_req = FBFetchGapRequest::create(&mut builder, &args);
        builder.finish(fb_req, None);
        let control = Buffer::from_bytes(builder.finished_data());
        let msg_type = FBMsgType::EFetchGapRequest.0 as u16;
        let fut = self
            .rpc
            .call(&self.server, &conn, req_id, control, None, msg_type)
            .map_err(rpc_error_to_px)?;
        let resp = fut.await.map_err(rpc_error_to_px)?;
        let ctrl = resp
            .control
            .ok_or_else(|| PxReplicaError::Internal("fetch_gap response missing control buffer".into()))?;
        let r = FBFetchGapResponseRef::new(ctrl.bytes());
        if !r.valid() {
            return Err(PxReplicaError::Internal("fetch_gap response malformed".into()));
        }
        check_ret_code(r.ret_code(), r.error_msg())?;
        let payload = r.payload().map(bytes::Bytes::copy_from_slice).unwrap_or_default();
        Ok(FetchGapReply {
            group_id: r.group_id(),
            slot: r.slot(),
            term: r.term(),
            ballot_round: r.ballot_round(),
            leader_id: r.leader_id(),
            payload,
        })
    }

    /// Send a fire-and-forget `ChosenNotification` via crow-rpc. No
    /// reply is expected — the frame is sent with no completion callback.
    pub fn send_chosen(
        &self,
        rpc_endpoint: &str,
        group_id: u64,
        slot: u64,
        term: u64,
        leader_id: u64,
        ballot_round: u64,
    ) -> Result<(), PxReplicaError> {
        let req_id = self.next_id();
        let conn = self.conn_for(rpc_endpoint)?;
        let mut builder = FlatBufferBuilder::new();
        let args = FBChosenNotificationArgs {
            id: req_id,
            rpc_create_nano: 0,
            version: 1,
            group_id,
            slot,
            term,
            leader_id,
            ballot_round,
        };
        let fb_req = FBChosenNotification::create(&mut builder, &args);
        builder.finish(fb_req, None);
        let control = Buffer::from_bytes(builder.finished_data());
        let msg_type = FBMsgType::EChosenNotification.0 as u16;
        self.rpc
            .send(
                &self.server,
                &conn,
                req_id,
                control,
                None,
                msg_type,
                noop_completion(),
                std::ptr::null_mut(),
            )
            .map_err(rpc_error_to_px)
    }

    /// Send a fire-and-forget `BatchChosenNotification` via crow-rpc.
    #[allow(clippy::too_many_arguments)]
    pub fn send_batch_chosen(
        &self,
        rpc_endpoint: &str,
        group_id: u64,
        start_slot: u64,
        end_slot: u64,
        term: u64,
        leader_id: u64,
        ballot_round: u64,
    ) -> Result<(), PxReplicaError> {
        let req_id = self.next_id();
        let conn = self.conn_for(rpc_endpoint)?;
        let mut builder = FlatBufferBuilder::new();
        let args = FBBatchChosenNotificationArgs {
            id: req_id,
            rpc_create_nano: 0,
            version: 1,
            group_id,
            start_slot,
            end_slot,
            term,
            leader_id,
            ballot_round,
        };
        let fb_req = FBBatchChosenNotification::create(&mut builder, &args);
        builder.finish(fb_req, None);
        let control = Buffer::from_bytes(builder.finished_data());
        let msg_type = FBMsgType::EBatchChosenNotification.0 as u16;
        self.rpc
            .send(
                &self.server,
                &conn,
                req_id,
                control,
                None,
                msg_type,
                noop_completion(),
                std::ptr::null_mut(),
            )
            .map_err(rpc_error_to_px)
    }

    /// Request a snapshot from a peer via crow-rpc. The response carries
    /// header info (`term_at_slot`, `membership_epoch`, `at_slot`) in the
    /// control buffer and the full snapshot bytes in the data buffer.
    pub async fn send_snapshot(
        &self,
        rpc_endpoint: &str,
        group_id: u64,
    ) -> Result<SnapshotReply, PxReplicaError> {
        let req_id = self.next_id();
        let conn = self.conn_for(rpc_endpoint)?;
        let mut builder = FlatBufferBuilder::new();
        let args = FBSnapshotRequestArgs {
            id: req_id,
            rpc_create_nano: 0,
            group_id,
        };
        let fb_req = FBSnapshotRequest::create(&mut builder, &args);
        builder.finish(fb_req, None);
        let control = Buffer::from_bytes(builder.finished_data());
        let msg_type = FBMsgType::ESnapshotRequest.0 as u16;
        let fut = self
            .rpc
            .call(&self.server, &conn, req_id, control, None, msg_type)
            .map_err(rpc_error_to_px)?;
        let resp = fut.await.map_err(rpc_error_to_px)?;
        let ctrl = resp
            .control
            .ok_or_else(|| PxReplicaError::Internal("snapshot response missing control buffer".into()))?;
        let r = FBSnapshotResponseRef::new(ctrl.bytes());
        if !r.valid() {
            return Err(PxReplicaError::Internal("snapshot response malformed".into()));
        }
        check_ret_code(r.ret_code(), r.error_msg())?;
        let data = resp
            .data
            .map(|d| bytes::Bytes::copy_from_slice(d.bytes()))
            .unwrap_or_default();
        Ok(SnapshotReply {
            group_id: r.group_id(),
            term_at_slot: r.term_at_slot(),
            membership_epoch: r.membership_epoch(),
            at_slot: r.at_slot(),
            data,
        })
    }

    /// Get the underlying `RpcServer` (for the `LearnerStream` to share
    /// the connection pool).
    pub(crate) fn server(&self) -> &Arc<RpcServer> {
        &self.server
    }

    /// Get the underlying `RpcClient` (for the `LearnerStream` to share
    /// the response correlation).
    pub(crate) fn rpc(&self) -> &Arc<RpcClient> {
        &self.rpc
    }

    /// Get or create a connection for an endpoint (exposed for the
    /// `LearnerStream` to share the connection pool).
    pub(crate) fn get_conn(&self, rpc_endpoint: &str) -> Result<Connection, PxReplicaError> {
        self.conn_for(rpc_endpoint)
    }

    /// Allocate a new request ID (exposed for the `LearnerStream`).
    pub(crate) fn alloc_id(&self) -> u64 {
        self.next_id()
    }
}

impl Default for PxRpcTransport {
    fn default() -> Self {
        Self::new()
    }
}

/// Snapshot reply: header info + exported bytes.
pub struct SnapshotReply {
    pub group_id: u64,
    pub term_at_slot: u64,
    pub membership_epoch: u64,
    pub at_slot: u64,
    pub data: bytes::Bytes,
}

// ── Error mapping ────────────────────────────────────────────────

fn rpc_error_to_px(e: RpcError) -> PxReplicaError {
    PxReplicaError::Internal(format!("crow-rpc error: {e:?}"))
}

fn check_ret_code(code: FBKvRetCode, msg: Option<&str>) -> Result<(), PxReplicaError> {
    match code {
        FBKvRetCode::Success => Ok(()),
        FBKvRetCode::NotFound => Err(PxReplicaError::GroupNotFound(0)),
        FBKvRetCode::Unavailable => Err(PxReplicaError::ShuttingDown),
        FBKvRetCode::Internal | FBKvRetCode::InvalidArgument => {
            Err(PxReplicaError::Internal(msg.unwrap_or("internal error").into()))
        }
        _ => Err(PxReplicaError::Internal(format!("unknown ret_code: {code:?}"))),
    }
}

// ── Endpoint parsing ─────────────────────────────────────────────

/// Normalize a service-registry endpoint: prepend `http://` if no
/// scheme, rewrite `0.0.0.0` to `127.0.0.1`.
fn normalize_endpoint(endpoint: &str) -> String {
    let with_scheme = if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        endpoint.to_string()
    } else {
        format!("http://{endpoint}")
    };
    with_scheme.replacen("://0.0.0.0:", "://127.0.0.1:", 1)
}

/// Parse `http://host:port` into `(host, port)`.
fn parse_endpoint(endpoint: &str) -> Result<(String, i32), String> {
    let without_scheme = endpoint
        .strip_prefix("http://")
        .or_else(|| endpoint.strip_prefix("https://"))
        .unwrap_or(endpoint);
    let (host, port_str) = without_scheme
        .rsplit_once(':')
        .ok_or_else(|| format!("invalid endpoint: {endpoint}"))?;
    let port: i32 = port_str
        .parse()
        .map_err(|_| format!("invalid port in endpoint: {endpoint}"))?;
    Ok((host.to_string(), port))
}
