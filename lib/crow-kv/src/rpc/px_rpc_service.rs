// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// `submit_response` takes a raw `conn_handle` from the FFI dispatch
// callback — the unsafe is inherent to the FFI boundary (the pointer
// is a valid `Connection*` for the duration of the callback, verified
// by the C++ transport). Confined to `submit_response` calls.
#![allow(unsafe_code)]
#![allow(dead_code)] // Wired in Phase 7 (server wiring)

//! crow-rpc handler set for the KV consensus service (R32 migration).
//!
//! Each handler dispatches by `msg_type` to the existing consensus
//! logic — the same logic bodies as the former `PxReplicaService` in
//! `px_service.rs`. The response is a flatbuffer frame built per
//! `design-crow-rpc.md` §6 (build → finish → attach) and submitted via
//! `RpcServer::submit_response`.
//!
//! Handlers run on the C++ I/O worker thread. Synchronous paths
//! (validation, epoch fence checks) run inline; async paths (Paxos
//! acceptor calls) spawn a tokio task via the captured `Handle` and
//! submit the response from the task. Each handler closure captures an
//! `Arc<RpcServer>` so it can submit responses from either the dispatch
//! thread (sync error path) or the spawned task (async success path).

use std::sync::Arc;

use crow_protocol::fb::FBMsgType;
use crow_protocol::kv_consensus_fb::{
    FBAcceptRequest, FBAcceptedResponse, FBAcceptedResponseArgs, FBAcceptedValue, FBAcceptedValueArgs,
    FBBatchChosenNotification, FBChosenNotification, FBFetchGapRequest, FBFetchGapResponse,
    FBFetchGapResponseArgs, FBHeartbeatRequest, FBHeartbeatResponse, FBHeartbeatResponseArgs, FBKvRetCode,
    FBPreVoteRequest, FBPreVoteResponse, FBPreVoteResponseArgs, FBPrepareRequest, FBPromiseResponse,
    FBPromiseResponseArgs, FBRequestVoteRequest, FBRequestVoteResponse, FBRequestVoteResponseArgs,
    FBSnapshotRequest, FBSnapshotResponse, FBSnapshotResponseArgs, FBStepDownRequest, FBStepDownResponse,
    FBStepDownResponseArgs,
};
use crow_rpc_ffi::{Buffer, RpcServer, ServerRequest};
use flatbuffers::FlatBufferBuilder;
use tokio::runtime::Handle;
use tracing::{debug, warn};

use crate::cluster::local_replica::PxLocalReplica;
use crate::cluster::px_kv_store::PxKvStore;
use crate::cluster::replica::{
    HeartbeatRequestPayload, PxReplicaError, ReplicaHandler, StepDownRequestPayload, VoteRequestPayload,
};
use crate::paxos::roles::{DedupTag, PxAcceptReply, PxBallot, PxLogEntry, PxPrepareReply};

/// crow-rpc handler set for the KV consensus service. Holds the same
/// dependencies as the former `PxReplicaService` plus a tokio `Handle`
/// for spawning async work from the C++ I/O thread callback.
pub struct PxRpcService {
    store: Arc<PxKvStore>,
    rt: Handle,
}

impl PxRpcService {
    pub(crate) fn new(store: Arc<PxKvStore>, rt: Handle) -> Self {
        Self { store, rt }
    }

    /// Register all consensus request handlers into the `RpcServer`.
    pub(crate) fn register_handlers(self: &Arc<Self>, server: &Arc<RpcServer>) {
        server.register_handler(
            FBMsgType::EPrepareRequest.0 as u16,
            Self::make_handler(Arc::clone(self), Arc::clone(server), Self::handle_prepare),
        );
        server.register_handler(
            FBMsgType::EAcceptRequest.0 as u16,
            Self::make_handler(Arc::clone(self), Arc::clone(server), Self::handle_accept),
        );
        server.register_handler(
            FBMsgType::EPreVoteRequest.0 as u16,
            Self::make_handler(Arc::clone(self), Arc::clone(server), Self::handle_pre_vote),
        );
        server.register_handler(
            FBMsgType::ERequestVoteRequest.0 as u16,
            Self::make_handler(Arc::clone(self), Arc::clone(server), Self::handle_request_vote),
        );
        server.register_handler(
            FBMsgType::EHeartbeatRequest.0 as u16,
            Self::make_handler(Arc::clone(self), Arc::clone(server), Self::handle_heartbeat),
        );
        server.register_handler(
            FBMsgType::EStepDownRequest.0 as u16,
            Self::make_handler(Arc::clone(self), Arc::clone(server), Self::handle_step_down),
        );
        server.register_handler(
            FBMsgType::EChosenNotification.0 as u16,
            Self::make_handler(Arc::clone(self), Arc::clone(server), Self::handle_chosen_notice),
        );
        server.register_handler(
            FBMsgType::EBatchChosenNotification.0 as u16,
            Self::make_handler(Arc::clone(self), Arc::clone(server), Self::handle_batch_chosen),
        );
        server.register_handler(
            FBMsgType::EFetchGapRequest.0 as u16,
            Self::make_handler(Arc::clone(self), Arc::clone(server), Self::handle_fetch_gap),
        );
        server.register_handler(
            FBMsgType::ESnapshotRequest.0 as u16,
            Self::make_handler(Arc::clone(self), Arc::clone(server), Self::handle_snapshot),
        );
    }

    fn make_handler(
        this: Arc<Self>,
        server: Arc<RpcServer>,
        f: fn(&Self, &ServerRequest<'_>, &Arc<RpcServer>),
    ) -> impl Fn(ServerRequest<'_>) + Send + 'static {
        move |req| {
            f(&this, &req, &server);
        }
    }

    // ── Prepare ──────────────────────────────────────────────────

    #[allow(clippy::too_many_lines)]
    fn handle_prepare(&self, req: &ServerRequest<'_>, server: &Arc<RpcServer>) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let msg_type = FBMsgType::EPromiseResponse.0 as u16;

        let Ok(fb_req) = flatbuffers::root::<FBPrepareRequest>(req.control) else {
            submit_error(
                server,
                req.conn_handle,
                req_id,
                create_nano,
                msg_type,
                FBKvRetCode::InvalidArgument,
                "invalid request flatbuffer",
            );
            return;
        };
        let group_id = fb_req.group_id();
        let slot = fb_req.slot();
        let round = fb_req.round();
        let leader_id = fb_req.leader_id();
        let term = fb_req.term();
        let membership_epoch = fb_req.membership_epoch();

        let Some(group) = self.store.get_group(group_id) else {
            submit_error(
                server,
                req.conn_handle,
                req_id,
                create_nano,
                msg_type,
                FBKvRetCode::NotFound,
                "px group not found",
            );
            return;
        };
        let responder_epoch = group.membership_epoch();

        // Membership-epoch fence (same as px_service.rs L131-147).
        if membership_epoch != responder_epoch {
            let converged_epoch = group.adopt_membership_epoch(membership_epoch);
            warn!(
                store_id = self.store.store_id,
                group_id,
                slot,
                round,
                leader_id,
                proposer_epoch = membership_epoch,
                responder_epoch,
                converged_epoch,
                "prepare rejected by membership-epoch fence; adopting higher epoch from proposer"
            );
            let term = group.local_replica().current_term_snapshot();
            let ctrl = build_promise_response(
                req_id,
                create_nano,
                FBKvRetCode::Success,
                None,
                1,
                slot,
                round,
                leader_id,
                None,
                false,
                0,
                0,
                term,
                false,
                responder_epoch,
                true,
            );
            unsafe {
                let _ = server.submit_response(req.conn_handle, &ctrl, None, msg_type, req_id);
            }
            return;
        }

        let ballot = PxBallot { round, leader_id };
        let store = Arc::clone(&self.store);
        let conn_handle_usize = req.conn_handle as usize;
        let server = Arc::clone(server);
        self.rt.spawn(async move {
            let replica = group.local_replica();
            let reply =
                <PxLocalReplica as ReplicaHandler>::on_prepare(replica, slot, ballot, term, group_id).await;
            let ctrl = match reply {
                Ok(PxPrepareReply::Promised { slot, accepted }) => {
                    let mut builder = FlatBufferBuilder::new();
                    let prev_accepted_off = accepted.as_ref().map(|entry| {
                        let payload = builder.create_vector(&entry.payload);
                        FBAcceptedValue::create(
                            &mut builder,
                            &FBAcceptedValueArgs {
                                slot: entry.slot,
                                round: entry.ballot.round,
                                leader_id: entry.ballot.leader_id,
                                term: entry.term,
                                payload: Some(payload),
                            },
                        )
                    });
                    let term = replica.current_term_snapshot();
                    finish_promise_response(
                        &mut builder,
                        req_id,
                        create_nano,
                        FBKvRetCode::Success,
                        None,
                        1,
                        slot,
                        round,
                        leader_id,
                        prev_accepted_off,
                        false,
                        0,
                        0,
                        term,
                        false,
                        responder_epoch,
                        false,
                    )
                }
                Ok(PxPrepareReply::TermStale { slot, new_term }) => build_promise_response(
                    req_id,
                    create_nano,
                    FBKvRetCode::Success,
                    None,
                    1,
                    slot,
                    round,
                    leader_id,
                    None,
                    false,
                    0,
                    0,
                    new_term,
                    true,
                    responder_epoch,
                    false,
                ),
                Ok(PxPrepareReply::Rejected {
                    slot,
                    current_promised,
                }) => {
                    warn!(
                        store_id = store.store_id,
                        group_id,
                        slot,
                        current_round = current_promised.round,
                        current_leader_id = current_promised.leader_id,
                        "prepare rejected; next step: proposer should retry with a higher ballot"
                    );
                    let term = replica.current_term_snapshot();
                    build_promise_response(
                        req_id,
                        create_nano,
                        FBKvRetCode::Success,
                        None,
                        1,
                        slot,
                        round,
                        leader_id,
                        None,
                        true,
                        current_promised.round,
                        current_promised.leader_id,
                        term,
                        false,
                        responder_epoch,
                        false,
                    )
                }
                Ok(PxPrepareReply::EpochMismatch { .. }) => {
                    unreachable!("on_prepare does not produce EpochMismatch")
                }
                Err(e) => {
                    let (code, msg) = px_error_to_ret_code(&e);
                    build_promise_response(
                        req_id,
                        create_nano,
                        code,
                        Some(&msg),
                        1,
                        slot,
                        round,
                        leader_id,
                        None,
                        false,
                        0,
                        0,
                        replica.current_term_snapshot(),
                        false,
                        responder_epoch,
                        false,
                    )
                }
            };
            let conn_handle = conn_handle_usize as *mut std::ffi::c_void;
            unsafe {
                let _ = server.submit_response(conn_handle, &ctrl, None, msg_type, req_id);
            }
        });
    }

    // ── Accept ───────────────────────────────────────────────────

    #[allow(clippy::too_many_lines)]
    fn handle_accept(&self, req: &ServerRequest<'_>, server: &Arc<RpcServer>) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let msg_type = FBMsgType::EAcceptedResponse.0 as u16;

        let Ok(fb_req) = flatbuffers::root::<FBAcceptRequest>(req.control) else {
            submit_error(
                server,
                req.conn_handle,
                req_id,
                create_nano,
                msg_type,
                FBKvRetCode::InvalidArgument,
                "invalid request flatbuffer",
            );
            return;
        };
        let group_id = fb_req.group_id();
        let slot = fb_req.slot();
        let round = fb_req.round();
        let leader_id = fb_req.leader_id();
        let term = fb_req.term();
        let membership_epoch = fb_req.membership_epoch();

        let Some(fb_value) = fb_req.value() else {
            submit_error(
                server,
                req.conn_handle,
                req_id,
                create_nano,
                msg_type,
                FBKvRetCode::InvalidArgument,
                "missing value",
            );
            return;
        };
        let payload_bytes: Vec<u8> = fb_value.payload().map(|v| v.bytes().to_vec()).unwrap_or_default();
        let entry = PxLogEntry {
            slot,
            ballot: PxBallot { round, leader_id },
            term,
            payload: payload_bytes.into(),
        };

        // Dedup tags: prefer repeated dedup_tags, fall back to legacy client_id/seq.
        let dedup_tags: Vec<DedupTag> = if let Some(tags) = fb_req.dedup_tags() {
            tags.iter()
                .map(|t| DedupTag {
                    client_id: t.client_id(),
                    seq: t.seq(),
                })
                .collect()
        } else {
            let cid = fb_req.client_id();
            let seq = fb_req.seq();
            if cid != 0 || seq != 0 {
                vec![DedupTag { client_id: cid, seq }]
            } else {
                Vec::new()
            }
        };

        let Some(group) = self.store.get_group(group_id) else {
            submit_error(
                server,
                req.conn_handle,
                req_id,
                create_nano,
                msg_type,
                FBKvRetCode::NotFound,
                "px group not found",
            );
            return;
        };
        let responder_epoch = group.membership_epoch();

        if membership_epoch != responder_epoch {
            let converged_epoch = group.adopt_membership_epoch(membership_epoch);
            warn!(
                store_id = self.store.store_id,
                group_id,
                slot,
                round,
                leader_id,
                proposer_epoch = membership_epoch,
                responder_epoch,
                converged_epoch,
                "accept rejected by membership-epoch fence; adopting higher epoch from proposer"
            );
            let term = group.local_replica().current_term_snapshot();
            let ctrl = build_accepted_response(
                req_id,
                create_nano,
                FBKvRetCode::Success,
                None,
                1,
                slot,
                round,
                leader_id,
                false,
                0,
                0,
                term,
                false,
                responder_epoch,
                true,
            );
            unsafe {
                let _ = server.submit_response(req.conn_handle, &ctrl, None, msg_type, req_id);
            }
            return;
        }

        let store = Arc::clone(&self.store);
        let conn_handle_usize = req.conn_handle as usize;
        let server = Arc::clone(server);
        self.rt.spawn(async move {
            let replica = group.local_replica();
            let reply = <PxLocalReplica as ReplicaHandler>::on_accept(replica, &entry, group_id).await;
            if matches!(reply, Ok(PxAcceptReply::Accepted { .. })) {
                replica.learner.record_dedup_tags(&dedup_tags, entry.slot);
            }
            let (rejected, rejected_round, rejected_leader_id, term_stale, reply_term) = match reply {
                Ok(PxAcceptReply::Accepted { .. }) => (false, 0, 0, false, replica.current_term_snapshot()),
                Ok(PxAcceptReply::Rejected { current_promised, .. }) => {
                    warn!(
                        store_id = store.store_id,
                        group_id,
                        slot,
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
                Ok(PxAcceptReply::TermStale { new_term, .. }) => {
                    warn!(
                        store_id = store.store_id,
                        group_id, slot, new_term, "accept rejected by term fence; proposer should step down"
                    );
                    (false, 0, 0, true, new_term)
                }
                Ok(PxAcceptReply::EpochMismatch { .. }) => {
                    unreachable!("on_accept does not produce EpochMismatch")
                }
                Err(e) => {
                    let (code, msg) = px_error_to_ret_code(&e);
                    let ctrl = build_accepted_response(
                        req_id,
                        create_nano,
                        code,
                        Some(&msg),
                        1,
                        slot,
                        round,
                        leader_id,
                        false,
                        0,
                        0,
                        replica.current_term_snapshot(),
                        false,
                        responder_epoch,
                        false,
                    );
                    let conn_handle = conn_handle_usize as *mut std::ffi::c_void;
                    unsafe {
                        let _ = server.submit_response(conn_handle, &ctrl, None, msg_type, req_id);
                    }
                    return;
                }
            };
            let ctrl = build_accepted_response(
                req_id,
                create_nano,
                FBKvRetCode::Success,
                None,
                1,
                slot,
                round,
                leader_id,
                rejected,
                rejected_round,
                rejected_leader_id,
                reply_term,
                term_stale,
                responder_epoch,
                false,
            );
            let conn_handle = conn_handle_usize as *mut std::ffi::c_void;
            unsafe {
                let _ = server.submit_response(conn_handle, &ctrl, None, msg_type, req_id);
            }
        });
    }

    // ── PreVote ──────────────────────────────────────────────────

    fn handle_pre_vote(&self, req: &ServerRequest<'_>, server: &Arc<RpcServer>) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let msg_type = FBMsgType::EPreVoteResponse.0 as u16;

        let Ok(fb_req) = flatbuffers::root::<FBPreVoteRequest>(req.control) else {
            submit_error(
                server,
                req.conn_handle,
                req_id,
                create_nano,
                msg_type,
                FBKvRetCode::InvalidArgument,
                "invalid request flatbuffer",
            );
            return;
        };
        let group_id = fb_req.group_id();
        let Some(group) = self.store.get_group(group_id) else {
            submit_error(
                server,
                req.conn_handle,
                req_id,
                create_nano,
                msg_type,
                FBKvRetCode::NotFound,
                "px group not found",
            );
            return;
        };
        let payload = VoteRequestPayload {
            term: fb_req.term(),
            candidate_id: fb_req.candidate_id(),
            accepted_log_tip_slot: fb_req.accepted_log_tip_slot(),
            accepted_log_tip_term: fb_req.accepted_log_tip_term(),
        };
        let conn_handle_usize = req.conn_handle as usize;
        let server = Arc::clone(server);
        self.rt.spawn(async move {
            let replica = group.local_replica();
            let reply = <PxLocalReplica as ReplicaHandler>::on_pre_vote(replica, payload, group_id).await;
            let ctrl = match reply {
                Ok(r) => build_pre_vote_response(
                    req_id,
                    create_nano,
                    FBKvRetCode::Success,
                    None,
                    1,
                    group_id,
                    r.term,
                    r.granted,
                    r.contiguous_chosen,
                    r.last_chosen_term,
                    r.highest_seen_slot,
                ),
                Err(e) => {
                    let (code, msg) = px_error_to_ret_code(&e);
                    build_pre_vote_response(
                        req_id,
                        create_nano,
                        code,
                        Some(&msg),
                        1,
                        group_id,
                        0,
                        false,
                        0,
                        0,
                        0,
                    )
                }
            };
            let conn_handle = conn_handle_usize as *mut std::ffi::c_void;
            unsafe {
                let _ = server.submit_response(conn_handle, &ctrl, None, msg_type, req_id);
            }
        });
    }

    // ── RequestVote ──────────────────────────────────────────────

    fn handle_request_vote(&self, req: &ServerRequest<'_>, server: &Arc<RpcServer>) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let msg_type = FBMsgType::ERequestVoteResponse.0 as u16;

        let Ok(fb_req) = flatbuffers::root::<FBRequestVoteRequest>(req.control) else {
            submit_error(
                server,
                req.conn_handle,
                req_id,
                create_nano,
                msg_type,
                FBKvRetCode::InvalidArgument,
                "invalid request flatbuffer",
            );
            return;
        };
        let group_id = fb_req.group_id();
        let Some(group) = self.store.get_group(group_id) else {
            submit_error(
                server,
                req.conn_handle,
                req_id,
                create_nano,
                msg_type,
                FBKvRetCode::NotFound,
                "px group not found",
            );
            return;
        };
        let payload = VoteRequestPayload {
            term: fb_req.term(),
            candidate_id: fb_req.candidate_id(),
            accepted_log_tip_slot: fb_req.accepted_log_tip_slot(),
            accepted_log_tip_term: fb_req.accepted_log_tip_term(),
        };
        let conn_handle_usize = req.conn_handle as usize;
        let server = Arc::clone(server);
        self.rt.spawn(async move {
            let replica = group.local_replica();
            let reply = <PxLocalReplica as ReplicaHandler>::on_request_vote(replica, payload, group_id).await;
            let ctrl = match reply {
                Ok(r) => build_request_vote_response(
                    req_id,
                    create_nano,
                    FBKvRetCode::Success,
                    None,
                    1,
                    group_id,
                    r.term,
                    r.granted,
                    r.contiguous_chosen,
                    r.last_chosen_term,
                    r.highest_seen_slot,
                ),
                Err(e) => {
                    let (code, msg) = px_error_to_ret_code(&e);
                    build_request_vote_response(
                        req_id,
                        create_nano,
                        code,
                        Some(&msg),
                        1,
                        group_id,
                        0,
                        false,
                        0,
                        0,
                        0,
                    )
                }
            };
            let conn_handle = conn_handle_usize as *mut std::ffi::c_void;
            unsafe {
                let _ = server.submit_response(conn_handle, &ctrl, None, msg_type, req_id);
            }
        });
    }

    // ── Heartbeat ────────────────────────────────────────────────

    fn handle_heartbeat(&self, req: &ServerRequest<'_>, server: &Arc<RpcServer>) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let msg_type = FBMsgType::EHeartbeatResponse.0 as u16;

        let Ok(fb_req) = flatbuffers::root::<FBHeartbeatRequest>(req.control) else {
            submit_error(
                server,
                req.conn_handle,
                req_id,
                create_nano,
                msg_type,
                FBKvRetCode::InvalidArgument,
                "invalid request flatbuffer",
            );
            return;
        };
        let group_id = fb_req.group_id();
        let Some(group) = self.store.get_group(group_id) else {
            submit_error(
                server,
                req.conn_handle,
                req_id,
                create_nano,
                msg_type,
                FBKvRetCode::NotFound,
                "px group not found",
            );
            return;
        };
        let payload = HeartbeatRequestPayload {
            term: fb_req.term(),
            leader_id: fb_req.leader_id(),
            prev_log_slot: fb_req.prev_log_slot(),
            prev_log_term: fb_req.prev_log_term(),
            committed_safe_slot: fb_req.committed_safe_slot(),
            lease_grant_until_ms_mono: fb_req.lease_grant_until_ms_mono(),
            t_send_ms_mono: fb_req.t_send_ms_mono(),
        };
        let conn_handle_usize = req.conn_handle as usize;
        let server = Arc::clone(server);
        self.rt.spawn(async move {
            let replica = group.local_replica();
            let reply = <PxLocalReplica as ReplicaHandler>::on_heartbeat(replica, payload, group_id).await;
            let ctrl = match reply {
                Ok(r) => build_heartbeat_response(
                    req_id,
                    create_nano,
                    FBKvRetCode::Success,
                    None,
                    1,
                    group_id,
                    r.term,
                    r.success,
                    r.contiguous_chosen,
                    r.last_chosen_term,
                    r.contiguous_applied,
                    r.highest_seen_slot,
                    r.durable_snapshot_slot,
                ),
                Err(e) => {
                    let (code, msg) = px_error_to_ret_code(&e);
                    build_heartbeat_response(
                        req_id,
                        create_nano,
                        code,
                        Some(&msg),
                        1,
                        group_id,
                        0,
                        false,
                        0,
                        0,
                        0,
                        0,
                        0,
                    )
                }
            };
            let conn_handle = conn_handle_usize as *mut std::ffi::c_void;
            unsafe {
                let _ = server.submit_response(conn_handle, &ctrl, None, msg_type, req_id);
            }
        });
    }

    // ── StepDown ─────────────────────────────────────────────────

    fn handle_step_down(&self, req: &ServerRequest<'_>, server: &Arc<RpcServer>) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let msg_type = FBMsgType::EStepDownResponse.0 as u16;

        let Ok(fb_req) = flatbuffers::root::<FBStepDownRequest>(req.control) else {
            submit_error(
                server,
                req.conn_handle,
                req_id,
                create_nano,
                msg_type,
                FBKvRetCode::InvalidArgument,
                "invalid request flatbuffer",
            );
            return;
        };
        let group_id = fb_req.group_id();
        let Some(group) = self.store.get_group(group_id) else {
            submit_error(
                server,
                req.conn_handle,
                req_id,
                create_nano,
                msg_type,
                FBKvRetCode::NotFound,
                "px group not found",
            );
            return;
        };
        let reason = fb_req.reason().unwrap_or("").to_string();
        let payload = StepDownRequestPayload {
            term: fb_req.term(),
            target_leader_id: fb_req.target_leader_id(),
            reason,
        };
        let conn_handle_usize = req.conn_handle as usize;
        let server = Arc::clone(server);
        self.rt.spawn(async move {
            let replica = group.local_replica();
            let reply = <PxLocalReplica as ReplicaHandler>::on_step_down(replica, &payload, group_id).await;
            let ctrl = match reply {
                Ok(r) => build_step_down_response(
                    req_id,
                    create_nano,
                    FBKvRetCode::Success,
                    None,
                    1,
                    group_id,
                    r.accepted,
                    r.current_term,
                    r.current_leader_id,
                ),
                Err(e) => {
                    let (code, msg) = px_error_to_ret_code(&e);
                    build_step_down_response(req_id, create_nano, code, Some(&msg), 1, group_id, false, 0, 0)
                }
            };
            let conn_handle = conn_handle_usize as *mut std::ffi::c_void;
            unsafe {
                let _ = server.submit_response(conn_handle, &ctrl, None, msg_type, req_id);
            }
        });
    }

    // ── ChosenNotification (fire-and-forget) ─────────────────────

    fn handle_chosen_notice(&self, req: &ServerRequest<'_>, _server: &Arc<RpcServer>) {
        let Ok(fb_req) = flatbuffers::root::<FBChosenNotification>(req.control) else {
            debug!(
                store_id = self.store.store_id,
                "chosen notice: invalid flatbuffer"
            );
            return;
        };
        let group_id = fb_req.group_id();
        let slot = fb_req.slot();
        let term = fb_req.term();
        let leader_id = fb_req.leader_id();
        let ballot_round = fb_req.ballot_round();

        let Some(group) = self.store.get_group(group_id) else {
            debug!(
                store_id = self.store.store_id,
                group_id, slot, term, "chosen notice dropped (group not found)"
            );
            return;
        };
        let chosen_ballot = PxBallot {
            round: ballot_round,
            leader_id,
        };
        self.rt.spawn(async move {
            let replica = group.local_replica();
            let accepted = replica.accepted_at(slot).await;
            let ballot_matches = accepted.as_ref().is_some_and(|e| e.ballot == chosen_ballot);
            if ballot_matches {
                replica.learner.update_chosen_frontier(slot, term);
                replica.wake_apply_loop();
                debug!(
                    slot,
                    term, leader_id, ballot_round, "chosen notification applied (ballot match)"
                );
            } else {
                replica.note_chosen(slot, term);
                replica.record_gap(slot);
                if let Some(ref entry) = accepted {
                    replica.incr_chosen_notice_stale();
                    debug!(
                        slot,
                        term,
                        leader_id,
                        chosen_ballot_round = ballot_round,
                        accepted_ballot_round = entry.ballot.round,
                        accepted_ballot_leader = entry.ballot.leader_id,
                        "chosen notification stale ballot (gap recorded)"
                    );
                } else {
                    replica.incr_chosen_notice_missing();
                    debug!(
                        slot,
                        term, leader_id, "chosen notification missing value (gap recorded)"
                    );
                }
            }
        });
    }

    // ── BatchChosenNotification (fire-and-forget) ────────────────

    fn handle_batch_chosen(&self, req: &ServerRequest<'_>, _server: &Arc<RpcServer>) {
        let Ok(fb_req) = flatbuffers::root::<FBBatchChosenNotification>(req.control) else {
            debug!(store_id = self.store.store_id, "batch chosen: invalid flatbuffer");
            return;
        };
        let group_id = fb_req.group_id();
        let start_slot = fb_req.start_slot();
        let end_slot = fb_req.end_slot();
        let term = fb_req.term();
        let leader_id = fb_req.leader_id();
        let ballot_round = fb_req.ballot_round();

        let Some(group) = self.store.get_group(group_id) else {
            debug!(
                store_id = self.store.store_id,
                group_id, "batch chosen dropped (group not found)"
            );
            return;
        };
        let chosen_ballot = PxBallot {
            round: ballot_round,
            leader_id,
        };
        self.rt.spawn(async move {
            let replica = group.local_replica();
            let mut advanced_count = 0u64;
            for slot in start_slot..=end_slot {
                let accepted = replica.accepted_at(slot).await;
                if accepted.as_ref().is_some_and(|e| e.ballot == chosen_ballot) {
                    replica.learner.update_chosen_frontier(slot, term);
                    advanced_count += 1;
                } else {
                    replica.note_chosen(slot, term);
                    replica.record_gap(slot);
                }
            }
            replica.wake_apply_loop();
            debug!(
                group_id,
                start_slot,
                end_slot,
                term,
                leader_id,
                ballot_round,
                advanced_count,
                "batch chosen notification applied"
            );
        });
    }

    // ── FetchGap ─────────────────────────────────────────────────

    fn handle_fetch_gap(&self, req: &ServerRequest<'_>, server: &Arc<RpcServer>) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let msg_type = FBMsgType::EFetchGapResponse.0 as u16;

        let Ok(fb_req) = flatbuffers::root::<FBFetchGapRequest>(req.control) else {
            submit_error(
                server,
                req.conn_handle,
                req_id,
                create_nano,
                msg_type,
                FBKvRetCode::InvalidArgument,
                "invalid request flatbuffer",
            );
            return;
        };
        let group_id = fb_req.group_id();
        let slot = fb_req.slot();

        let Some(group) = self.store.get_group(group_id) else {
            submit_error(
                server,
                req.conn_handle,
                req_id,
                create_nano,
                msg_type,
                FBKvRetCode::NotFound,
                "px group not found",
            );
            return;
        };
        if let Some(resp) = group.handle_fetch_gap(slot) {
            let payload_vec = resp.payload.clone();
            let ctrl = build_fetch_gap_response(
                req_id,
                create_nano,
                FBKvRetCode::Success,
                None,
                1,
                group_id,
                resp.slot,
                resp.term,
                resp.ballot_round,
                resp.leader_id,
                &payload_vec,
            );
            unsafe {
                let _ = server.submit_response(req.conn_handle, &ctrl, None, msg_type, req_id);
            }
        } else {
            debug!(
                store_id = self.store.store_id,
                group_id, slot, "fetch_gap no value (not yet chosen)"
            );
            submit_error(
                server,
                req.conn_handle,
                req_id,
                create_nano,
                msg_type,
                FBKvRetCode::NotFound,
                "slot not yet chosen",
            );
        }
    }

    // ── Snapshot ─────────────────────────────────────────────────

    fn handle_snapshot(&self, req: &ServerRequest<'_>, server: &Arc<RpcServer>) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let msg_type = FBMsgType::ESnapshotResponse.0 as u16;

        let Ok(fb_req) = flatbuffers::root::<FBSnapshotRequest>(req.control) else {
            submit_error(
                server,
                req.conn_handle,
                req_id,
                create_nano,
                msg_type,
                FBKvRetCode::InvalidArgument,
                "invalid snapshot request flatbuffer",
            );
            return;
        };
        let group_id = fb_req.group_id();

        let Some(group) = self.store.get_group(group_id) else {
            submit_error(
                server,
                req.conn_handle,
                req_id,
                create_nano,
                msg_type,
                FBKvRetCode::NotFound,
                "px group not found",
            );
            return;
        };

        let replica = group.local_replica();
        let export_result = replica.learner.engine().snapshot_export();
        let (at_slot, bytes) = match export_result {
            Ok(v) => v,
            Err(e) => {
                submit_error(
                    server,
                    req.conn_handle,
                    req_id,
                    create_nano,
                    msg_type,
                    FBKvRetCode::Internal,
                    &format!("snapshot export failed: {e}"),
                );
                return;
            }
        };

        let membership_epoch = group.membership_epoch();
        let conn_handle_usize = req.conn_handle as usize;
        let store_id = self.store.store_id;
        let server = Arc::clone(server);
        self.rt.spawn(async move {
            let term_at_slot = group
                .local_replica()
                .accepted_at(at_slot)
                .await
                .map_or(0, |entry| entry.term);

            debug!(
                store_id,
                group_id,
                at_slot,
                term_at_slot,
                membership_epoch,
                snapshot_bytes = bytes.len(),
                "serving snapshot export"
            );

            let mut builder = FlatBufferBuilder::new();
            let args = FBSnapshotResponseArgs {
                id: req_id,
                rpc_create_nano: create_nano,
                ret_code: FBKvRetCode::Success,
                error_msg: None,
                group_id,
                term_at_slot,
                membership_epoch,
                at_slot,
            };
            let fb_resp = FBSnapshotResponse::create(&mut builder, &args);
            builder.finish(fb_resp, None);
            let ctrl = Buffer::from_bytes(builder.finished_data());
            let data = bytes::Bytes::from(bytes);
            let conn_handle = conn_handle_usize as *mut std::ffi::c_void;
            unsafe {
                let _ = server.submit_response(conn_handle, ctrl.bytes(), Some(&data), msg_type, req_id);
            }
        });
    }
}

// ── Helper functions ─────────────────────────────────────────────

fn px_error_to_ret_code(e: &PxReplicaError) -> (FBKvRetCode, String) {
    match e {
        PxReplicaError::GroupNotFound(_) => (FBKvRetCode::NotFound, e.to_string()),
        PxReplicaError::ShuttingDown => (FBKvRetCode::Unavailable, e.to_string()),
        PxReplicaError::Internal(_) => (FBKvRetCode::Internal, e.to_string()),
    }
}

fn submit_error(
    server: &Arc<RpcServer>,
    conn_handle: *mut std::ffi::c_void,
    req_id: u64,
    create_nano: u64,
    msg_type: u16,
    code: FBKvRetCode,
    msg: &str,
) {
    let mut builder = FlatBufferBuilder::new();
    let error_msg = builder.create_string(msg);
    let args = FBPromiseResponseArgs {
        id: req_id,
        rpc_create_nano: create_nano,
        ret_code: code,
        error_msg: Some(error_msg),
        version: 0,
        slot: 0,
        round: 0,
        leader_id: 0,
        previously_accepted: None,
        rejected: false,
        rejected_round: 0,
        rejected_leader_id: 0,
        term: 0,
        term_stale: false,
        membership_epoch: 0,
        epoch_mismatch: false,
    };
    let resp = FBPromiseResponse::create(&mut builder, &args);
    builder.finish(resp, None);
    let buf = builder.finished_data();
    unsafe {
        let _ = server.submit_response(conn_handle, buf, None, msg_type, req_id);
    }
}

// ── Response builders ────────────────────────────────────────────

/// Build a `FBPromiseResponse` with a pre-existing `FBAcceptedValue`
/// offset (for the `Promised` reply with `previously_accepted`).
/// The caller must have already created the offset in `builder`.
#[allow(clippy::too_many_arguments)]
fn finish_promise_response(
    builder: &mut FlatBufferBuilder,
    req_id: u64,
    create_nano: u64,
    ret_code: FBKvRetCode,
    error_msg: Option<&str>,
    version: u32,
    slot: u64,
    round: u64,
    leader_id: u64,
    previously_accepted: Option<flatbuffers::WIPOffset<FBAcceptedValue>>,
    rejected: bool,
    rejected_round: u64,
    rejected_leader_id: u64,
    term: u64,
    term_stale: bool,
    membership_epoch: u64,
    epoch_mismatch: bool,
) -> Vec<u8> {
    let error_msg = error_msg.map(|m| builder.create_string(m));
    let args = FBPromiseResponseArgs {
        id: req_id,
        rpc_create_nano: create_nano,
        ret_code,
        error_msg,
        version,
        slot,
        round,
        leader_id,
        previously_accepted,
        rejected,
        rejected_round,
        rejected_leader_id,
        term,
        term_stale,
        membership_epoch,
        epoch_mismatch,
    };
    let resp = FBPromiseResponse::create(builder, &args);
    builder.finish(resp, None);
    builder.finished_data().to_vec()
}

#[allow(clippy::too_many_arguments)]
fn build_promise_response(
    req_id: u64,
    create_nano: u64,
    ret_code: FBKvRetCode,
    error_msg: Option<&str>,
    version: u32,
    slot: u64,
    round: u64,
    leader_id: u64,
    previously_accepted: Option<flatbuffers::WIPOffset<FBAcceptedValue>>,
    rejected: bool,
    rejected_round: u64,
    rejected_leader_id: u64,
    term: u64,
    term_stale: bool,
    membership_epoch: u64,
    epoch_mismatch: bool,
) -> Vec<u8> {
    let mut builder = FlatBufferBuilder::new();
    finish_promise_response(
        &mut builder,
        req_id,
        create_nano,
        ret_code,
        error_msg,
        version,
        slot,
        round,
        leader_id,
        previously_accepted,
        rejected,
        rejected_round,
        rejected_leader_id,
        term,
        term_stale,
        membership_epoch,
        epoch_mismatch,
    )
}

#[allow(clippy::too_many_arguments)]
fn build_accepted_response(
    req_id: u64,
    create_nano: u64,
    ret_code: FBKvRetCode,
    error_msg: Option<&str>,
    version: u32,
    slot: u64,
    round: u64,
    leader_id: u64,
    rejected: bool,
    rejected_round: u64,
    rejected_leader_id: u64,
    term: u64,
    term_stale: bool,
    membership_epoch: u64,
    epoch_mismatch: bool,
) -> Vec<u8> {
    let mut builder = FlatBufferBuilder::new();
    let error_msg = error_msg.map(|m| builder.create_string(m));
    let args = FBAcceptedResponseArgs {
        id: req_id,
        rpc_create_nano: create_nano,
        ret_code,
        error_msg,
        version,
        slot,
        round,
        leader_id,
        rejected,
        rejected_round,
        rejected_leader_id,
        term,
        term_stale,
        membership_epoch,
        epoch_mismatch,
    };
    let resp = FBAcceptedResponse::create(&mut builder, &args);
    builder.finish(resp, None);
    builder.finished_data().to_vec()
}

#[allow(clippy::too_many_arguments)]
fn build_pre_vote_response(
    req_id: u64,
    create_nano: u64,
    ret_code: FBKvRetCode,
    error_msg: Option<&str>,
    version: u32,
    group_id: u64,
    term: u64,
    granted: bool,
    contiguous_chosen: u64,
    last_chosen_term: u64,
    highest_seen_slot: u64,
) -> Vec<u8> {
    let mut builder = FlatBufferBuilder::new();
    let error_msg = error_msg.map(|m| builder.create_string(m));
    let args = FBPreVoteResponseArgs {
        id: req_id,
        rpc_create_nano: create_nano,
        ret_code,
        error_msg,
        version,
        group_id,
        term,
        granted,
        contiguous_chosen,
        last_chosen_term,
        highest_seen_slot,
    };
    let resp = FBPreVoteResponse::create(&mut builder, &args);
    builder.finish(resp, None);
    builder.finished_data().to_vec()
}

#[allow(clippy::too_many_arguments)]
fn build_request_vote_response(
    req_id: u64,
    create_nano: u64,
    ret_code: FBKvRetCode,
    error_msg: Option<&str>,
    version: u32,
    group_id: u64,
    term: u64,
    granted: bool,
    contiguous_chosen: u64,
    last_chosen_term: u64,
    highest_seen_slot: u64,
) -> Vec<u8> {
    let mut builder = FlatBufferBuilder::new();
    let error_msg = error_msg.map(|m| builder.create_string(m));
    let args = FBRequestVoteResponseArgs {
        id: req_id,
        rpc_create_nano: create_nano,
        ret_code,
        error_msg,
        version,
        group_id,
        term,
        granted,
        contiguous_chosen,
        last_chosen_term,
        highest_seen_slot,
    };
    let resp = FBRequestVoteResponse::create(&mut builder, &args);
    builder.finish(resp, None);
    builder.finished_data().to_vec()
}

#[allow(clippy::too_many_arguments)]
fn build_heartbeat_response(
    req_id: u64,
    create_nano: u64,
    ret_code: FBKvRetCode,
    error_msg: Option<&str>,
    version: u32,
    group_id: u64,
    term: u64,
    success: bool,
    contiguous_chosen: u64,
    last_chosen_term: u64,
    contiguous_applied: u64,
    highest_seen_slot: u64,
    durable_snapshot_slot: u64,
) -> Vec<u8> {
    let mut builder = FlatBufferBuilder::new();
    let error_msg = error_msg.map(|m| builder.create_string(m));
    let args = FBHeartbeatResponseArgs {
        id: req_id,
        rpc_create_nano: create_nano,
        ret_code,
        error_msg,
        version,
        group_id,
        term,
        success,
        contiguous_chosen,
        last_chosen_term,
        contiguous_applied,
        highest_seen_slot,
        durable_snapshot_slot,
    };
    let resp = FBHeartbeatResponse::create(&mut builder, &args);
    builder.finish(resp, None);
    builder.finished_data().to_vec()
}

#[allow(clippy::too_many_arguments)]
fn build_step_down_response(
    req_id: u64,
    create_nano: u64,
    ret_code: FBKvRetCode,
    error_msg: Option<&str>,
    version: u32,
    group_id: u64,
    accepted: bool,
    current_term: u64,
    current_leader_id: u64,
) -> Vec<u8> {
    let mut builder = FlatBufferBuilder::new();
    let error_msg = error_msg.map(|m| builder.create_string(m));
    let args = FBStepDownResponseArgs {
        id: req_id,
        rpc_create_nano: create_nano,
        ret_code,
        error_msg,
        version,
        group_id,
        accepted,
        current_term,
        current_leader_id,
    };
    let resp = FBStepDownResponse::create(&mut builder, &args);
    builder.finish(resp, None);
    builder.finished_data().to_vec()
}

#[allow(clippy::too_many_arguments)]
fn build_fetch_gap_response(
    req_id: u64,
    create_nano: u64,
    ret_code: FBKvRetCode,
    error_msg: Option<&str>,
    version: u32,
    group_id: u64,
    slot: u64,
    term: u64,
    ballot_round: u64,
    leader_id: u64,
    payload: &[u8],
) -> Vec<u8> {
    let mut builder = FlatBufferBuilder::new();
    let error_msg = error_msg.map(|m| builder.create_string(m));
    let payload_vec = builder.create_vector(payload);
    let args = FBFetchGapResponseArgs {
        id: req_id,
        rpc_create_nano: create_nano,
        ret_code,
        error_msg,
        version,
        group_id,
        slot,
        term,
        ballot_round,
        leader_id,
        payload: Some(payload_vec),
    };
    let resp = FBFetchGapResponse::create(&mut builder, &args);
    builder.finish(resp, None);
    builder.finished_data().to_vec()
}
