//! Tonic `PxService` implementation that delegates to `PxLocalReplica`.
//!
//! This module contains the wire-format handler (`PxReplicaService`) that
//! converts between protobuf messages and the in-memory Paxos types,
//! then forwards to the node so that all real logic lives in one place.

use std::sync::Arc;
use tonic::{Request, Response, Status};
use tracing::{debug, warn};

use crate::cluster::px_kv_store::PxKvStore;
use crate::common::optional_u64;
use crate::paxos::roles::{PxAcceptReply, PxBallot, PxLogEntry, PxLogEntryKind, PxPrepareReply};

use crate::rpc::px_service_server::PxService;
use crate::rpc::{AcceptRequest, AcceptedResponse, AcceptedValue, PrepareRequest, PromiseResponse};

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
        let group = self.store.get_group(req.group_id).ok_or_else(|| Status::not_found("px group not found"))?;
        let replica = group.local_replica();
        let reply = replica.on_prepare(req.slot, ballot).await;

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
            },
            PxPrepareReply::Rejected { slot, current_promised } => {
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
                }
            }
        };

        Ok(Response::new(response))
    }

    async fn accept(&self, request: Request<AcceptRequest>) -> Result<Response<AcceptedResponse>, Status> {
        let req = request.into_inner();
        debug!(
            store_id = self.store.store_id,
            group_id = req.group_id,
            request_id = req.request_id,
            slot = req.slot,
            round = req.round,
            leader_id = req.leader_id,
            "received paxos accept rpc"
        );
        let value = req.value.ok_or_else(|| {
            warn!(
                store_id = self.store.store_id,
                group_id = req.group_id,
                request_id = req.request_id,
                slot = req.slot,
                "accept rpc missing value; next step: check caller/protobuf conversion"
            );
            Status::invalid_argument("missing value")
        })?;
        let entry = PxLogEntry {
            slot: req.slot,
            ballot: PxBallot {
                round: req.round,
                leader_id: req.leader_id,
            },
            term: req.term,
            kind: PxLogEntryKind::Write,
            payload: Arc::new(value.payload),
            client_id: optional_u64(req.client_id),
            seq: optional_u64(req.seq),
        };

        let group = self.store.get_group(req.group_id).ok_or_else(|| Status::not_found("px group not found"))?;
        let replica = group.local_replica();
        let reply = replica.on_accept(entry.clone()).await;
        if matches!(reply, PxAcceptReply::Accepted { .. }) {
            replica.learn(&entry);
        }

        let (rejected, rejected_round, rejected_leader_id) = match reply {
            PxAcceptReply::Accepted { .. } => (false, 0, 0),
            PxAcceptReply::Rejected { current_promised, .. } => {
                warn!(
                    store_id = self.store.store_id,
                    group_id = req.group_id,
                    request_id = req.request_id,
                    slot = req.slot,
                    current_round = current_promised.round,
                    current_leader_id = current_promised.leader_id,
                    "accept rejected; next step: proposer should run prepare with a higher ballot"
                );
                (true, current_promised.round, current_promised.leader_id)
            }
        };

        let response = AcceptedResponse {
            version: 1,
            slot: req.slot,
            round: req.round,
            leader_id: req.leader_id,
            rejected,
            rejected_round,
            rejected_leader_id,
            request_id: req.request_id,
            request_create_ms: req.request_create_ms,
        };

        Ok(Response::new(response))
    }
}

fn log_entry_to_proto(entry: &PxLogEntry) -> AcceptedValue {
    AcceptedValue {
        slot: entry.slot,
        round: entry.ballot.round,
        leader_id: entry.ballot.leader_id,
        term: entry.term,
        payload: (*entry.payload).clone(),
    }
}
