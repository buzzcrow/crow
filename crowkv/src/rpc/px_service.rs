//! Tonic `PxService` implementation that delegates to `PxNode`.
//!
//! This module contains the wire-format handler (`PxNodeService`) that
//! converts between protobuf messages and the in-memory Paxos types,
//! then forwards to the node so that all real logic lives in one place.

use tonic::{Request, Response, Status};
use tracing::{debug, warn};

use crate::node::PxNode;
use crate::paxos::roles::{AcceptReply, Ballot, Learner, LogEntry, LogEntryKind, PrepareReply};

use crate::rpc::px_service_server::PxService;
use crate::rpc::{AcceptRequest, AcceptedResponse, AcceptedValue, PrepareRequest, PromiseResponse};

/// gRPC service wrapper that delegates `Prepare`/`Accept` to `PxNode`.
#[derive(Clone)]
pub struct PxNodeService {
    node: PxNode,
}

impl PxNodeService {
    pub fn new(node: PxNode) -> Self {
        Self { node }
    }
}

#[tonic::async_trait]
impl PxService for PxNodeService {
    async fn prepare(
        &self,
        request: Request<PrepareRequest>,
    ) -> Result<Response<PromiseResponse>, Status> {
        let req = request.into_inner();
        debug!(
            request_id = req.request_id,
            slot = req.slot,
            round = req.round,
            leader_id = req.leader_id,
            "received paxos prepare rpc"
        );
        let ballot = Ballot {
            round: req.round,
            leader_id: req.leader_id,
        };
        let reply = self.node.on_prepare(req.slot, ballot).await;

        let response = match reply {
            PrepareReply::Promised { slot, accepted } => PromiseResponse {
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
            PrepareReply::Rejected {
                slot,
                current_promised,
            } => {
                warn!(
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

    async fn accept(
        &self,
        request: Request<AcceptRequest>,
    ) -> Result<Response<AcceptedResponse>, Status> {
        let req = request.into_inner();
        debug!(
            request_id = req.request_id,
            slot = req.slot,
            round = req.round,
            leader_id = req.leader_id,
            "received paxos accept rpc"
        );
        let value = req.value.ok_or_else(|| {
            warn!(
                request_id = req.request_id,
                slot = req.slot,
                "accept rpc missing value; next step: check caller/protobuf conversion"
            );
            Status::invalid_argument("missing value")
        })?;
        let entry = LogEntry {
            slot: req.slot,
            ballot: Ballot {
                round: req.round,
                leader_id: req.leader_id,
            },
            term: req.term,
            kind: LogEntryKind::Write,
            payload: value.payload,
            client_id: optional_u64(req.client_id),
            seq: optional_u64(req.seq),
        };

        let reply = self.node.on_accept(entry.clone()).await;
        if matches!(reply, AcceptReply::Accepted { .. }) {
            self.node.learner.learn(entry);
        }

        let (rejected, rejected_round, rejected_leader_id) = match reply {
            AcceptReply::Accepted { .. } => (false, 0, 0),
            AcceptReply::Rejected {
                current_promised, ..
            } => {
                warn!(
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

fn optional_u64(value: u64) -> Option<u64> {
    if value == 0 {
        None
    } else {
        Some(value)
    }
}

fn log_entry_to_proto(entry: &LogEntry) -> AcceptedValue {
    AcceptedValue {
        slot: entry.slot,
        round: entry.ballot.round,
        leader_id: entry.ballot.leader_id,
        term: entry.term,
        payload: entry.payload.clone(),
    }
}
