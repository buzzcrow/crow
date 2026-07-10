//! Tonic `PeerService` implementation that delegates to `PxAcceptor`.
//!
//! P1 M2: only `Prepare` and `Accept` unary methods.

use tonic::{Request, Response, Status};

use crate::paxos::acceptor::PxAcceptor;
use crate::paxos::protocol::{PxAcceptReply, PxPrepareReply};
use crate::paxos::slot_list::SlotIndex;
use crate::paxos::slot_node::{PxBallot, PxLogEntry};

use crate::rpc::peer_service_server::PeerService;
use crate::rpc::{AcceptRequest, AcceptedResponse, AcceptedValue, PrepareRequest, PromiseResponse};

/// gRPC service wrapper around an in-memory `PxAcceptor`.
#[derive(Clone)]
pub struct AcceptorService {
    inner: std::sync::Arc<tokio::sync::Mutex<PxAcceptor>>,
}

impl AcceptorService {
    pub fn new(acceptor: PxAcceptor) -> Self {
        Self {
            inner: std::sync::Arc::new(tokio::sync::Mutex::new(acceptor)),
        }
    }
}

#[tonic::async_trait]
impl PeerService for AcceptorService {
    async fn prepare(
        &self,
        request: Request<PrepareRequest>,
    ) -> Result<Response<PromiseResponse>, Status> {
        let req = request.into_inner();
        let ballot = PxBallot {
            round: req.round,
            leader_id: req.leader_id,
        };
        let mut guard = self.inner.lock().await;
        let reply = guard.prepare(req.slot, ballot).await;
        drop(guard);

        let response = match reply {
            PxPrepareReply::Promised { slot, accepted } => PromiseResponse {
                version: 1,
                slot,
                round: req.round,
                leader_id: req.leader_id,
                previously_accepted: accepted.as_ref().map(px_log_entry_to_wire),
                rejected: false,
                rejected_round: 0,
                rejected_leader_id: 0,
            },
            PxPrepareReply::Rejected {
                slot,
                current_promised,
            } => PromiseResponse {
                version: 1,
                slot,
                round: req.round,
                leader_id: req.leader_id,
                previously_accepted: None,
                rejected: true,
                rejected_round: current_promised.round,
                rejected_leader_id: current_promised.leader_id,
            },
        };

        Ok(Response::new(response))
    }

    async fn accept(
        &self,
        request: Request<AcceptRequest>,
    ) -> Result<Response<AcceptedResponse>, Status> {
        let req = request.into_inner();
        let value = req
            .value
            .ok_or_else(|| Status::invalid_argument("missing value"))?;
        let entry = PxLogEntry {
            slot: req.slot,
            ballot: PxBallot {
                round: req.round,
                leader_id: req.leader_id,
            },
            term: req.term,
            kind: crate::paxos::slot_node::LogEntryKind::Write,
            payload: value.payload,
            client_id: None,
            seq: None,
        };

        let mut guard = self.inner.lock().await;
        let reply = guard.accept(entry).await;
        drop(guard);

        let (rejected, rejected_round, rejected_leader_id) = match reply {
            PxAcceptReply::Accepted { .. } => (false, 0, 0),
            PxAcceptReply::Rejected {
                current_promised, ..
            } => (true, current_promised.round, current_promised.leader_id),
        };

        let response = AcceptedResponse {
            version: 1,
            slot: req.slot,
            round: req.round,
            leader_id: req.leader_id,
            rejected,
            rejected_round,
            rejected_leader_id,
        };

        Ok(Response::new(response))
    }
}

fn px_log_entry_to_wire(entry: &PxLogEntry) -> AcceptedValue {
    AcceptedValue {
        slot: entry.slot,
        round: entry.ballot.round,
        leader_id: entry.ballot.leader_id,
        term: entry.term,
        payload: entry.payload.clone(),
    }
}
