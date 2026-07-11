use crate::cluster::replica::{Replica, ReplicaClient};
use crate::paxos::roles::{PxAcceptReply, PxBallot, PxLogEntry, PxLogEntryKind, PxPrepareReply};
use crate::paxos::PxNodeId;
use crate::rpc::px_service_client::PxServiceClient;
use crate::rpc::{AcceptRequest, AcceptedValue, PrepareRequest};
use tokio::sync::OnceCell;
use tonic::transport::Channel;

#[derive(Debug)]
pub struct PxRemoteReplica {
    pub(crate) node_id: PxNodeId,
    pub(crate) endpoint: String,
    grpc_client: OnceCell<PxServiceClient<Channel>>,
    pub(crate) voting: bool,
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
        group_id: u64,
    ) -> Result<PxPrepareReply, tonic::Status> {
        let mut client = self.get_client().await?.clone();
        let resp = client
            .prepare(PrepareRequest {
                version: 1,
                slot,
                round: ballot.round,
                leader_id: ballot.leader_id,
                request_id: 0,
                request_create_ms: 0,
                group_id,
            })
            .await?
            .into_inner();

        if resp.rejected {
            Ok(PxPrepareReply::Rejected {
                slot: resp.slot,
                current_promised: PxBallot::new(resp.rejected_round, resp.rejected_leader_id),
            })
        } else {
            Ok(PxPrepareReply::Promised {
                slot: resp.slot,
                accepted: resp
                    .previously_accepted
                    .map(Self::accepted_value_to_log_entry),
            })
        }
    }

    async fn send_accept(
        &self,
        entry: &PxLogEntry,
        group_id: u64,
    ) -> Result<PxAcceptReply, tonic::Status> {
        let mut client = self.get_client().await?.clone();
        let resp = client
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
                    payload: entry.payload.clone(),
                }),
                request_id: 0,
                request_create_ms: 0,
                client_id: entry.client_id.unwrap_or(0),
                seq: entry.seq.unwrap_or(0),
                group_id,
            })
            .await?
            .into_inner();

        if resp.rejected {
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
}

impl PxRemoteReplica {
    async fn get_client(&self) -> Result<&PxServiceClient<Channel>, tonic::Status> {
        self.grpc_client
            .get_or_try_init(|| async {
                PxServiceClient::connect(format!("http://{}", self.endpoint))
                    .await
                    .map_err(|e| tonic::Status::unavailable(e.to_string()))
            })
            .await
    }

    pub fn new(node_id: PxNodeId, endpoint: String) -> Self {
        Self {
            node_id,
            endpoint,
            grpc_client: OnceCell::new(),
            voting: true,
        }
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
