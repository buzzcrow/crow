//! Common replica interface and shared definitions.
//!
//! This module defines traits for replicas:
//! - `Replica`: Metadata interface (id, endpoint, voting)
//! - `ReplicaHandler`: Server-side handlers (local replicas)
//! - `ReplicaClient`: Client-side senders (remote replicas)
//!
//! Both handler/client traits are **transport-neutral**: errors flow through
//! [`PxReplicaError`], an in-process enum. The gRPC adapter
//! (`rpc::px_service`) maps both directions across the network boundary.
//!
//! Key work: error taxonomy ([`PxReplicaError`]), election handler methods
//! (`on_pre_vote` / `on_request_vote` / `on_heartbeat` / `on_step_down`),
//! matching client senders.

use crate::paxos::roles::SlotIndex;
use crate::paxos::roles::{PxAcceptReply, PxBallot, PxLogEntry, PxPrepareReply};
use crate::paxos::{PxGroupId, PxNodeId, PxTerm};

/// Transport-neutral replica error.
///
/// All [`ReplicaHandler`] and [`ReplicaClient`] methods return this. The gRPC
/// adapter (`crate::rpc::px_service`) maps to/from `tonic::Status` at the
/// network boundary so `crowkv` library code never names `tonic::Status`
/// outside of `rpc/`.
#[derive(Debug, thiserror::Error)]
pub enum PxReplicaError {
    #[error("group {0} not found on this replica")]
    GroupNotFound(PxGroupId),
    #[error("replica is shutting down")]
    ShuttingDown,
    #[error("internal invariant violation: {0}")]
    Internal(String),
}

/// Information returned by a `PreVote` / `RequestVote` reply.
///
/// Carries the responder's term plus the learner-frontier triple used by the
/// candidate's bulk-Phase-1 floor / ceiling computation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct VoteReply {
    pub term: PxTerm,
    pub granted: bool,
    pub contiguous_chosen: SlotIndex,
    pub last_chosen_term: PxTerm,
    pub highest_seen_slot: SlotIndex,
}

/// `Heartbeat` request payload from the leader.
#[derive(Clone, Copy, Debug)]
pub struct HeartbeatRequestPayload {
    pub term: PxTerm,
    pub leader_id: PxNodeId,
    pub prev_log_slot: SlotIndex,
    pub prev_log_term: PxTerm,
    pub committed_safe_slot: SlotIndex,
    pub lease_grant_until_ms_mono: u64,
    pub t_send_ms_mono: u64,
}

/// `Heartbeat` reply from a follower.
#[derive(Clone, Copy, Debug)]
pub struct HeartbeatReply {
    pub term: PxTerm,
    pub success: bool,
    pub contiguous_chosen: SlotIndex,
    pub last_chosen_term: PxTerm,
    pub contiguous_applied: SlotIndex,
    pub highest_seen_slot: SlotIndex,
    /// Highest slot durably captured in this follower's own last engine
    /// snapshot (`snapshot_slot`).
    /// The leader aggregates this across voting peers to track the group's
    /// real "durable on leader + >=1 peer" watermark.
    pub durable_snapshot_slot: SlotIndex,
}

/// Inputs for the `RequestVote` / `PreVote` decision.
#[derive(Clone, Copy, Debug)]
pub struct VoteRequestPayload {
    pub term: PxTerm,
    pub candidate_id: PxNodeId,
    pub accepted_log_tip_slot: SlotIndex,
    pub accepted_log_tip_term: PxTerm,
}

/// Admin step-down request payload (strict-fence policy: target replica must
/// still be leader at the requested term to accept).
#[derive(Clone, Debug)]
pub struct StepDownRequestPayload {
    pub term: PxTerm,
    pub target_leader_id: PxNodeId,
    pub reason: String,
}

/// Admin step-down reply.
#[derive(Clone, Copy, Debug)]
pub struct StepDownReply {
    pub accepted: bool,
    pub current_term: PxTerm,
    pub current_leader_id: PxNodeId,
}

/// Common trait for replica metadata.
///
/// This trait defines the minimal metadata interface that all replicas must implement.
pub trait Replica {
    /// Get the node ID of this replica.
    fn id(&self) -> u64;

    /// Get the endpoint of this replica (if available).
    ///
    /// Local replicas may not have an endpoint if the server hasn't started yet.
    fn endpoint(&self) -> Option<&str>;

    /// Whether this replica is a voting member (has a vote in quorum).
    fn voting(&self) -> bool;
}

/// Server-side handler trait for local replicas.
///
/// All errors are transport-neutral ([`PxReplicaError`]); the gRPC adapter
/// translates to `tonic::Status` only at the network boundary.
#[allow(async_fn_in_trait)]
pub trait ReplicaHandler: Replica {
    /// Phase-1 `Prepare` handler.
    ///
    /// `term` is the proposer's election term. If it lags the responder's
    /// `current_term`, the handler replies [`PxPrepareReply::TermStale`].
    /// If it leads, the handler adopts the new term via
    /// [`crate::cluster::local_replica::PxLocalReplica::become_follower`]
    /// before forwarding to the acceptor.
    async fn on_prepare(
        &self,
        slot: u64,
        ballot: PxBallot,
        term: PxTerm,
        group_id: u64,
    ) -> Result<PxPrepareReply, PxReplicaError>;

    /// Phase-2 `Accept` handler.
    async fn on_accept(&self, entry: PxLogEntry, group_id: u64) -> Result<PxAcceptReply, PxReplicaError>;

    /// `PreVote` handler. No state mutation; returns the vote decision plus the
    /// learner-frontier triple for the candidate's bulk-Phase-1 calculation.
    async fn on_pre_vote(&self, req: VoteRequestPayload, group_id: u64) -> Result<VoteReply, PxReplicaError>;

    /// `RequestVote` handler. State-mutating: on grant, persists `voted_for`
    /// and bumps `current_term` if higher.
    async fn on_request_vote(
        &self,
        req: VoteRequestPayload,
        group_id: u64,
    ) -> Result<VoteReply, PxReplicaError>;

    /// `Heartbeat` handler. Bumps `vote_lockout_until`, adopts a higher term
    /// observed in the request, records the current leader id, and resets
    /// the follower's election deadline (driver-managed).
    async fn on_heartbeat(
        &self,
        req: HeartbeatRequestPayload,
        group_id: u64,
    ) -> Result<HeartbeatReply, PxReplicaError>;

    /// `StepDown` handler. Strict fence per §7.1.
    async fn on_step_down(
        &self,
        req: &StepDownRequestPayload,
        group_id: u64,
    ) -> Result<StepDownReply, PxReplicaError>;
}

/// Client-side sender trait for remote replicas.
///
/// All errors are transport-neutral ([`PxReplicaError`]); transport-level gRPC
/// failures fold into [`PxReplicaError::Internal`] inside the gRPC client
/// adapter.
#[allow(async_fn_in_trait)]
pub trait ReplicaClient: Replica {
    async fn send_prepare(
        &self,
        slot: u64,
        ballot: PxBallot,
        term: PxTerm,
        group_id: u64,
        membership_epoch: u64,
    ) -> Result<PxPrepareReply, PxReplicaError>;
    async fn send_accept(
        &self,
        entry: &PxLogEntry,
        client_id: Option<u64>,
        seq: Option<u64>,
        group_id: u64,
        membership_epoch: u64,
    ) -> Result<PxAcceptReply, PxReplicaError>;
    async fn send_pre_vote(
        &self,
        req: VoteRequestPayload,
        group_id: u64,
    ) -> Result<VoteReply, PxReplicaError>;
    async fn send_request_vote(
        &self,
        req: VoteRequestPayload,
        group_id: u64,
    ) -> Result<VoteReply, PxReplicaError>;
    async fn send_heartbeat(
        &self,
        req: HeartbeatRequestPayload,
        group_id: u64,
    ) -> Result<HeartbeatReply, PxReplicaError>;
    async fn send_step_down(
        &self,
        req: &StepDownRequestPayload,
        group_id: u64,
    ) -> Result<StepDownReply, PxReplicaError>;
}
