//! Common replica interface and shared definitions.
//!
//! This module defines traits for replicas:
//! - `Replica`: Metadata interface (id, endpoint, voting)
//! - `ReplicaHandler`: Server-side handlers (local replicas)
//! - `ReplicaClient`: Client-side senders (remote replicas)

use crate::paxos::roles::{PxAcceptReply, PxBallot, PxLogEntry, PxPrepareReply};

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
/// This trait defines the handlers that process incoming Paxos requests.
#[allow(async_fn_in_trait)]
pub trait ReplicaHandler: Replica {
    /// Phase-1 `Prepare` handler.
    async fn on_prepare(
        &self,
        slot: u64,
        ballot: PxBallot,
        group_id: u64,
    ) -> Result<PxPrepareReply, tonic::Status>;

    /// Phase-2 `Accept` handler.
    async fn on_accept(
        &self,
        entry: PxLogEntry,
        group_id: u64,
    ) -> Result<PxAcceptReply, tonic::Status>;
}

/// Client-side sender trait for remote replicas.
///
/// This trait defines methods for sending Paxos requests to remote nodes.
#[allow(async_fn_in_trait)]
pub trait ReplicaClient: Replica {
    /// Send a Prepare request to a remote replica.
    async fn send_prepare(
        &self,
        slot: u64,
        ballot: PxBallot,
        group_id: u64,
    ) -> Result<PxPrepareReply, tonic::Status>;

    /// Send an Accept request to a remote replica.
    async fn send_accept(
        &self,
        entry: &PxLogEntry,
        group_id: u64,
    ) -> Result<PxAcceptReply, tonic::Status>;
}
