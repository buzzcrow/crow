//! `PxLocalReplica` — local acceptor/learner for a single CrowKV group member.
//!
//! Wraps the consensus state (`PxAcceptor`) and learner (`PxLearner`).
//! All proposer/KV logic has been moved to `PxGroup` and `KvStore`.

use crate::cluster::replica::{Replica, ReplicaHandler};
use crate::paxos::acceptor::PxAcceptor;
use crate::paxos::learner::PxLearner;
use crate::paxos::roles::{Acceptor, Learner, PxAcceptReply, PxBallot, PxLogEntry, PxPrepareReply};

/// Hard-coded role for a replica in P1 M2 (no election yet).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PxLocalReplicaRole {
    /// Drives Prepare / Accept rounds, tracks quorum.
    Leader,
    /// Only serves Acceptor handlers.
    Follower,
}

/// Runtime container for one local group member.
///
/// Pure acceptor/learner. Proposer logic lives in `PxGroup`.
pub struct PxLocalReplica {
    pub id: u64,
    pub role: PxLocalReplicaRole,
    pub acceptor: PxAcceptor,
    pub learner: PxLearner,
    pub voting: bool,
}

impl std::fmt::Debug for PxLocalReplica {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PxLocalReplica")
            .field("id", &self.id)
            .field("role", &self.role)
            .field("voting", &self.voting)
            .finish_non_exhaustive()
    }
}

impl Replica for PxLocalReplica {
    fn id(&self) -> u64 {
        self.id
    }

    fn endpoint(&self) -> Option<&str> {
        None
    }

    fn voting(&self) -> bool {
        self.voting
    }
}

impl ReplicaHandler for PxLocalReplica {
    async fn on_prepare(
        &self,
        slot: u64,
        ballot: PxBallot,
        _group_id: u64,
    ) -> Result<PxPrepareReply, tonic::Status> {
        Ok(self.on_prepare(slot, ballot).await)
    }

    async fn on_accept(
        &self,
        entry: PxLogEntry,
        _group_id: u64,
    ) -> Result<PxAcceptReply, tonic::Status> {
        Ok(self.on_accept(entry).await)
    }
}

impl PxLocalReplica {
    pub fn new(id: u64, role: PxLocalReplicaRole) -> Self {
        Self {
            id,
            role,
            acceptor: PxAcceptor::new(),
            learner: PxLearner::new(),
            voting: true,
        }
    }

    #[must_use]
    pub fn with_voting(mut self, voting: bool) -> Self {
        self.voting = voting;
        self
    }

    /// Convenience: is this replica the leader?
    pub fn is_leader(&self) -> bool {
        self.role == PxLocalReplicaRole::Leader
    }

    pub fn set_role(&mut self, role: PxLocalReplicaRole) {
        self.role = role;
    }

    /// Phase-1 `Prepare` handler — delegates to the in-memory acceptor.
    pub async fn on_prepare(&self, slot: u64, ballot: PxBallot) -> PxPrepareReply {
        self.acceptor.prepare(slot, ballot).await
    }

    /// Phase-2 `Accept` handler — delegates to the in-memory acceptor.
    pub async fn on_accept(&self, entry: PxLogEntry) -> PxAcceptReply {
        self.acceptor.accept(entry).await
    }

    /// Learn a chosen entry (apply to state machine).
    pub fn learn(&self, entry: &PxLogEntry) {
        self.learner.learn(entry.clone());
    }

    /// Read the currently accepted value at a slot (for verification).
    pub async fn accepted_at(&self, slot: u64) -> Option<PxLogEntry> {
        self.acceptor.accepted_at(slot)
    }

    /// Read the currently promised ballot at a slot (for verification).
    pub async fn promised_at(&self, slot: u64) -> Option<PxBallot> {
        self.acceptor.promised_at(slot)
    }
}
