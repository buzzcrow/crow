//! `PxLocalReplica` — local acceptor/learner for a single `CrowKV` group member.
//!
//! Wraps the consensus state (`PxAcceptor`) and learner (`PxLearner`).
//! All proposer/KV logic has been moved to `PxGroup` and `KvStore`.

use crate::cluster::health::HealthReport;
use crate::cluster::replica::{Replica, ReplicaHandler};
use crate::cluster::shutdown::ShutdownReport;
use crate::cluster::snapshot::{KvStoreSnapshot, LocalReplicaSnapshot};
use crate::paxos::acceptor::PxAcceptor;
use crate::paxos::learner::PxLearner;
use crate::paxos::roles::{Acceptor, Learner, PxAcceptReply, PxBallot, PxLogEntry, PxPrepareReply};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tracing::{debug, info};

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
    /// Idempotency gate for [`Self::shutdown`].
    shutdown_started: AtomicBool,
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
    async fn on_prepare(&self, slot: u64, ballot: PxBallot, _group_id: u64) -> Result<PxPrepareReply, tonic::Status> {
        Ok(self.on_prepare(slot, ballot).await)
    }

    async fn on_accept(&self, entry: PxLogEntry, _group_id: u64) -> Result<PxAcceptReply, tonic::Status> {
        Ok(self.on_accept(entry).await)
    }
}

impl PxLocalReplica {
    #[must_use]
    pub fn new(id: u64, role: PxLocalReplicaRole) -> Self {
        Self {
            id,
            role,
            acceptor: PxAcceptor::new(),
            learner: PxLearner::new(),
            voting: true,
            shutdown_started: AtomicBool::new(false),
        }
    }

    /// Cascade shutdown into acceptor + learner.
    ///
    /// Acceptor and learner currently rely on `Drop` for resource release
    /// (slot list reclaim, in-memory KV map drop). The explicit cascade is a
    /// hook for future persistence layers (P3) which will need to flush.
    #[tracing::instrument(level = "debug", skip_all, fields(replica_l_id = self.id))]
    #[allow(clippy::unused_async)] // async kept for cascade uniformity (P3 will await flush)
    pub async fn shutdown(&self, _per_layer_timeout: Duration) -> ShutdownReport {
        if self.shutdown_started.swap(true, Ordering::AcqRel) {
            debug!(replica_l_id = self.id, "PxLocalReplica::shutdown is a no-op (already shut down)");
            return ShutdownReport::new();
        }
        info!(replica_l_id = self.id, "PxLocalReplica shutdown (acceptor/learner cleanup deferred to Drop)");
        ShutdownReport::new()
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

    /// Point-in-time snapshot for the topology endpoint. Exposes only cheap
    /// (`O(1)`) data: the kv-store key count via `DashMap::len()`.
    #[allow(clippy::cast_possible_truncation)]
    #[must_use]
    pub fn snapshot(&self) -> LocalReplicaSnapshot {
        let role = if self.role == PxLocalReplicaRole::Leader { "leader" } else { "follower" };
        LocalReplicaSnapshot {
            id: self.id,
            role,
            voting: self.voting,
            kv_store: KvStoreSnapshot {
                key_count: self.learner.store().len() as u64,
            },
        }
    }

    /// Cached health for this local replica.
    ///
    /// V1 reports `Unhealthy` if `shutdown()` has run (resources released);
    /// otherwise `Ok`. P3 will extend this to surface acceptor/learner
    /// persistence I/O errors.
    #[must_use]
    pub fn health(&self) -> HealthReport {
        if self.shutdown_started.load(Ordering::Acquire) {
            HealthReport::unhealthy(format!("local replica {} has been shut down", self.id))
        } else {
            HealthReport::ok()
        }
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
    #[allow(clippy::unused_async)]
    pub async fn accepted_at(&self, slot: u64) -> Option<PxLogEntry> {
        self.acceptor.accepted_at(slot)
    }

    /// Read the currently promised ballot at a slot (for verification).
    #[allow(clippy::unused_async)]
    pub async fn promised_at(&self, slot: u64) -> Option<PxBallot> {
        self.acceptor.promised_at(slot)
    }
}
