use std::sync::atomic::{AtomicU64, Ordering};

use tokio::time::{sleep, Duration};
use tracing::{debug, error, info, warn};

use crate::cluster::local_replica::PxLocalReplica;
use crate::cluster::remote_replica::PxRemoteReplica;
use crate::cluster::replica::{Replica, ReplicaClient, ReplicaHandler};
use crate::common::config::PaxosConfig;
use crate::paxos::error::{PxPaxosError, PxPaxosPhase, PxRetryAction};
use crate::paxos::roles::{PxAcceptReply, PxBallot, PxLogEntry, PxLogEntryKind, PxPrepareReply};
use crate::paxos::{PxGroupId, PxNodeId};

pub struct PxGroup {
    pub group_id: PxGroupId,
    pub leader_id: PxNodeId,
    cached_quorum: usize,
    local_replica: PxLocalReplica,
    remote_replicas: Vec<RemoteReplicaKind>,
    valid_replica_count: usize,
    next_slot: AtomicU64,
    /// When true, always run Phase-1 Prepare before Accept (classic Paxos).
    force_classic: bool,
}

impl std::fmt::Debug for PxGroup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PxGroup")
            .field("group_id", &self.group_id)
            .field("cached_quorum", &self.cached_quorum)
            .field("leader_id", &self.leader_id)
            .field("local_replica_id", &self.local_replica.id)
            .field("valid_replica_count", &self.valid_replica_count)
            .field("remote_replicas_len", &self.remote_replicas.len())
            .finish()
    }
}

impl PxGroup {
    pub fn new(group_id: PxGroupId, local_replica: PxLocalReplica) -> Self {
        let mut group = Self {
            group_id,
            leader_id: 0,
            cached_quorum: 0,
            local_replica,
            remote_replicas: Vec::new(),
            valid_replica_count: 0,
            next_slot: AtomicU64::new(1),
            force_classic: false,
        };
        group.recompute_quorum();
        group
    }

    // ── Getters ───────────────────────────────────────────────────

    pub fn group_id(&self) -> PxGroupId {
        self.group_id
    }

    pub fn local_replica(&self) -> &PxLocalReplica {
        &self.local_replica
    }

    pub fn force_classic(&self) -> bool {
        self.force_classic
    }

    pub fn is_leader(&self) -> bool {
        self.leader_id == self.local_replica.id
    }

    pub fn quorum(&self) -> usize {
        self.cached_quorum
    }

    /// Number of remote replica slots (including placeholders).
    pub fn remote_replica_count(&self) -> usize {
        self.remote_replicas.len()
    }

    /// Get a remote replica by ID.
    pub fn get_remote_replica(&self, node_id: PxNodeId) -> Option<&PxRemoteReplica> {
        let idx = node_id as usize;
        self.remote_replicas.get(idx).and_then(|r| r.as_real())
    }

    pub fn member_endpoint(&self, node_id: PxNodeId) -> Option<&str> {
        let idx = node_id as usize;
        self.remote_replicas.get(idx).and_then(|r| r.endpoint())
    }

    /// Return the endpoint of the current leader, if known.
    /// Returns None if local replica is not the leader (caller should forward request).
    pub fn leader_endpoint(&self) -> Option<String> {
        if self.is_leader() {
            // Local is leader, no need to forward
            None
        } else {
            // Local is not leader, check if we have the leader's endpoint
            let idx = self.leader_id as usize;
            self.remote_replicas
                .get(idx)
                .and_then(|r| r.endpoint())
                .map(str::to_string)
        }
    }

    // ── Setters ───────────────────────────────────────────────────

    pub fn set_leader_id(&mut self, leader_id: PxNodeId) {
        self.leader_id = leader_id;
    }

    pub fn set_force_classic(&mut self, force: bool) {
        self.force_classic = force;
    }

    /// Set up the group with a list of remote replicas.
    pub fn set_remote_replicas(&mut self, remote_replicas: Vec<PxRemoteReplica>) {
        let max_node_id = remote_replicas.iter().map(|r| r.node_id).max().unwrap_or(0);
        let vec_len = (max_node_id + 1) as usize;
        self.remote_replicas = (0..vec_len)
            .map(|_| RemoteReplicaKind::Placeholder)
            .collect();
        self.valid_replica_count = 0;

        for remote in remote_replicas {
            let idx = remote.node_id as usize;
            if idx < self.remote_replicas.len() {
                self.remote_replicas[idx] = RemoteReplicaKind::Real(remote);
                self.valid_replica_count += 1;
            }
        }
        self.recompute_quorum();
    }

    pub fn update_member_endpoint(
        &mut self,
        node_id: PxNodeId,
        endpoint: impl Into<String>,
    ) -> Option<String> {
        let endpoint = endpoint.into();
        let idx = node_id as usize;

        if let Some(replica) = self.remote_replicas.get_mut(idx) {
            if let RemoteReplicaKind::Real(remote) = replica {
                let old_endpoint = remote.endpoint.clone();
                endpoint.clone_into(&mut remote.endpoint);
                Some(old_endpoint)
            } else {
                None
            }
        } else {
            Some(endpoint)
        }
    }

    // ── Add/Remove ────────────────────────────────────────────────

    /// Add a remote replica to the group.
    pub fn add_remote_replica(&mut self, remote: PxRemoteReplica) {
        info!(
            group_id = self.group_id,
            remote_id = remote.node_id,
            endpoint = remote.endpoint,
            "added remote replica to group"
        );

        let idx = remote.node_id as usize;
        // Ensure vec is large enough
        while idx >= self.remote_replicas.len() {
            self.remote_replicas
                .push(RemoteReplicaKind::Placeholder);
        }

        // Check if this was a placeholder before
        if matches!(
            self.remote_replicas[idx],
            RemoteReplicaKind::Placeholder
        ) {
            self.valid_replica_count += 1;
        }

        self.remote_replicas[idx] = RemoteReplicaKind::Real(remote);
        self.recompute_quorum();
    }

    // ── Proposer ──────────────────────────────────────────────

    /// Propose an opaque payload through Paxos. Returns the slot if chosen,
    /// or an error string.
    pub async fn propose(
        &self,
        payload: Vec<u8>,
        client_id: Option<u64>,
        seq: Option<u64>,
    ) -> ProposeResult {
        let replica = &self.local_replica;

        if !self.is_leader() {
            return ProposeResult::NotLeader {
                leader_hint: self.leader_endpoint().unwrap_or_default(),
            };
        }

        let group_id = self.group_id;
        let total = self.valid_replica_count + 1;
        let quorum = total / 2 + 1;
        let mut slot = self.next_slot.fetch_add(1, Ordering::SeqCst);
        let mut last_error = String::new();

        info!(
            group_id,
            my_id = self.local_replica.id,
            client_id = ?client_id,
            seq = ?seq,
            peer_count = self.valid_replica_count,
            total,
            quorum,
            "start paxos proposal"
        );

        'slot_retry: for _slot_attempt in 0..PaxosConfig::DEFAULT.max_slot_retries {
            let base_entry = self.base_entry(slot, payload.clone(), client_id, seq);
            let mut force_prepare = self.force_classic; // Classic: always prepare; Leader: Phase-2 only
            let mut min_round = 0u64;

            for attempt in 0..PaxosConfig::DEFAULT.max_paxos_retries {
                let mut entry = base_entry.clone();
                let mut adopted_foreign_value = false;
                debug!(
                    group_id,
                    slot, attempt, force_prepare, min_round, "start paxos attempt"
                );

                if force_prepare {
                    match self
                        .run_prepare_phase(
                            replica,
                            slot,
                            payload.as_slice(),
                            client_id,
                            seq,
                            quorum,
                            min_round,
                        )
                        .await
                    {
                        PrepareAttempt::Proceed {
                            entry: prepared_entry,
                            foreign_value,
                        } => {
                            entry = prepared_entry;
                            adopted_foreign_value = foreign_value;
                        }
                        PrepareAttempt::Retry {
                            next_min_round,
                            error,
                        } => {
                            warn!(
                                group_id,
                                slot,
                                attempt,
                                next_min_round,
                                error = error.keyword(),
                                "prepare retry requested"
                            );
                            last_error = error.keyword().to_string();
                            min_round = next_min_round;
                            sleep(self.retry_backoff(attempt)).await;
                            continue;
                        }
                        PrepareAttempt::Fail { error } => {
                            error!(
                                group_id,
                                slot,
                                attempt,
                                error = error.keyword(),
                                "prepare failed"
                            );
                            last_error = error.keyword().to_string();
                            break;
                        }
                    }
                } else if min_round > entry.ballot.round {
                    entry.ballot.round = min_round;
                }

                match self.run_accept_phase(replica, &entry, quorum).await {
                    AcceptAttempt::Chosen => {
                        replica.learn(&entry);
                        info!(
                            group_id,
                            slot = entry.slot,
                            round = entry.ballot.round,
                            leader_id = entry.ballot.leader_id,
                            "paxos entry chosen and learned locally"
                        );

                        if adopted_foreign_value || entry.payload != payload {
                            last_error = PxPaxosError::ForeignValueChosen { slot }
                                .keyword()
                                .to_string();
                            warn!(
                                group_id,
                                slot,
                                error = last_error,
                                "foreign value chosen; retrying client value on next slot"
                            );
                            slot = self.next_slot.fetch_add(1, Ordering::SeqCst);
                            continue 'slot_retry;
                        }

                        return ProposeResult::Chosen { slot };
                    }
                    AcceptAttempt::Retry {
                        next_min_round,
                        error,
                    } => {
                        warn!(
                            group_id,
                            slot,
                            attempt,
                            next_min_round,
                            error = error.keyword(),
                            "accept retry requested; running prepare with higher ballot"
                        );
                        last_error = error.keyword().to_string();
                        min_round = next_min_round;
                        force_prepare = true;
                        sleep(self.retry_backoff(attempt)).await;
                    }
                    AcceptAttempt::Fail { error } => {
                        error!(
                            group_id,
                            slot,
                            attempt,
                            error = error.keyword(),
                            "accept failed"
                        );
                        last_error = error.keyword().to_string();
                        break;
                    }
                }
            }

            warn!(
                group_id,
                slot, last_error, "slot proposal failed; retrying on next slot"
            );
            slot = self.next_slot.fetch_add(1, Ordering::SeqCst);
        }

        error!(
            group_id,
            last_error,
            max_paxos_retries = PaxosConfig::DEFAULT.max_paxos_retries,
            max_slot_retries = PaxosConfig::DEFAULT.max_slot_retries,
            "paxos proposal exhausted retry budget"
        );
        ProposeResult::Err(if last_error.is_empty() {
            "paxos retry exhausted".to_string()
        } else {
            format!(
                "{} (after {} paxos retries, {} slot retries)",
                last_error,
                PaxosConfig::DEFAULT.max_paxos_retries,
                PaxosConfig::DEFAULT.max_slot_retries
            )
        })
    }

    fn base_entry(
        &self,
        slot: u64,
        payload: Vec<u8>,
        client_id: Option<u64>,
        seq: Option<u64>,
    ) -> PxLogEntry {
        PxLogEntry {
            slot,
            ballot: PxBallot {
                round: 0,
                leader_id: self.local_replica.id,
            },
            term: 0,
            kind: PxLogEntryKind::Write,
            payload,
            client_id,
            seq,
        }
    }

    fn consider_accepted(adopted: &mut Option<PxLogEntry>, candidate: PxLogEntry) {
        let should_replace = adopted
            .as_ref()
            .map_or(true, |current| candidate.ballot > current.ballot);
        if should_replace {
            *adopted = Some(candidate);
        }
    }

    async fn run_prepare_phase(
        &self,
        replica: &PxLocalReplica,
        slot: u64,
        payload: &[u8],
        client_id: Option<u64>,
        seq: Option<u64>,
        quorum: usize,
        min_round: u64,
    ) -> PrepareAttempt {
        let mut max_round = min_round;
        if let Some(b) = replica.promised_at(slot).await {
            max_round = max_round.max(b.round);
        }

        let ballot = PxBallot {
            round: max_round + 1,
            leader_id: self.local_replica.id,
        };
        let group_id = self.group_id;
        debug!(
            group_id,
            slot,
            round = ballot.round,
            peer_count = self.valid_replica_count,
            quorum,
            "run prepare phase"
        );
        let mut entry = self.base_entry(slot, payload.to_vec(), client_id, seq);
        entry.ballot = ballot;

        let mut promised = 0usize;
        let mut highest_rejected_round: Option<u64> = None;
        let mut adopted: Option<PxLogEntry> = None;

        match <PxLocalReplica as ReplicaHandler>::on_prepare(replica, slot, ballot, group_id).await
        {
            Ok(PxPrepareReply::Promised { accepted, .. }) => {
                promised += 1;
                if let Some(prev) = accepted {
                    Self::consider_accepted(&mut adopted, prev);
                }
            }
            Ok(PxPrepareReply::Rejected {
                current_promised, ..
            }) => {
                highest_rejected_round = Some(current_promised.round);
            }
            Err(error) => {
                warn!(
                    group_id,
                    slot,
                    replica_id = replica.id,
                    error = %error,
                    "local prepare handler failed"
                );
            }
        }

        for remote in &self.remote_replicas {
            if let RemoteReplicaKind::Real(remote) = remote {
                match remote.send_prepare(slot, ballot, group_id).await {
                    Ok(PxPrepareReply::Promised { accepted, .. }) => {
                        promised += 1;
                        if let Some(prev) = accepted {
                            Self::consider_accepted(&mut adopted, prev);
                        }
                    }
                    Ok(PxPrepareReply::Rejected {
                        current_promised, ..
                    }) => {
                        let candidate = current_promised.round;
                        highest_rejected_round = Some(
                            highest_rejected_round.map_or(candidate, |r| r.max(candidate)),
                        );
                    }
                    Err(error) => {
                        warn!(
                            group_id,
                            slot,
                            remote_id = remote.node_id,
                            endpoint = remote.endpoint,
                            error = %error,
                            "prepare rpc failed"
                        );
                    }
                }
            }
        }

        if promised < quorum {
            if let Some(round) = highest_rejected_round {
                let error = PxPaxosError::PrepareRejected {
                    promised: PxBallot::new(round, 0),
                };
                let next_min_round = match error.retry_action() {
                    PxRetryAction::RetrySameSlot {
                        min_round: Some(round),
                        ..
                    } => round,
                    _ => round,
                };
                return PrepareAttempt::Retry {
                    next_min_round,
                    error,
                };
            }
            return PrepareAttempt::Fail {
                error: PxPaxosError::QuorumUnavailable {
                    phase: PxPaxosPhase::Prepare,
                },
            };
        }

        let mut foreign_value = false;
        if let Some(prev) = adopted {
            foreign_value = prev.payload.as_slice() != payload;
            if foreign_value {
                warn!(
                    group_id,
                    slot,
                    adopted_round = prev.ballot.round,
                    adopted_leader_id = prev.ballot.leader_id,
                    "prepare adopted foreign value"
                );
            }
            entry = prev;
        }
        PrepareAttempt::Proceed {
            entry,
            foreign_value,
        }
    }

    async fn run_accept_phase(
        &self,
        replica: &PxLocalReplica,
        entry: &PxLogEntry,
        quorum: usize,
    ) -> AcceptAttempt {
        let mut accepted = 0usize;
        let mut highest_rejected_round: Option<u64> = None;
        let group_id = self.group_id;
        debug!(
            group_id,
            slot = entry.slot,
            round = entry.ballot.round,
            peer_count = self.valid_replica_count,
            quorum,
            "run accept phase"
        );

        match <PxLocalReplica as ReplicaHandler>::on_accept(replica, entry.clone(), group_id).await
        {
            Ok(PxAcceptReply::Accepted { .. }) => {
                accepted += 1;
            }
            Ok(PxAcceptReply::Rejected {
                current_promised, ..
            }) => {
                highest_rejected_round = Some(current_promised.round);
            }
            Err(error) => {
                warn!(
                    group_id,
                    slot = entry.slot,
                    replica_id = replica.id,
                    error = %error,
                    "local accept handler failed"
                );
            }
        }

        for remote in &self.remote_replicas {
            if let RemoteReplicaKind::Real(remote) = remote {
                match remote.send_accept(entry, group_id).await {
                    Ok(PxAcceptReply::Accepted { .. }) => {
                        accepted += 1;
                    }
                    Ok(PxAcceptReply::Rejected {
                        current_promised, ..
                    }) => {
                        let candidate = current_promised.round;
                        highest_rejected_round = Some(
                            highest_rejected_round.map_or(candidate, |r| r.max(candidate)),
                        );
                    }
                    Err(error) => {
                        warn!(
                            group_id,
                            slot = entry.slot,
                            remote_id = remote.node_id,
                            endpoint = remote.endpoint,
                            error = %error,
                            "accept rpc failed"
                        );
                    }
                }
            }
        }

        if accepted >= quorum {
            return AcceptAttempt::Chosen;
        }

        if let Some(round) = highest_rejected_round {
            let error = PxPaxosError::AcceptRejected {
                promised: PxBallot::new(round, 0),
            };
            let next_min_round = match error.retry_action() {
                PxRetryAction::RetrySameSlot {
                    min_round: Some(round),
                    ..
                } => round,
                _ => round + 1,
            };
            return AcceptAttempt::Retry {
                next_min_round,
                error,
            };
        }
        AcceptAttempt::Fail {
            error: PxPaxosError::QuorumUnavailable {
                phase: PxPaxosPhase::Accept,
            },
        }
    }

    fn recompute_quorum(&mut self) {
        let voting_count = self.remote_replicas.iter().filter(|r| r.voting()).count()
            + u32::from(self.local_replica.voting()) as usize;
        self.cached_quorum = (voting_count / 2) + 1;
    }

    fn retry_backoff(&self, attempt: usize) -> Duration {
        let factor = 1u64 << attempt.min(6);
        Duration::from_millis(
            PaxosConfig::DEFAULT
                .retry_base_backoff_ms
                .saturating_mul(factor),
        )
    }
}

/// Remote replica kind - either a real remote replica or a placeholder.
#[derive(Debug)]
pub(crate) enum RemoteReplicaKind {
    Real(PxRemoteReplica),
    Placeholder,
}

impl RemoteReplicaKind {
    fn endpoint(&self) -> Option<&str> {
        match self {
            Self::Real(r) => Some(r.endpoint.as_str()),
            Self::Placeholder => None,
        }
    }

    fn voting(&self) -> bool {
        match self {
            Self::Real(r) => r.voting,
            Self::Placeholder => false,
        }
    }

    fn as_real(&self) -> Option<&PxRemoteReplica> {
        match self {
            Self::Real(r) => Some(r),
            Self::Placeholder => None,
        }
    }
}

/// Result of a `PxGroup::propose` call.
#[derive(Debug)]
pub enum ProposeResult {
    Chosen { slot: u64 },
    NotLeader { leader_hint: String },
    Err(String),
}

enum PrepareAttempt {
    Proceed {
        entry: PxLogEntry,
        foreign_value: bool,
    },
    Retry {
        next_min_round: u64,
        error: PxPaxosError,
    },
    Fail {
        error: PxPaxosError,
    },
}

enum AcceptAttempt {
    Chosen,
    Retry {
        next_min_round: u64,
        error: PxPaxosError,
    },
    Fail {
        error: PxPaxosError,
    },
}
