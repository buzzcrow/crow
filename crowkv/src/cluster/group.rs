#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::too_many_lines)]
#![allow(clippy::missing_fields_in_debug)]

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};

use tokio::sync::Mutex as AsyncMutex;
use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::cluster::election;
use crate::cluster::health::{HealthReport, HealthStatus};
use crate::cluster::local_replica::PxLocalReplica;
use crate::cluster::remote_replica::PxRemoteReplica;
use crate::cluster::replica::{Replica, ReplicaClient, ReplicaHandler};
use crate::cluster::shutdown::ShutdownReport;
use crate::cluster::snapshot::GroupSnapshot;
use crate::common::config::{PaxosConfig, PxElectionConfig};
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
    /// Leader-election / heartbeat / lease tunables for this group's
    /// [`crate::cluster::election::spawn`] driver task.
    election_cfg: PxElectionConfig,
    /// Per-leadership-tenure [`CancellationToken`]. Cancelled in
    /// [`Self::shutdown`] and by every step-down trigger (Step 9.6). The
    /// bulk-Phase-1 sweep (Step 7) and the election driver (Step 9.1+) both
    /// honor it.
    tenure_cancel: CancellationToken,
    /// `JoinHandle` of the spawned election driver (`None` while the driver
    /// has not been started or is disabled). Wrapped in an async mutex so
    /// `shutdown` can `await` it cooperatively without blocking other
    /// readers of `self`.
    driver_handle: AsyncMutex<Option<JoinHandle<()>>>,
    /// Handoff from a freshly elected candidate to the upcoming
    /// `run_leader_state` invocation. Holds `(term, peer_floor,
    /// peer_ceiling)` for bulk Phase 1. Consumed once on Leader-state
    /// entry. Step 9.6.
    pub(crate) pending_leader_handoff: parking_lot::Mutex<Option<PendingLeaderHandoff>>,
    /// Term stamped on becoming leader (Step 9.8). The propose
    /// leadership gate accepts a proposal only when the local replica's
    /// `role == Leader` **and** its `current_term == proposing_term`.
    /// Mismatch on either field means the leader tenure ended (the
    /// driver stepped down or moved to a new term) and the proposal
    /// must fail fast with `NotLeader` instead of racing into Paxos
    /// with stale identity.
    ///
    /// Default `0` matches the default `current_term` of a freshly
    /// constructed [`PxLocalReplica`], so testkit pinned-leader groups
    /// pass the gate without explicit stamping.
    proposing_term: AtomicU64,
}

/// Bundle handed off from `run_candidate_election` to `run_leader_state`
/// when a candidate wins quorum. Carries the floor / ceiling needed by
/// [`PxGroup::run_bulk_phase1`] under the new tenure's cancel token.
#[derive(Clone, Copy, Debug)]
pub struct PendingLeaderHandoff {
    pub term: u64,
    pub peer_floor: u64,
    pub peer_ceiling: u64,
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
            election_cfg: PxElectionConfig::DEFAULT,
            tenure_cancel: CancellationToken::new(),
            driver_handle: AsyncMutex::new(None),
            pending_leader_handoff: parking_lot::Mutex::new(None),
            proposing_term: AtomicU64::new(0),
        };
        group.recompute_quorum();
        group
    }

    /// Override the election driver configuration before
    /// [`Self::start_election_loop`] is called.
    ///
    /// Test helpers (e.g. `testkit::cluster::start_cluster`) flip
    /// `election_driver_disabled = true` here to keep pinned-leader tests
    /// deterministic. Production sites use the default profile.
    pub fn set_election_config(&mut self, cfg: PxElectionConfig) {
        self.election_cfg = cfg;
    }

    /// Snapshot of the active election configuration.
    #[must_use]
    pub fn election_config(&self) -> PxElectionConfig {
        self.election_cfg
    }

    /// Borrow of the group's per-leadership-tenure cancellation token.
    ///
    /// Step 7 (bulk Phase 1) and Steps 9.5–9.7 (heartbeat ticker, lease
    /// step-down, admin step-down) all clone this token to abort
    /// leader-only work cooperatively.
    #[must_use]
    pub fn tenure_cancel(&self) -> CancellationToken {
        self.tenure_cancel.clone()
    }

    /// Spawn the per-group [`election::spawn`] driver task.
    ///
    /// Must be called after the [`PxGroup`] is wrapped in an
    /// [`Arc`] (see `PxKvStore::add_group`) so the driver can hold a
    /// [`Weak`] back-reference. No-op when
    /// `election_cfg.election_driver_disabled` is set or when the driver
    /// has already been started.
    ///
    /// **Step 9.1**: the spawned task is a scaffold loop (observe role,
    /// honor cancel). The state machine fills in across 9.2..9.8.
    pub async fn start_election_loop(self: &Arc<Self>) {
        if self.election_cfg.election_driver_disabled {
            debug!(
                group_id = self.group_id,
                replica_l_id = self.local_replica.id,
                "election driver disabled by config; not spawning"
            );
            return;
        }
        let mut slot = self.driver_handle.lock().await;
        if slot.is_some() {
            debug!(
                group_id = self.group_id,
                replica_l_id = self.local_replica.id,
                "election driver already running; not spawning again"
            );
            return;
        }
        let weak: Weak<Self> = Arc::downgrade(self);
        let handle = election::spawn(weak, self.election_cfg, self.tenure_cancel.clone());
        *slot = Some(handle);
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
            self.remote_replicas.get(idx).and_then(|r| r.endpoint()).map(str::to_string)
        }
    }

    // ── Setters ───────────────────────────────────────────────────

    /// Set the leader replica ID for this group.
    ///
    /// # Note
    /// This is a test-only helper that directly sets the leader ID without
    /// notifying remote replicas. In production, leader selection should be
    /// automatic via Paxos, with leader status propagated through RPC to
    /// remote replicas. This function bypasses that mechanism and should only
    /// be used in tests where manual leader assignment is acceptable.
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
        self.remote_replicas = (0..vec_len).map(|_| RemoteReplicaKind::Placeholder).collect();
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

    pub fn update_member_endpoint(&mut self, node_id: PxNodeId, endpoint: impl Into<String>) -> Option<String> {
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

    // ── Snapshot ──────────────────────────────────────────────────

    /// Point-in-time snapshot of this group: local replica + each remote.
    /// Used by `/topology`.
    #[must_use]
    pub fn snapshot(&self) -> GroupSnapshot {
        let local_replica = self.local_replica.snapshot();
        let remotes = self
            .remote_replicas
            .iter()
            .filter_map(|r| match r {
                RemoteReplicaKind::Real(r) => Some(r.snapshot()),
                RemoteReplicaKind::Placeholder => None,
            })
            .collect();
        GroupSnapshot {
            group_id: self.group_id,
            leader_id: self.leader_id,
            force_classic: self.force_classic,
            local_replica,
            remotes,
        }
    }

    // ── Health ────────────────────────────────────────────────────

    /// Aggregate cached health across the local replica and all remotes.
    ///
    /// Each layer decides its own status; here we take the worst-of from
    /// children and additionally downgrade to `Degraded` if fewer than
    /// `quorum` voting replicas are reachable (cached). `Unhealthy` if
    /// quorum is impossible.
    #[must_use]
    pub fn health(&self) -> HealthReport {
        let mut report = HealthReport::ok();

        // 1. Local replica.
        let local = self.local_replica.health();
        report.merge_child(&format!("local#{}", self.local_replica.id), local);

        // 2. Remotes — count how many voting members are believed Ok.
        let mut ok_voting = u32::from(self.local_replica.voting() && report.status != HealthStatus::Unhealthy);
        for remote in &self.remote_replicas {
            if let RemoteReplicaKind::Real(r) = remote {
                let r_health = r.health();
                let voting_ok = r.voting() && r_health.status == HealthStatus::Ok;
                if voting_ok {
                    ok_voting += 1;
                }
                report.merge_child(&format!("remote#{}", r.node_id), r_health);
            }
        }

        // 3. Quorum check.
        let quorum = self.cached_quorum as u32;
        if quorum > 0 && ok_voting < quorum {
            // Worst-of with what children already produced. If any child is
            // already Unhealthy, that wins; otherwise mark Degraded.
            if report.status != HealthStatus::Unhealthy {
                report.status = HealthStatus::Degraded;
            }
            report.note(format!(
                "group {}: only {ok_voting}/{} voting replicas reachable (quorum {quorum})",
                self.group_id,
                self.valid_replica_count + 1,
            ));
        }

        report
    }

    /// Report JSON-serializable health info for API responses.
    #[must_use]
    pub fn report_health(&self) -> crate::cluster::health_info::HealthGroupInfo {
        let g_health = self.health();
        let role = if self.leader_id == self.local_replica.id { "leader" } else { "follower" };
        let local_health = self.local_replica.health();
        let local_replica = crate::cluster::health_info::HealthReplicaInfo {
            id: self.local_replica.id,
            role: role.to_string(),
            status: local_health.status.as_str().to_string(),
            messages: local_health.messages,
        };
        let remotes: Vec<crate::cluster::health_info::HealthRemoteInfo> = self
            .remote_replicas
            .iter()
            .filter_map(|r| match r {
                RemoteReplicaKind::Real(remote) => {
                    let h = remote.health();
                    Some(crate::cluster::health_info::HealthRemoteInfo {
                        id: remote.node_id,
                        endpoint: remote.endpoint.clone(),
                        status: h.status.as_str().to_string(),
                        messages: h.messages,
                    })
                }
                RemoteReplicaKind::Placeholder => None,
            })
            .collect();
        crate::cluster::health_info::HealthGroupInfo {
            group_id: self.group_id,
            status: g_health.status.as_str().to_string(),
            messages: g_health.messages,
            local_replica,
            remotes,
        }
    }

    // ── Shutdown ──────────────────────────────────────────────────

    /// Cascade shutdown through this group's replicas.
    ///
    /// Iterates real remote replicas and closes their gRPC channels, then
    /// shuts down the local replica (which in turn cascades through
    /// `acceptor` / `learner` / `slot_list` / `kv_store`). Continues on errors;
    /// aggregated `critical:` messages are returned.
    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(group_id = self.group_id, replica_l_id = self.local_replica.id)
    )]
    pub async fn shutdown(&self, per_layer_timeout: Duration) -> ShutdownReport {
        let mut report = ShutdownReport::new();
        info!(
            group_id = self.group_id,
            replica_l_id = self.local_replica.id,
            remote_count = self.valid_replica_count,
            "PxGroup shutdown starting"
        );

        // 0. Cancel the per-tenure token and await the election driver
        //    (Step 9.1). Driver is cooperative; a 100 ms scaffold tick is
        //    well within `per_layer_timeout`.
        self.tenure_cancel.cancel();
        if let Some(handle) = self.driver_handle.lock().await.take() {
            match tokio::time::timeout(per_layer_timeout, handle).await {
                Ok(Ok(())) => {}
                Ok(Err(join_err)) => {
                    warn!(
                        group_id = self.group_id,
                        error = %join_err,
                        "election driver task panicked during shutdown"
                    );
                }
                Err(_) => {
                    warn!(
                        group_id = self.group_id,
                        timeout_ms = per_layer_timeout.as_millis() as u64,
                        "election driver task did not exit within per-layer timeout"
                    );
                }
            }
        }

        // 1. Close remote gRPC channels first so no in-flight RPCs spin.
        for remote in &self.remote_replicas {
            if let RemoteReplicaKind::Real(remote) = remote {
                let sub = remote.shutdown(per_layer_timeout).await;
                report.merge(sub);
            }
        }

        // 2. Shutdown local replica.
        let sub = self.local_replica.shutdown(per_layer_timeout).await;
        report.merge(sub);

        info!(group_id = self.group_id, error_count = report.errors.len(), "PxGroup shutdown complete");
        report
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
            self.remote_replicas.push(RemoteReplicaKind::Placeholder);
        }

        // Check if this was a placeholder before
        if matches!(self.remote_replicas[idx], RemoteReplicaKind::Placeholder) {
            self.valid_replica_count += 1;
        }

        self.remote_replicas[idx] = RemoteReplicaKind::Real(remote);
        self.recompute_quorum();
    }

    /// Remove a remote replica by node ID. Returns true if it was present.
    pub fn remove_remote_replica(&mut self, node_id: PxNodeId) -> bool {
        let idx = node_id as usize;
        if idx < self.remote_replicas.len() && matches!(self.remote_replicas[idx], RemoteReplicaKind::Real(_)) {
            info!(group_id = self.group_id, remote_id = node_id, "removed remote replica from group");
            self.remote_replicas[idx] = RemoteReplicaKind::Placeholder;
            self.valid_replica_count -= 1;
            self.recompute_quorum();
            return true;
        }
        false
    }

    /// Node IDs of every voting real remote replica. Used by the election
    /// driver (Step 9.3) to fan out `RequestVote` / `Heartbeat`.
    #[must_use]
    pub fn voting_remote_ids(&self) -> Vec<PxNodeId> {
        self.remote_replicas
            .iter()
            .filter_map(|r| match r {
                RemoteReplicaKind::Real(remote) if remote.voting => Some(remote.node_id),
                _ => None,
            })
            .collect()
    }

    /// Return info about all real remote replicas: `(node_id, endpoint)`.
    pub fn remote_replica_info(&self) -> Vec<(PxNodeId, &str)> {
        self.remote_replicas
            .iter()
            .filter_map(|r| match r {
                RemoteReplicaKind::Real(remote) => Some((remote.node_id, remote.endpoint.as_str())),
                RemoteReplicaKind::Placeholder => None,
            })
            .collect()
    }

    // ── Proposer ──────────────────────────────────────────────

    /// Propose an opaque payload through Paxos. Returns the slot if chosen,
    /// or an error string.
    /// Stamp the term under which this group accepts proposals. Called
    /// by the election driver on `become_leader()` (Step 9.8). The
    /// propose leadership gate compares this against
    /// `current_term_snapshot()` and rejects proposals whose stamped
    /// term has been superseded.
    pub fn stamp_proposing_term(&self, term: u64) {
        self.proposing_term.store(term, Ordering::Release);
    }

    /// Current value of [`Self::stamp_proposing_term`].
    #[must_use]
    pub fn proposing_term(&self) -> u64 {
        self.proposing_term.load(Ordering::Acquire)
    }

    pub async fn propose(&self, payload: Vec<u8>, client_id: Option<u64>, seq: Option<u64>) -> ProposeResult {
        let replica = &self.local_replica;

        // Step 9.8 leadership gate. Checks BOTH:
        //   * role == Leader  -- captures the role atomic flipped by
        //     become_leader / become_follower.
        //   * current_term == proposing_term -- captures the case where
        //     the local replica advanced into a new term (became
        //     follower under HigherTerm, then re-elected) without the
        //     proposing tenure having stamped the new term yet.
        // Either miss surfaces as `NotLeader { hint: leader_endpoint() }`
        // before slot allocation, draining in-flight client proposals
        // per the step-down sequence (Step 9.6 §6). The legacy
        // `self.leader_id == self.local_replica.id` check is preserved
        // as a fallback for pinned-leader testkit groups that never
        // call become_leader (and therefore observe role=Leader from
        // construction time with current_term=0=proposing_term).
        let role_is_leader = replica.role() == crate::cluster::local_replica::PxLocalReplicaRole::Leader;
        let current_term = replica.current_term_snapshot();
        let proposing_term = self.proposing_term();
        let legacy_pinned = self.leader_id == replica.id;
        let gate_pass = legacy_pinned || (role_is_leader && current_term == proposing_term);
        if !gate_pass {
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
                debug!(group_id, slot, attempt, force_prepare, min_round, "start paxos attempt");

                if force_prepare {
                    match self.run_prepare_phase(replica, slot, payload.as_slice(), client_id, seq, quorum, min_round).await {
                        PrepareAttempt::Proceed {
                            entry: prepared_entry,
                            foreign_value,
                        } => {
                            entry = prepared_entry;
                            adopted_foreign_value = foreign_value;
                        }
                        PrepareAttempt::Retry { next_min_round, error } => {
                            warn!(group_id, slot, attempt, next_min_round, error = error.keyword(), "prepare retry requested");
                            last_error = error.keyword().to_string();
                            min_round = next_min_round;
                            sleep(Self::retry_backoff(attempt)).await;
                            continue;
                        }
                        PrepareAttempt::Fail { error } => {
                            error!(group_id, slot, attempt, error = error.keyword(), "prepare failed");
                            if let PxPaxosError::TermStale { current_term } = &error {
                                warn!(group_id, slot, current_term, "stepping down: peer term observed during prepare");
                                replica.become_follower(*current_term);
                                return ProposeResult::NotLeader {
                                    leader_hint: self.leader_endpoint().unwrap_or_default(),
                                };
                            }
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

                        if adopted_foreign_value || *entry.payload != payload {
                            last_error = PxPaxosError::ForeignValueChosen { slot }.keyword().to_string();
                            warn!(group_id, slot, error = last_error, "foreign value chosen; retrying client value on next slot");
                            slot = self.next_slot.fetch_add(1, Ordering::SeqCst);
                            continue 'slot_retry;
                        }

                        return ProposeResult::Chosen { slot };
                    }
                    AcceptAttempt::Retry { next_min_round, error } => {
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
                        sleep(Self::retry_backoff(attempt)).await;
                    }
                    AcceptAttempt::Fail { error } => {
                        error!(group_id, slot, attempt, error = error.keyword(), "accept failed");
                        if let PxPaxosError::TermStale { current_term } = &error {
                            warn!(group_id, slot, current_term, "stepping down: peer term observed during accept");
                            replica.become_follower(*current_term);
                            return ProposeResult::NotLeader {
                                leader_hint: self.leader_endpoint().unwrap_or_default(),
                            };
                        }
                        last_error = error.keyword().to_string();
                        break;
                    }
                }
            }

            warn!(group_id, slot, last_error, "slot proposal failed; retrying on next slot");
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

    /// Bulk Phase 1: a new leader's open-prefix repair sweep over
    /// `[floor + 1, ceiling]`.
    ///
    /// Per `doc/todo_leader.md` Step 7 and `design-leader-election.md` §4:
    ///
    /// - `floor`  = `max(local.contiguous_chosen, peer_contiguous_chosen_max)`
    ///   — values from peer `RequestVote` / `PreVote` replies (Step 9 supplies
    ///   the aggregate via `peer_contiguous_chosen_max`).
    /// - `ceiling` = `max(local.acceptor.highest_seen_slot,
    ///                  self.next_slot - 1,
    ///                  peer_highest_seen_slot_max)`.
    ///
    /// For each slot in `[floor + 1, ceiling]` (batched by
    /// `cfg.bulk_prepare_window`):
    /// 1. Run Phase-1 `Prepare` at ballot `(0, me)` under term `T`.
    /// 2. If `PrepareAttempt::Proceed` adopted a previously-Accepted value,
    ///    re-Accept that value. Otherwise emit a `NoOp` entry so the slot is
    ///    decided (and the contiguous-chosen watermark can advance).
    /// 3. Re-Accept via the existing `run_accept_phase`.
    ///
    /// After issuing (not waiting on completion of) the batch, `next_slot` is
    /// bumped to `ceiling + 1` so steady-state proposals continue past the
    /// repaired range (§4.4).
    ///
    /// Cancellation: the `cancel` token is checked before each slot. On
    /// cancel the loop aborts without re-Accepting any further slots — the
    /// next leader will redo the sweep (§8 "Cancel any in-flight bulk
    /// Phase-1 repair").
    #[tracing::instrument(level = "info", skip_all, fields(group_id = self.group_id, term = term))]
    pub async fn run_bulk_phase1(
        &self,
        term: u64,
        peer_contiguous_chosen_max: u64,
        peer_highest_seen_slot_max: u64,
        cfg: PxElectionConfig,
        cancel: tokio_util::sync::CancellationToken,
    ) {
        let replica = &self.local_replica;
        let group_id = self.group_id;
        let total = self.valid_replica_count + 1;
        let quorum = total / 2 + 1;

        let floor = replica.contiguous_chosen().max(peer_contiguous_chosen_max);
        let local_ceiling = replica.highest_seen_slot();
        let next_slot_minus_one = self.next_slot.load(Ordering::Acquire).saturating_sub(1);
        let ceiling = local_ceiling.max(next_slot_minus_one).max(peer_highest_seen_slot_max);

        info!(
            group_id,
            term,
            floor,
            ceiling,
            local_contiguous_chosen = replica.contiguous_chosen(),
            local_highest_seen_slot = local_ceiling,
            peer_contiguous_chosen_max,
            peer_highest_seen_slot_max,
            "bulk phase 1 start"
        );

        if ceiling <= floor {
            // Nothing to repair.
            info!(group_id, term, "bulk phase 1 skipped (empty range)");
            return;
        }

        let mut slots_repaired = 0u64;
        let window = cfg.bulk_prepare_window.max(1);

        for slot in (floor + 1)..=ceiling {
            if cancel.is_cancelled() {
                warn!(group_id, term, slot, slots_repaired, "bulk phase 1 cancelled (step down)");
                return;
            }
            if slots_repaired >= window {
                // Yield between windows so the runtime can interleave other
                // group work. Step 9 may re-enter this loop after the yield.
                tokio::task::yield_now().await;
                slots_repaired = 0;
            }

            // Issue Phase-1 Prepare at ballot (0, me) under term T. We pass an
            // empty payload so any adopted foreign value strictly comes from a
            // remote's previously-Accepted entry; if none exist the entry
            // stays a NoOp (we re-tag below).
            let attempt = self.run_prepare_phase(replica, slot, &[], None, None, quorum, 0).await;
            let mut entry = match attempt {
                PrepareAttempt::Proceed { entry, .. } => entry,
                PrepareAttempt::Retry { error, .. } | PrepareAttempt::Fail { error } => {
                    warn!(group_id, term, slot, error = error.keyword(), "bulk phase 1 prepare failed; will be retried by next leader");
                    continue;
                }
            };

            // If no foreign value was adopted (entry still has the empty
            // payload we passed in), emit a NoOp so the slot is decided.
            if entry.payload.is_empty() {
                entry.kind = PxLogEntryKind::NoOp;
            }
            entry.term = term;

            match self.run_accept_phase(replica, &entry, quorum).await {
                AcceptAttempt::Chosen => {
                    replica.learn(&entry);
                    slots_repaired += 1;
                }
                AcceptAttempt::Retry { error, .. } | AcceptAttempt::Fail { error } => {
                    warn!(group_id, term, slot, error = error.keyword(), "bulk phase 1 accept failed; will be retried by next leader");
                }
            }
        }

        // Steady-state proposals continue past the repaired range.
        let next = ceiling.saturating_add(1);
        self.next_slot.fetch_max(next, Ordering::AcqRel);
        info!(group_id, term, ceiling, next_slot = next, "bulk phase 1 done");
    }

    fn base_entry(&self, slot: u64, payload: Vec<u8>, client_id: Option<u64>, seq: Option<u64>) -> PxLogEntry {
        PxLogEntry {
            slot,
            ballot: PxBallot {
                round: 0,
                leader_id: self.local_replica.id,
            },
            term: self.local_replica.current_term_snapshot(),
            kind: PxLogEntryKind::Write,
            payload: Arc::new(payload),
            client_id,
            seq,
        }
    }

    fn consider_accepted(adopted: &mut Option<PxLogEntry>, candidate: PxLogEntry) {
        let should_replace = adopted.as_ref().map_or(true, |current| candidate.ballot > current.ballot);
        if should_replace {
            *adopted = Some(candidate);
        }
    }

    #[allow(clippy::too_many_arguments)]
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
        debug!(group_id, slot, round = ballot.round, peer_count = self.valid_replica_count, quorum, "run prepare phase");
        let mut entry = self.base_entry(slot, payload.to_vec(), client_id, seq);
        entry.ballot = ballot;
        let term = entry.term;

        let mut promised = 0usize;
        let mut highest_rejected_round: Option<u64> = None;
        let mut highest_seen_term: Option<u64> = None;
        let mut adopted: Option<PxLogEntry> = None;

        match <PxLocalReplica as ReplicaHandler>::on_prepare(replica, slot, ballot, term, group_id).await {
            Ok(PxPrepareReply::Promised { accepted, .. }) => {
                promised += 1;
                if let Some(prev) = accepted {
                    Self::consider_accepted(&mut adopted, prev);
                }
            }
            Ok(PxPrepareReply::Rejected { current_promised, .. }) => {
                highest_rejected_round = Some(current_promised.round);
            }
            Ok(PxPrepareReply::TermStale { new_term, .. }) => {
                highest_seen_term = Some(highest_seen_term.map_or(new_term, |t| t.max(new_term)));
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
                match remote.send_prepare(slot, ballot, term, group_id).await {
                    Ok(PxPrepareReply::Promised { accepted, .. }) => {
                        promised += 1;
                        if let Some(prev) = accepted {
                            Self::consider_accepted(&mut adopted, prev);
                        }
                    }
                    Ok(PxPrepareReply::Rejected { current_promised, .. }) => {
                        let candidate = current_promised.round;
                        highest_rejected_round = Some(highest_rejected_round.map_or(candidate, |r| r.max(candidate)));
                    }
                    Ok(PxPrepareReply::TermStale { new_term, .. }) => {
                        highest_seen_term = Some(highest_seen_term.map_or(new_term, |t| t.max(new_term)));
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

        if let Some(new_term) = highest_seen_term {
            // A peer's `current_term > term`. The proposer is a stale leader;
            // bubble up `TermStale` so the group-level propose loop steps down.
            return PrepareAttempt::Fail {
                error: PxPaxosError::TermStale { current_term: new_term },
            };
        }

        if promised < quorum {
            if let Some(round) = highest_rejected_round {
                let error = PxPaxosError::PrepareRejected {
                    promised: PxBallot::new(round, 0),
                };
                let next_min_round = match error.retry_action() {
                    PxRetryAction::RetrySameSlot { min_round: Some(round), .. } => round,
                    _ => round,
                };
                return PrepareAttempt::Retry { next_min_round, error };
            }
            return PrepareAttempt::Fail {
                error: PxPaxosError::QuorumUnavailable { phase: PxPaxosPhase::Prepare },
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
        PrepareAttempt::Proceed { entry, foreign_value }
    }

    async fn run_accept_phase(&self, replica: &PxLocalReplica, entry: &PxLogEntry, quorum: usize) -> AcceptAttempt {
        let mut accepted = 0usize;
        let mut highest_rejected_round: Option<u64> = None;
        let mut highest_seen_term: Option<u64> = None;
        let group_id = self.group_id;
        debug!(
            group_id,
            slot = entry.slot,
            round = entry.ballot.round,
            peer_count = self.valid_replica_count,
            quorum,
            "run accept phase"
        );

        match <PxLocalReplica as ReplicaHandler>::on_accept(replica, entry.clone(), group_id).await {
            Ok(PxAcceptReply::Accepted { .. }) => {
                accepted += 1;
            }
            Ok(PxAcceptReply::Rejected { current_promised, .. }) => {
                highest_rejected_round = Some(current_promised.round);
            }
            Ok(PxAcceptReply::TermStale { new_term, .. }) => {
                highest_seen_term = Some(highest_seen_term.map_or(new_term, |t| t.max(new_term)));
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
                    Ok(PxAcceptReply::Rejected { current_promised, .. }) => {
                        let candidate = current_promised.round;
                        highest_rejected_round = Some(highest_rejected_round.map_or(candidate, |r| r.max(candidate)));
                    }
                    Ok(PxAcceptReply::TermStale { new_term, .. }) => {
                        highest_seen_term = Some(highest_seen_term.map_or(new_term, |t| t.max(new_term)));
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

        if let Some(new_term) = highest_seen_term {
            return AcceptAttempt::Fail {
                error: PxPaxosError::TermStale { current_term: new_term },
            };
        }

        if let Some(round) = highest_rejected_round {
            let error = PxPaxosError::AcceptRejected {
                promised: PxBallot::new(round, 0),
            };
            let next_min_round = match error.retry_action() {
                PxRetryAction::RetrySameSlot { min_round: Some(round), .. } => round,
                _ => round + 1,
            };
            return AcceptAttempt::Retry { next_min_round, error };
        }
        AcceptAttempt::Fail {
            error: PxPaxosError::QuorumUnavailable { phase: PxPaxosPhase::Accept },
        }
    }

    fn recompute_quorum(&mut self) {
        let voting_count = self.remote_replicas.iter().filter(|r| r.voting()).count() + u32::from(self.local_replica.voting()) as usize;
        self.cached_quorum = (voting_count / 2) + 1;
    }

    fn retry_backoff(attempt: usize) -> Duration {
        let factor = 1u64 << attempt.min(6);
        Duration::from_millis(PaxosConfig::DEFAULT.retry_base_backoff_ms.saturating_mul(factor))
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
    Proceed { entry: PxLogEntry, foreign_value: bool },
    Retry { next_min_round: u64, error: PxPaxosError },
    Fail { error: PxPaxosError },
}

enum AcceptAttempt {
    Chosen,
    Retry { next_min_round: u64, error: PxPaxosError },
    Fail { error: PxPaxosError },
}
