#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::too_many_lines)]
#![allow(clippy::missing_fields_in_debug)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::Mutex as AsyncMutex;
use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::cluster::group_election::PendingLeaderHandoff;
use crate::cluster::local_replica::PxLocalReplica;
use crate::cluster::remote_replica::PxRemoteReplica;
use crate::cluster::replica::{Replica, ReplicaClient, ReplicaHandler};
use crate::cluster::status::{GroupStatus, StatusLevel};
use crate::common::config::{PaxosConfig, PxElectionConfig};
use crate::common::report::OperationReport;
use crate::paxos::error::{PxPaxosError, PxPaxosPhase, PxRetryAction};
use crate::paxos::roles::{PxAcceptReply, PxBallot, PxLogEntry, PxLogEntryKind, PxPrepareReply, SlotIndex};
use crate::paxos::{PxGroupId, PxNodeId};

pub struct PxGroup {
    pub group_id: PxGroupId,
    cached_quorum: usize,
    local_replica: PxLocalReplica,
    pub(crate) remote_replicas: Vec<RemoteReplicaKind>,
    pub(crate) valid_replica_count: usize,
    pub(crate) next_slot: AtomicU64,
    /// When true, always run Phase-1 Prepare before Accept (classic Paxos).
    force_classic: bool,
    /// Leader-election / heartbeat / lease tunables for this group's
    /// [`crate::cluster::group_election::spawn`] driver task.
    pub(crate) election_cfg: PxElectionConfig,
    /// Per-leadership-tenure [`CancellationToken`]. Cancelled in
    /// [`Self::shutdown`] and by every step-down trigger. The bulk-Phase-1
    /// sweep and the election driver both honor it.
    pub(crate) tenure_cancel: CancellationToken,
    /// `JoinHandle` of the spawned election driver (`None` while the driver
    /// has not been started or is disabled). Wrapped in an async mutex so
    /// `shutdown` can `await` it cooperatively without blocking other
    /// readers of `self`.
    pub(crate) driver_handle: AsyncMutex<Option<JoinHandle<()>>>,
    /// Handoff from a freshly elected candidate to the upcoming
    /// `run_leader_state` invocation. Holds `(term, peer_floor,
    /// peer_ceiling)` for bulk Phase 1. Consumed once on Leader-state
    /// entry.
    pub(crate) pending_leader_handoff: parking_lot::Mutex<Option<PendingLeaderHandoff>>,
    /// Term stamped on becoming leader. The propose leadership gate
    /// accepts a proposal only when the local replica's `role == Leader`
    /// **and** its `current_term == proposing_term`. Mismatch on either
    /// field means the leader tenure ended (the driver stepped down or
    /// moved to a new term) and the proposal must fail fast with
    /// `NotLeader` instead of racing into Paxos with stale identity.
    ///
    /// Default `0` matches the default `current_term` of a freshly
    /// constructed [`PxLocalReplica`], so testkit pinned-leader groups
    /// pass the gate without explicit stamping.
    pub(crate) proposing_term: AtomicU64,
    /// Last-known `contiguous_applied` per voting peer, refreshed from
    /// heartbeat replies. Peers never heard from are absent (treated as
    /// `0`), which keeps [`Self::group_safe_slot`] conservative until every
    /// member has reported. Only meaningful while this replica is leader.
    pub(crate) peer_applied: parking_lot::Mutex<HashMap<PxNodeId, SlotIndex>>,
    /// Group safe-slot: `min(contiguous_applied)` across the local replica
    /// and all voting peers. Every slot `<= group_safe_slot` is applied on a
    /// majority-and-then-some — specifically on *every* member that has
    /// reported — so a bounded-stale read served at this slot reflects state
    /// no follower can contradict. Recomputed at the end of each quorum
    /// heartbeat round. `0` means "not yet established".
    pub(crate) group_safe_slot: AtomicU64,
    /// Proposer sliding-window admission gate. Holds `PaxosConfig::proposer_window`
    /// permits; each in-flight `propose` call holds one for its duration, so at
    /// most `window` proposals are allocated-but-not-chosen at once. A proposal
    /// that cannot immediately acquire a permit returns `ProposeResult::Busy`
    /// (retryable) rather than blocking the caller.
    pub(crate) proposer_window: tokio::sync::Semaphore,
}

impl std::fmt::Debug for PxGroup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PxGroup")
            .field("group_id", &self.group_id)
            .field("cached_quorum", &self.cached_quorum)
            .field("leader_id", &self.leader_id())
            .field("local_replica_id", &self.local_replica.id)
            .field("valid_replica_count", &self.valid_replica_count)
            .field("remote_replicas_len", &self.remote_replicas.len())
            .finish_non_exhaustive()
    }
}

impl PxGroup {
    pub fn new(group_id: PxGroupId, local_replica: PxLocalReplica) -> Self {
        let mut group = Self {
            group_id,
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
            peer_applied: parking_lot::Mutex::new(HashMap::new()),
            group_safe_slot: AtomicU64::new(0),
            proposer_window: tokio::sync::Semaphore::new(PaxosConfig::DEFAULT.proposer_window),
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

    /// Snapshot of the believed leader id for this group. Delegates to the
    /// local replica's election state, which is the single source of truth
    /// (updated by `become_leader` / `become_follower` / `on_heartbeat` /
    /// `on_request_vote`). Returns `0` (the "unknown leader" sentinel) when
    /// the local replica has not yet learned of any leader.
    #[must_use]
    pub fn leader_id(&self) -> PxNodeId {
        self.local_replica.believed_leader_id().unwrap_or(0)
    }

    pub fn quorum(&self) -> usize {
        self.cached_quorum
    }

    /// Group safe-slot snapshot: the highest slot known to be applied on the
    /// local replica **and** every voting peer that has reported. Bounded /
    /// safe-slot reads use this as their freshness floor. `0` until the first
    /// quorum heartbeat round establishes it.
    #[must_use]
    pub fn group_safe_slot(&self) -> SlotIndex {
        self.group_safe_slot.load(Ordering::Acquire)
    }

    /// Record a voting peer's reported `contiguous_applied` and recompute the
    /// group safe-slot as the min over the local replica plus every voting
    /// peer's last-known applied. A peer that has never reported is treated as
    /// `0`, so the safe-slot only rises once *all* voting members are heard
    /// from — the conservative choice that preserves the bounded-stale read
    /// guarantee. Called from the leader heartbeat round.
    pub(crate) fn note_peer_applied(&self, peer_id: PxNodeId, applied: SlotIndex) {
        let mut peers = self.peer_applied.lock();
        peers.insert(peer_id, applied);
        let mut safe = self.local_replica.contiguous_applied();
        for remote in &self.remote_replicas {
            if let RemoteReplicaKind::Real(r) = remote {
                if r.voting {
                    let peer_applied = peers.get(&r.node_id).copied().unwrap_or(0);
                    safe = safe.min(peer_applied);
                }
            }
        }
        drop(peers);
        // Monotonic within a tenure: a transient peer regression cannot pull
        // the published safe-slot backwards (it only ever advances).
        self.group_safe_slot.fetch_max(safe, Ordering::AcqRel);
    }

    /// Clear all peer-applied tracking and reset the published group safe-slot
    /// to `0`. Called at the start of every leader tenure: `group_safe_slot`
    /// only ever advances (via `fetch_max`) *within* a tenure, so without this
    /// reset a freshly elected leader would inherit the previous tenure's
    /// elevated safe-slot and stale per-peer watermarks — overstating freshness
    /// for bounded-stale reads until new heartbeats arrive. After the reset the
    /// safe-slot stays at `0` until every voting member has reported again,
    /// which is the conservative guarantee `group_safe_slot`'s docs promise.
    pub(crate) fn reset_safe_slot_tracking(&self) {
        self.peer_applied.lock().clear();
        self.group_safe_slot.store(0, Ordering::Release);
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
        if self.local_replica.is_leader() {
            // Local is leader, no need to forward
            return None;
        }
        // Local is not leader; look up the believed leader's remote endpoint.
        let leader_id = self.local_replica.believed_leader_id()?;
        let idx = leader_id as usize;
        self.remote_replicas
            .get(idx)
            .and_then(|r| r.endpoint())
            .map(str::to_string)
    }

    // ── Setters ───────────────────────────────────────────────────

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

    /// Replace the endpoint of an existing real remote replica. Returns the
    /// previous endpoint string when a real replica was updated, or `None`
    /// when `node_id` is out of range / refers to a placeholder.
    pub fn update_member_endpoint(
        &mut self,
        node_id: PxNodeId,
        endpoint: impl Into<String>,
    ) -> Option<String> {
        let endpoint = endpoint.into();
        let idx = node_id as usize;

        if let Some(RemoteReplicaKind::Real(remote)) = self.remote_replicas.get_mut(idx) {
            let old_endpoint = remote.endpoint.clone();
            endpoint.clone_into(&mut remote.endpoint);
            Some(old_endpoint)
        } else {
            None
        }
    }

    // ── Status ────────────────────────────────────────────────────

    /// Point-in-time status of this group: local replica + each remote.
    /// Used by `/topology`.
    #[must_use]
    pub fn status(&self) -> GroupStatus {
        let local_replica = self.local_replica.status();
        let remotes: Vec<_> = self
            .remote_replicas
            .iter()
            .filter_map(|r| match r {
                RemoteReplicaKind::Real(r) => Some(r.status()),
                RemoteReplicaKind::Placeholder => None,
            })
            .collect();

        let local_status = local_replica.status;
        let mut status = local_status;
        let mut messages: Vec<_> = local_replica
            .messages
            .iter()
            .map(|msg| format!("local#{}: {msg}", self.local_replica.id))
            .collect();
        let mut ok_voting = u32::from(self.local_replica.voting() && local_status != StatusLevel::Unhealthy);

        for remote in &remotes {
            let remote_status = remote.status;
            if remote.voting && remote_status == StatusLevel::Ok {
                ok_voting += 1;
            }
            status = StatusLevel::worst(status, remote_status);
            messages.extend(
                remote
                    .messages
                    .iter()
                    .map(|msg| format!("remote#{}: {msg}", remote.id)),
            );
        }

        let quorum = self.cached_quorum as u32;
        if quorum > 0 && ok_voting < quorum {
            if status != StatusLevel::Unhealthy {
                status = StatusLevel::Degraded;
            }
            messages.push(format!(
                "group {}: only {ok_voting}/{} voting replicas reachable (quorum {quorum})",
                self.group_id,
                self.valid_replica_count + 1,
            ));
        }

        GroupStatus {
            group_id: self.group_id,
            leader_id: self.leader_id(),
            local_replica_id: local_replica.id,
            force_classic: self.force_classic,
            status,
            messages,
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
    pub async fn shutdown(&self, per_layer_timeout: Duration) -> OperationReport {
        let mut report = OperationReport::new();
        info!(
            group_id = self.group_id,
            replica_l_id = self.local_replica.id,
            remote_count = self.valid_replica_count,
            "PxGroup shutdown starting"
        );

        // 0. Cancel the per-tenure token and await the election driver.
        //    Driver is cooperative; a 100 ms scaffold tick is
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

        info!(
            group_id = self.group_id,
            error_count = report.errors.len(),
            "PxGroup shutdown complete"
        );
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
        if idx < self.remote_replicas.len() && matches!(self.remote_replicas[idx], RemoteReplicaKind::Real(_))
        {
            info!(
                group_id = self.group_id,
                remote_id = node_id,
                "removed remote replica from group"
            );
            self.remote_replicas[idx] = RemoteReplicaKind::Placeholder;
            self.valid_replica_count -= 1;
            self.recompute_quorum();
            return true;
        }
        false
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
    pub async fn propose(&self, payload: Vec<u8>, client_id: Option<u64>, seq: Option<u64>) -> ProposeResult {
        let replica = &self.local_replica;
        // Convert the client `Vec<u8>` to `Bytes` once at the entry
        // point. `Bytes::from(Vec<u8>)` reuses the existing allocation
        // (no copy) and gives us cheap `Clone` (ref-count bump) for the
        // slot-retry loop and the per-peer Accept fanout.
        let payload: bytes::Bytes = bytes::Bytes::from(payload);

        // Leadership gate. Checks BOTH:
        //   * role == Leader  -- captures the role atomic flipped by
        //     become_leader / become_follower.
        //   * current_term == proposing_term -- captures the case where
        //     the local replica advanced into a new term (became
        //     follower under HigherTerm, then re-elected) without the
        //     proposing tenure having stamped the new term yet.
        // Either miss surfaces as `NotLeader { hint: leader_endpoint() }`
        // before slot allocation, draining in-flight client proposals.
        let role_is_leader = replica.role() == crate::cluster::local_replica::PxLocalReplicaRole::Leader;
        let current_term = replica.current_term_snapshot();
        let proposing_term = self.proposing_term.load(Ordering::Acquire);
        // Pinned-leader testkit groups construct the local replica with
        // role == Leader and never advance the term, so they pass the
        // gate with current_term == 0 == proposing_term. Production
        // leaders pass once `stamp_proposing_term` has run on tenure entry.
        let gate_pass = role_is_leader && current_term == proposing_term;
        if !gate_pass {
            return ProposeResult::NotLeader {
                leader_hint: self.leader_endpoint().unwrap_or_default(),
            };
        }

        // Idempotency: a retried `(client_id, seq)` that the learner has
        // already applied returns its prior commit slot without re-running
        // Paxos (exactly-once writes, `requirement.md` §10.2). Checked before
        // window admission so duplicates never consume a window permit.
        if let (Some(cid), Some(s)) = (client_id, seq) {
            if let Some(cached_slot) = replica.learner.dedup_lookup(cid, s) {
                debug!(
                    group_id = self.group_id,
                    client_id = cid,
                    seq = s,
                    slot = cached_slot,
                    "dedup hit; returning cached commit without re-proposing"
                );
                return ProposeResult::Chosen { slot: cached_slot };
            }
        }

        // Sliding-window admission: cap concurrent in-flight proposals. The
        // permit is held for the whole proposal (released on drop at every
        // return path below), so a full window fails fast with `Busy` instead
        // of queuing unboundedly.
        let Ok(_window_permit) = self.proposer_window.try_acquire() else {
            warn!(
                group_id = self.group_id,
                window = PaxosConfig::DEFAULT.proposer_window,
                "proposer window full; rejecting proposal as Busy"
            );
            return ProposeResult::Busy;
        };

        let group_id = self.group_id;
        let total = self.valid_replica_count + 1;
        let quorum = total / 2 + 1;
        let mut slot = self.next_slot.fetch_add(1, Ordering::Relaxed);
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
                        .run_prepare_phase(replica, slot, payload.clone(), client_id, seq, quorum, min_round)
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
                            sleep(Self::retry_backoff(attempt)).await;
                            continue;
                        }
                        PrepareAttempt::Fail { error } => {
                            error!(group_id, slot, attempt, error = error.keyword(), "prepare failed");
                            if let PxPaxosError::TermStale { current_term } = &error {
                                warn!(
                                    group_id,
                                    slot, current_term, "stepping down: peer term observed during prepare"
                                );
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
                        // Tell peers the slot is chosen so their
                        // `last_chosen_slot` watermark advances before
                        // the next heartbeat tick.
                        self.fan_out_chosen_notice(&entry, group_id);
                        info!(
                            group_id,
                            slot = entry.slot,
                            round = entry.ballot.round,
                            leader_id = entry.ballot.leader_id,
                            "paxos entry chosen and learned locally"
                        );

                        if adopted_foreign_value || entry.payload != payload {
                            last_error = PxPaxosError::ForeignValueChosen { slot }.keyword().to_string();
                            warn!(
                                group_id,
                                slot,
                                error = last_error,
                                "foreign value chosen; retrying client value on next slot"
                            );
                            slot = self.next_slot.fetch_add(1, Ordering::Relaxed);
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
                        sleep(Self::retry_backoff(attempt)).await;
                    }
                    AcceptAttempt::Fail { error } => {
                        error!(group_id, slot, attempt, error = error.keyword(), "accept failed");
                        if let PxPaxosError::TermStale { current_term } = &error {
                            warn!(
                                group_id,
                                slot, current_term, "stepping down: peer term observed during accept"
                            );
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

            warn!(
                group_id,
                slot, last_error, "slot proposal failed; retrying on next slot"
            );
            slot = self.next_slot.fetch_add(1, Ordering::Relaxed);
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

    /// One background-repair step: find the lowest gap in the open prefix
    /// (the first unchosen slot below the highest slot this leader has seen
    /// chosen) and drive classic Paxos to close it.
    ///
    /// Classic Paxos here is self-healing: the Prepare phase adopts any value
    /// already accepted at the gap slot (recovering a half-committed write),
    /// and otherwise fills the slot with an empty `NoOp` so the contiguous
    /// frontier — and thus the group safe-slot — can advance past an abandoned
    /// slot. Distinct from the one-shot bulk Phase 1 run on leader entry: this
    /// runs repeatedly during steady-state leadership. A no-gap leader returns
    /// [`RepairOutcome::NoGap`] without any RPCs, so it is cheap to poll.
    pub(crate) async fn repair_once(&self) -> RepairOutcome {
        let replica = &self.local_replica;
        if replica.role() != crate::cluster::local_replica::PxLocalReplicaRole::Leader {
            return RepairOutcome::NotLeader;
        }
        let contiguous = replica.contiguous_chosen();
        let highest = replica.last_chosen_slot();
        if contiguous >= highest {
            return RepairOutcome::NoGap;
        }
        // The first slot above the contiguous frontier is, by definition, not
        // yet chosen locally — the lowest hole to fill.
        let gap_slot = contiguous + 1;
        let quorum = self.quorum();
        let group_id = self.group_id;
        debug!(
            group_id,
            gap_slot, contiguous, highest, "background repair: filling gap"
        );

        // Always run Phase 1 (classic) so an already-accepted value is
        // adopted rather than overwritten.
        let entry = match self
            .run_prepare_phase(replica, gap_slot, bytes::Bytes::new(), None, None, quorum, 0)
            .await
        {
            PrepareAttempt::Proceed { entry, .. } => entry,
            PrepareAttempt::Retry { error, .. } | PrepareAttempt::Fail { error } => {
                debug!(
                    group_id,
                    gap_slot,
                    error = error.keyword(),
                    "repair prepare did not proceed"
                );
                return RepairOutcome::Failed;
            }
        };

        match self.run_accept_phase(replica, &entry, quorum).await {
            AcceptAttempt::Chosen => {
                replica.learn(&entry);
                self.fan_out_chosen_notice(&entry, group_id);
                info!(group_id, slot = gap_slot, "background repair filled gap");
                RepairOutcome::Filled { slot: gap_slot }
            }
            AcceptAttempt::Retry { error, .. } | AcceptAttempt::Fail { error } => {
                debug!(
                    group_id,
                    gap_slot,
                    error = error.keyword(),
                    "repair accept did not choose"
                );
                RepairOutcome::Failed
            }
        }
    }

    fn base_entry(
        &self,
        slot: u64,
        payload: bytes::Bytes,
        client_id: Option<u64>,
        seq: Option<u64>,
    ) -> PxLogEntry {
        PxLogEntry {
            slot,
            ballot: PxBallot::new(0, self.local_replica.id),
            term: self.local_replica.current_term_snapshot(),
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

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn run_prepare_phase(
        &self,
        replica: &PxLocalReplica,
        slot: u64,
        payload: bytes::Bytes,
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
        let mut entry = self.base_entry(slot, payload.clone(), client_id, seq);
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
                        highest_rejected_round =
                            Some(highest_rejected_round.map_or(candidate, |r| r.max(candidate)));
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
                error: PxPaxosError::TermStale {
                    current_term: new_term,
                },
            };
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
            foreign_value = prev.payload != payload;
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

    pub(crate) async fn run_accept_phase(
        &self,
        replica: &PxLocalReplica,
        entry: &PxLogEntry,
        quorum: usize,
    ) -> AcceptAttempt {
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
                        highest_rejected_round =
                            Some(highest_rejected_round.map_or(candidate, |r| r.max(candidate)));
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
                error: PxPaxosError::TermStale {
                    current_term: new_term,
                },
            };
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

    /// Best-effort fan-out of a `ChosenNotification` to every real
    /// remote in this group after a slot has been chosen. The notice is
    /// fire-and-forget over the per-peer bidi `PxPeerStream`; failures
    /// are logged at `debug!` and never propagated, since the next
    /// heartbeat (carrying `committed_safe_slot`) will re-converge
    /// peer frontiers regardless.
    ///
    /// `leader_id` is taken from `entry.ballot.leader_id`, matching the
    /// proposer that chose the value. Sequential await rather than
    /// `JoinSet` fan-out is fine for now: each `send_chosen_notice` is
    /// just an mpsc enqueue (capacity = `peer_stream_window_frames`)
    /// once the per-peer bg task is running, so it returns near-
    /// instantly except when a peer is down (in which case it fast-
    /// fails via the connect-retry drain in `peer_stream.rs`).
    pub(crate) fn fan_out_chosen_notice(&self, entry: &PxLogEntry, group_id: u64) {
        let slot = entry.slot;
        let term = entry.term;
        let leader_id = entry.ballot.leader_id;
        for remote in &self.remote_replicas {
            let RemoteReplicaKind::Real(remote) = remote else {
                continue;
            };
            let remote_id = remote.node_id;
            if let Err(err) = remote.send_chosen_notice(slot, term, leader_id, group_id) {
                debug!(group_id, slot, term, remote_id, endpoint = %remote.endpoint, error = %err, "fan_out_chosen_notice: peer notice failed (best-effort)");
            }
        }
    }

    fn recompute_quorum(&mut self) {
        let voting_count = self.remote_replicas.iter().filter(|r| r.voting()).count()
            + u32::from(self.local_replica.voting()) as usize;
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

/// Test-only hooks (compiled under the `test-util` feature). These expose
/// crate-internal mechanisms — the proposer admission semaphore, a single
/// repair step, and peer-applied injection — to integration tests under
/// `tests/` without permanently widening the production public API.
#[cfg(feature = "test-util")]
impl PxGroup {
    /// Borrow the proposer sliding-window admission semaphore so a test can
    /// exhaust its permits and observe `ProposeResult::Busy`.
    #[must_use]
    pub fn proposer_window(&self) -> &tokio::sync::Semaphore {
        &self.proposer_window
    }

    /// Run one background-repair step, returning the slot that was filled
    /// (`Some`) or `None` when there was no gap to repair / repair did not
    /// choose. Wraps the internal [`Self::repair_once`].
    pub async fn repair_once_for_tests(&self) -> Option<u64> {
        match self.repair_once().await {
            RepairOutcome::Filled { slot } => Some(slot),
            RepairOutcome::NoGap | RepairOutcome::NotLeader | RepairOutcome::Failed => None,
        }
    }

    /// Inject a peer's reported `contiguous_applied` watermark, normally driven
    /// by the leader heartbeat round, so a test can exercise group-safe-slot
    /// computation deterministically. Wraps the internal [`Self::note_peer_applied`].
    pub fn note_peer_applied_for_tests(&self, peer_id: PxNodeId, applied: SlotIndex) {
        self.note_peer_applied(peer_id, applied);
    }
}

/// Result of a `PxGroup::propose` call.
#[derive(Debug)]
pub enum ProposeResult {
    Chosen {
        slot: u64,
    },
    NotLeader {
        leader_hint: String,
    },
    /// The proposer sliding window is full; the caller should retry shortly.
    /// Distinct from `Err` so the KV layer can surface a retryable signal.
    Busy,
    Err(String),
}

/// Result of one [`PxGroup::repair_once`] background-repair step.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum RepairOutcome {
    /// A gap was found and chosen (recovered value or `NoOp` fill) at `slot`.
    Filled { slot: u64 },
    /// The contiguous frontier already reaches the highest seen slot; nothing
    /// to repair (no RPCs issued).
    NoGap,
    /// This replica is not the leader; repair is a leader-only duty.
    NotLeader,
    /// The gap slot could not be chosen this round (quorum/transport); a later
    /// poll retries.
    Failed,
}

pub(crate) enum PrepareAttempt {
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

pub(crate) enum AcceptAttempt {
    Chosen,
    Retry {
        next_min_round: u64,
        error: PxPaxosError,
    },
    Fail {
        error: PxPaxosError,
    },
}
