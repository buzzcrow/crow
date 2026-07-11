//! Leader election driver task.
//!
//! Per-group async loop that observes the local replica's role and drives
//! the state machine forward.
//!
//! Key work: scaffold lifecycle (spawn / cancel / weak-drop exit),
//! follower election timer, `PreVote` / `RequestVote` fanout,
//! `become_leader` handoff into bulk Phase 1, leader heartbeat ticker +
//! lease renewal, step-down sequence (higher term / lease-unrenewable /
//! admin), admin `StepDown` signal routing, proposer leadership gate.

use std::sync::{Arc, Weak};
use std::time::Duration;

use std::time::Instant as StdInstant;

use tokio::task::{JoinHandle, JoinSet};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::cluster::group::{PendingLeaderHandoff, PxGroup};
use crate::cluster::local_replica::PxLocalReplicaRole;
use crate::cluster::replica::{HeartbeatRequestPayload, ReplicaClient, VoteRequestPayload};
use crate::common::config::PxElectionConfig;
use crate::paxos::PxNodeId;

/// Spawn the per-group election driver task.
///
/// Returns the spawned `JoinHandle`; the caller (currently
/// [`PxGroup::start_election_loop`]) stores it on the group so
/// [`PxGroup::shutdown`] can `cancel` and `await` it deterministically.
///
/// `group` is held weakly inside the task so a forgotten/dropped group does
/// not leak the driver — the task exits the first time `upgrade()` fails.
#[must_use]
pub fn spawn(group: Weak<PxGroup>, cfg: PxElectionConfig, cancel: CancellationToken) -> JoinHandle<()> {
    tokio::spawn(run(group, cfg, cancel))
}

/// Tiny xorshift64* PRNG used to randomize the per-replica election
/// deadline. Seeded from `(group_id, replica_id, now_nanos)` so concurrent
/// tests with paused tokio time still observe distinct sequences.
///
/// Avoids pulling in `rand` as a runtime dependency for a one-line need.
#[derive(Debug)]
struct XorShift64 {
    state: u64,
}

impl XorShift64 {
    fn new(seed: u64) -> Self {
        // 0 is a fixed point for xorshift; substitute a non-zero constant.
        Self {
            state: if seed == 0 { 0x9E37_79B9_7F4A_7C15 } else { seed },
        }
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    /// Uniform-ish in `[lo, hi]` (inclusive). Caller guarantees `lo <= hi`.
    fn random_between_ms(&mut self, lo: u64, hi: u64) -> u64 {
        if lo == hi {
            return lo;
        }
        let span = hi - lo + 1;
        lo + (self.next_u64() % span)
    }
}

/// Schedule the next election deadline based on `[election_min, election_max]`.
fn next_election_deadline(now: Instant, cfg: &PxElectionConfig, rng: &mut XorShift64) -> Instant {
    let jitter_ms = rng.random_between_ms(cfg.election_min_ms, cfg.election_max_ms);
    now + Duration::from_millis(jitter_ms)
}

#[tracing::instrument(level = "info", name = "election_driver", skip_all)]
async fn run(group: Weak<PxGroup>, cfg: PxElectionConfig, cancel: CancellationToken) {
    let (store_group_id, replica_l_id) = if let Some(g) = group.upgrade() {
        (g.group_id, g.local_replica().id)
    } else {
        debug!("election driver started after group was dropped; exiting");
        return;
    };
    info!(
        group_id = store_group_id,
        replica_l_id,
        election_min_ms = cfg.election_min_ms,
        election_max_ms = cfg.election_max_ms,
        heartbeat_interval_ms = cfg.heartbeat_interval_ms,
        lease_duration_ms = cfg.lease_duration_ms,
        "election driver started"
    );

    // Seed mixes group / replica identity with the wall clock so paused-time
    // tests covering multiple replicas in the same runtime still get
    // distinct deadline sequences.
    // Truncating the high bits of the wall-clock nanos is fine: the seed is
    // mixed with replica identity below and only feeds an xorshift PRNG.
    #[allow(clippy::cast_possible_truncation)]
    let seed_nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0u64, |d| d.as_nanos() as u64);
    let mut rng = XorShift64::new(seed_nanos.rotate_left(13) ^ store_group_id.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ replica_l_id.wrapping_mul(0xBF58_476D_1CE4_E5B9));

    let mut election_deadline = next_election_deadline(Instant::now(), &cfg, &mut rng);

    loop {
        if cancel.is_cancelled() {
            info!(group_id = store_group_id, replica_l_id, "election driver cancelled");
            return;
        }
        let Some(g) = group.upgrade() else {
            debug!(group_id = store_group_id, replica_l_id, "election driver: group dropped; exiting");
            return;
        };
        let role = g.local_replica().role();

        match role {
            PxLocalReplicaRole::Leader => {
                // 9.5: ticking heartbeats + lease renewal until step-down or
                // cancel. On step-down the replica is back to Follower with
                // a fresh deadline.
                run_leader_state(&g, &cfg, &cancel).await;
                election_deadline = next_election_deadline(Instant::now(), &cfg, &mut rng);
            }
            PxLocalReplicaRole::Follower | PxLocalReplicaRole::PreCandidate | PxLocalReplicaRole::Candidate => {
                drop(g);
                tokio::select! {
                    biased;
                    () = cancel.cancelled() => {
                        info!(group_id = store_group_id, replica_l_id, "election driver cancelled");
                        return;
                    }
                    () = tokio::time::sleep_until(election_deadline) => {
                        let Some(g) = group.upgrade() else {
                            debug!(group_id = store_group_id, replica_l_id, "election driver: group dropped; exiting");
                            return;
                        };
                        let role = g.local_replica().role();
                        let term = g.local_replica().current_term_snapshot();
                        debug!(group_id = store_group_id, replica_l_id, ?role, current_term = term, "election deadline fired");
                        match role {
                            PxLocalReplicaRole::Follower | PxLocalReplicaRole::PreCandidate | PxLocalReplicaRole::Candidate => {
                                run_election_attempt(&g, &cfg, &cancel).await;
                            }
                            PxLocalReplicaRole::Leader => { /* race: handled next iteration */ }
                        }
                        election_deadline = next_election_deadline(Instant::now(), &cfg, &mut rng);
                    }
                }
            }
        }
    }
}

/// Reason a leader is stepping down. Used for logs and (Step 11) metrics.
#[derive(Clone, Copy, Debug)]
enum StepDownReason {
    HigherTerm(u64),
    LeaseUnrenewable,
    Admin,
}

/// Leader-state inner loop: tick heartbeats + lease bookkeeping until
/// step-down (higher term, lease unrenewable, admin) or driver cancel.
///
/// Implements **Step 9.5** + **Step 9.6**: heartbeat ticker, lease
/// renewal, per-tenure cancel token for bulk Phase 1, lease-unrenewable
/// step-down trigger, and the canonical step-down execution sequence.
async fn run_leader_state(group: &Arc<PxGroup>, cfg: &PxElectionConfig, cancel: &CancellationToken) {
    let replica = group.local_replica();
    let group_id = group.group_id;
    let leader_id: PxNodeId = replica.id;
    let leader_term = replica.current_term_snapshot();

    // Reset lease state at the start of the tenure. The first heartbeat
    // round that gets quorum extends the lease and unlocks read fast-path.
    replica.reset_lease_to(StdInstant::now());

    // Per-leadership-tenure cancel token. Cancelled by the step-down
    // sequence (Step 9.6); aborts in-flight bulk Phase 1 and any future
    // tenure-bound work. Always a child of the driver-lifetime `cancel`
    // so shutdown still wins.
    let tenure_cancel = CancellationToken::new();
    {
        let parent = cancel.clone();
        let child = tenure_cancel.clone();
        tokio::spawn(async move {
            parent.cancelled().await;
            child.cancel();
        });
    }

    // Step 9.6: consume the handoff stashed by finalize_leader and spawn
    // bulk Phase 1 on the per-tenure cancel token.
    if let Some(handoff) = group.pending_leader_handoff.lock().take() {
        let group_for_task = group.clone();
        let cfg_for_task = *cfg;
        let cancel_for_task = tenure_cancel.clone();
        tokio::spawn(async move {
            group_for_task
                .run_bulk_phase1(handoff.term, handoff.peer_floor, handoff.peer_ceiling, cfg_for_task, cancel_for_task)
                .await;
        });
    }

    let mut ticker = tokio::time::interval(Duration::from_millis(cfg.heartbeat_interval_ms));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // First tick fires immediately; consume it so the loop starts clean.
    ticker.tick().await;

    info!(
        group_id,
        replica_l_id = leader_id,
        term = leader_term,
        heartbeat_interval_ms = cfg.heartbeat_interval_ms,
        lease_duration_ms = cfg.lease_duration_ms,
        "entering leader state"
    );

    loop {
        if replica.role() != PxLocalReplicaRole::Leader {
            info!(group_id, replica_l_id = leader_id, "leader state exiting: role changed externally");
            tenure_cancel.cancel();
            return;
        }
        if cancel.is_cancelled() {
            tenure_cancel.cancel();
            return;
        }
        // Step 9.6: lease-unrenewable check on every Leader tick.
        let last_quorum = replica.last_quorum_heartbeat_at();
        if StdInstant::now().duration_since(last_quorum) >= Duration::from_millis(cfg.lease_duration_ms) {
            step_down(group, &tenure_cancel, leader_term, StepDownReason::LeaseUnrenewable);
            return;
        }
        tokio::select! {
            biased;
            () = cancel.cancelled() => {
                tenure_cancel.cancel();
                return;
            }
            // Step 9.7: admin step-down via StepDown RPC.
            () = replica.admin_step_down_signal.notified() => {
                step_down(group, &tenure_cancel, leader_term, StepDownReason::Admin);
                return;
            }
            _ = ticker.tick() => {
                match run_heartbeat_round(group, cfg, leader_term).await {
                    HeartbeatOutcome::Continued => {}
                    HeartbeatOutcome::SteppedDown { peer_term } => {
                        step_down(group, &tenure_cancel, leader_term, StepDownReason::HigherTerm(peer_term));
                        return;
                    }
                }
            }
        }
    }
}

/// Canonical step-down execution sequence (Step 9.6 §1–6).
///
/// 1. Cancel the per-tenure [`CancellationToken`] — aborts in-flight bulk
///    Phase 1 and any future tenure-bound spawned work.
/// 2. Stop the heartbeat ticker (handled by returning from
///    [`run_leader_state`]).
/// 3. Persistent state: `role = Follower`. `current_term` is only raised
///    here when the trigger is `HigherTerm`; `voted_for` is preserved.
/// 4. Reset election deadline (done by the outer driver loop on return).
/// 5. Expire `LeaseState` (`become_follower` already calls
///    `LeaseState::expired()`).
/// 6. Drain in-flight proposals via the Step 9.8 leadership gate (they
///    return `NotLeader { hint: None }` because the current leader is
///    unknown).
fn step_down(group: &Arc<PxGroup>, tenure_cancel: &CancellationToken, my_term: u64, reason: StepDownReason) {
    info!(
        group_id = group.group_id,
        replica_l_id = group.local_replica().id,
        my_term,
        ?reason,
        "stepping down from leader"
    );
    // Step 11: bump per-reason step-down counters before the role flip
    // so concurrent metric snapshots observe the increment alongside
    // (or before) the role transition.
    let metrics = group.local_replica().election_metrics();
    match reason {
        StepDownReason::HigherTerm(_) => metrics.record_step_down_higher_term(),
        StepDownReason::LeaseUnrenewable => metrics.record_step_down_lease_unrenewable(),
        StepDownReason::Admin => metrics.record_step_down_admin(),
    }
    tenure_cancel.cancel();
    let target_term = match reason {
        StepDownReason::HigherTerm(t) => t.max(my_term),
        StepDownReason::LeaseUnrenewable | StepDownReason::Admin => my_term,
    };
    group.local_replica().become_follower(target_term);
    // Drop the cached leader id; after step-down the leader is
    // unknown until the next heartbeat / vote round establishes one.
    group.set_leader_id(0);
}

#[derive(Debug, PartialEq, Eq)]
enum HeartbeatOutcome {
    /// Round either gathered quorum-OK or failed without a higher term.
    Continued,
    /// A peer reply carried `peer_term > leader_term`; the leader-state
    /// loop steps down to follower in the observed term.
    SteppedDown { peer_term: u64 },
}

/// Fan out one heartbeat round to all voting peers.
///
/// On quorum-OK: extends `lease_read_until = max(prev, T_send +
/// lease_duration - max_clock_skew)` and bumps `last_quorum_heartbeat_at`.
/// On any peer reply with `term > leader_term`: calls
/// `become_follower(peer_term)` and returns [`HeartbeatOutcome::SteppedDown`].
async fn run_heartbeat_round(group: &Arc<PxGroup>, cfg: &PxElectionConfig, leader_term: u64) -> HeartbeatOutcome {
    let replica = group.local_replica();
    let group_id = group.group_id;
    let voting_peers = group.voting_remote_ids();
    let quorum = group.quorum();
    let t_send = StdInstant::now();
    // `t_send_ms_mono` is monotonic millis since the process-start anchor
    // shared with [`crate::cluster::local_replica::process_anchor`]. Peers
    // only use this for relative ordering inside a single leader's stream.
    let t_send_ms_mono = crate::cluster::local_replica::instant_to_anchor_ms(t_send);

    let payload = HeartbeatRequestPayload {
        term: leader_term,
        leader_id: replica.id,
        prev_log_slot: replica.last_chosen_slot(),
        prev_log_term: replica.last_chosen_term(),
        committed_safe_slot: replica.contiguous_chosen(),
        lease_grant_until_ms_mono: t_send_ms_mono.saturating_add(cfg.lease_duration_ms),
        t_send_ms_mono,
    };

    // Single-voter clusters trivially have quorum; renew lease without RPCs.
    let mut acks: usize = 1;
    if acks >= quorum {
        renew_lease(replica, t_send, cfg);
        return HeartbeatOutcome::Continued;
    }

    let mut joinset: JoinSet<(PxNodeId, Result<crate::cluster::replica::HeartbeatReply, crate::cluster::replica::PxReplicaError>)> = JoinSet::new();
    for peer_id in voting_peers {
        let group_for_task = group.clone();
        let req = payload;
        joinset.spawn(async move {
            let result = if let Some(remote) = group_for_task.get_remote_replica(peer_id) {
                remote.send_heartbeat(req, group_for_task.group_id).await
            } else {
                Err(crate::cluster::replica::PxReplicaError::Internal(format!("peer {peer_id} not present")))
            };
            (peer_id, result)
        });
    }

    while let Some(joined) = joinset.join_next().await {
        let Ok((peer_id, reply)) = joined else { continue };
        match reply {
            Ok(hb) => {
                if hb.term > leader_term {
                    info!(
                        group_id,
                        leader_id = replica.id,
                        my_term = leader_term,
                        peer_id,
                        peer_term = hb.term,
                        "heartbeat saw higher term"
                    );
                    joinset.abort_all();
                    return HeartbeatOutcome::SteppedDown { peer_term: hb.term };
                }
                if hb.success {
                    acks += 1;
                    if acks >= quorum {
                        renew_lease(replica, t_send, cfg);
                        // Keep draining remaining replies but no further
                        // state changes happen unless a higher term shows.
                    }
                }
            }
            Err(err) => {
                debug!(group_id, peer_id, error = ?err, "heartbeat transport error");
            }
        }
    }

    HeartbeatOutcome::Continued
}

fn renew_lease(replica: &crate::cluster::local_replica::PxLocalReplica, t_send: StdInstant, cfg: &PxElectionConfig) {
    let lease_dur = Duration::from_millis(cfg.lease_duration_ms);
    let skew = Duration::from_millis(cfg.max_clock_skew_ms);
    // Saturating sub avoids underflow when skew >= lease_duration.
    let extended_until = t_send + lease_dur.saturating_sub(skew);
    // Two atomic operations rather than a mutex round-trip; both fields
    // only ever advance, so a brief torn snapshot from a concurrent reader
    // cannot regress either value.
    replica.extend_lease_read_until(extended_until);
    replica.record_quorum_heartbeat(StdInstant::now());
}

/// Drive one full election attempt: optional `PreVote` (9.4) →
/// `Candidate` `RequestVote` (9.3) → `Leader` on win, otherwise stay
/// `Follower` / `Candidate` until the next deadline.
async fn run_election_attempt(group: &Arc<PxGroup>, cfg: &PxElectionConfig, cancel: &CancellationToken) {
    let replica = group.local_replica();

    if cfg.prevote_enabled {
        replica.become_precandidate();
        match run_prevote_round(group, cancel).await {
            PreVoteOutcome::Won { proposed_term } => {
                replica.become_candidate(proposed_term);
                run_candidate_election(group, cancel).await;
            }
            PreVoteOutcome::HigherTerm(t) => {
                replica.become_follower(t);
            }
            PreVoteOutcome::Lost => {
                // Step back to Follower in the same term; next deadline
                // will retry (with a fresh randomized jitter).
                replica.become_follower(replica.current_term_snapshot());
            }
        }
    } else {
        let new_term = replica.current_term_snapshot() + 1;
        replica.become_candidate(new_term);
        run_candidate_election(group, cancel).await;
    }
}

/// Outcome of one `PreVote` fanout.
#[derive(Debug)]
enum PreVoteOutcome {
    /// Quorum of peers (including self) granted the pre-vote for
    /// `proposed_term`; safe to bump the term and start a real election.
    Won { proposed_term: u64 },
    /// At least one peer reply carried a strictly higher term; the
    /// driver must step down rather than start an election.
    HigherTerm(u64),
    /// Quorum could not be gathered (rejections / timeouts / errors).
    Lost,
}

/// Run a single `PreVote` round.
///
/// Implements **Step 9.4**: fan out `PreVote(proposed_term)` without
/// bumping `current_term`. Outcome is determined by the same
/// `(quorum, higher-term)` rules as [`run_candidate_election`].
async fn run_prevote_round(group: &Arc<PxGroup>, cancel: &CancellationToken) -> PreVoteOutcome {
    let group_id = group.group_id;
    let replica = group.local_replica();
    let term = replica.current_term_snapshot();
    let proposed_term = term + 1;
    let candidate_id: PxNodeId = replica.id;

    let payload = VoteRequestPayload {
        term: proposed_term,
        candidate_id,
        last_chosen_slot: replica.last_chosen_slot(),
        last_chosen_term: replica.last_chosen_term(),
    };

    let voting_peers = group.voting_remote_ids();
    let quorum = group.quorum();
    let mut grants: usize = 1; // self-grant: a PreCandidate trivially votes for itself

    debug!(
        group_id,
        candidate_id,
        my_term = term,
        proposed_term,
        peer_count = voting_peers.len(),
        quorum,
        "precandidate fanning out PreVote"
    );

    if grants >= quorum {
        return PreVoteOutcome::Won { proposed_term };
    }

    let mut joinset: JoinSet<(PxNodeId, Result<crate::cluster::replica::VoteReply, crate::cluster::replica::PxReplicaError>)> = JoinSet::new();
    for peer_id in voting_peers {
        let group_for_task = group.clone();
        let req = payload;
        joinset.spawn(async move {
            let result = if let Some(remote) = group_for_task.get_remote_replica(peer_id) {
                remote.send_pre_vote(req, group_for_task.group_id).await
            } else {
                Err(crate::cluster::replica::PxReplicaError::Internal(format!("peer {peer_id} not present")))
            };
            (peer_id, result)
        });
    }

    while let Some(joined) = joinset.join_next().await {
        if cancel.is_cancelled() {
            joinset.abort_all();
            return PreVoteOutcome::Lost;
        }
        let Ok((peer_id, reply)) = joined else { continue };
        match reply {
            Ok(vote) => {
                if vote.term > proposed_term {
                    joinset.abort_all();
                    return PreVoteOutcome::HigherTerm(vote.term);
                }
                if vote.granted {
                    grants += 1;
                    if grants >= quorum {
                        joinset.abort_all();
                        return PreVoteOutcome::Won { proposed_term };
                    }
                }
            }
            Err(err) => {
                debug!(group_id, candidate_id, proposed_term, peer_id, error = ?err, "PreVote transport error");
            }
        }
    }

    info!(group_id, candidate_id, proposed_term, grants, quorum, "precandidate failed to gather pre-vote quorum");
    PreVoteOutcome::Lost
}

/// Run a single round of `Candidate`-state vote gathering.
///
/// Implements **Step 9.3**: fan out `RequestVote` in parallel via a
/// [`JoinSet`], aggregate replies, and finalize the outcome:
///
/// 1. If any peer reply carries `term > my_term` → `become_follower(term)`
///    and return.
/// 2. If `grants >= quorum` → `become_leader()`, spawn
///    [`PxGroup::run_bulk_phase1`] with the floor / ceiling derived from
///    the granting peers' frontier triples, return.
/// 3. Otherwise → stay in `Candidate` and let the next election deadline
///    restart the election in `current_term + 1`.
#[allow(clippy::too_many_lines)]
async fn run_candidate_election(group: &Arc<PxGroup>, cancel: &CancellationToken) {
    let group_id = group.group_id;
    let replica = group.local_replica();
    let term = replica.current_term_snapshot();
    let candidate_id: PxNodeId = replica.id;
    let last_chosen_slot = replica.last_chosen_slot();
    let last_chosen_term = replica.last_chosen_term();

    let payload = VoteRequestPayload {
        term,
        candidate_id,
        last_chosen_slot,
        last_chosen_term,
    };

    let voting_peers = group.voting_remote_ids();
    let quorum = group.quorum();
    // Local replica votes for itself in `become_candidate`.
    let mut grants: usize = 1;
    let mut peer_floor: u64 = replica.contiguous_chosen();
    let mut peer_ceiling: u64 = replica.highest_seen_slot();

    debug!(group_id, candidate_id, term, peer_count = voting_peers.len(), quorum, "candidate fanning out RequestVote");

    // Trivial-cluster fast path: local replica alone constitutes quorum.
    if grants >= quorum {
        finalize_leader(group, term, peer_floor, peer_ceiling);
        return;
    }

    let mut joinset: JoinSet<(PxNodeId, Result<crate::cluster::replica::VoteReply, crate::cluster::replica::PxReplicaError>)> = JoinSet::new();
    for peer_id in voting_peers {
        let group_for_task = group.clone();
        let req = payload;
        joinset.spawn(async move {
            let result = if let Some(remote) = group_for_task.get_remote_replica(peer_id) {
                remote.send_request_vote(req, group_for_task.group_id).await
            } else {
                Err(crate::cluster::replica::PxReplicaError::Internal(format!("peer {peer_id} not present")))
            };
            (peer_id, result)
        });
    }

    while let Some(joined) = joinset.join_next().await {
        if cancel.is_cancelled() {
            joinset.abort_all();
            return;
        }
        let (peer_id, reply) = match joined {
            Ok(pair) => pair,
            Err(join_err) => {
                warn!(group_id, error = %join_err, "RequestVote task panicked");
                continue;
            }
        };
        match reply {
            Ok(vote) => {
                if vote.term > term {
                    info!(
                        group_id,
                        candidate_id,
                        my_term = term,
                        peer_id,
                        peer_term = vote.term,
                        "RequestVote observed higher term; stepping down to follower"
                    );
                    replica.become_follower(vote.term);
                    joinset.abort_all();
                    return;
                }
                if vote.granted {
                    grants += 1;
                    peer_floor = peer_floor.max(vote.contiguous_chosen);
                    peer_ceiling = peer_ceiling.max(vote.highest_seen_slot);
                    debug!(group_id, candidate_id, term, peer_id, grants, quorum, "RequestVote granted");
                    if grants >= quorum {
                        joinset.abort_all();
                        finalize_leader(group, term, peer_floor, peer_ceiling);
                        return;
                    }
                } else {
                    debug!(group_id, candidate_id, term, peer_id, peer_term = vote.term, "RequestVote rejected");
                }
            }
            Err(err) => {
                debug!(
                    group_id,
                    candidate_id,
                    term,
                    peer_id,
                    error = ?err,
                    "RequestVote transport error"
                );
            }
        }
    }

    info!(
        group_id,
        candidate_id, term, grants, quorum, "candidate failed to gather quorum; will retry on next deadline"
    );
}

fn finalize_leader(group: &Arc<PxGroup>, term: u64, peer_floor: u64, peer_ceiling: u64) {
    let replica = group.local_replica();
    info!(
        group_id = group.group_id,
        replica_l_id = replica.id,
        term,
        peer_floor,
        peer_ceiling,
        "candidate won quorum; becoming leader"
    );
    // Hand bulk-Phase-1 inputs off to run_leader_state, which spawns the
    // sweep on the per-tenure cancel token (Step 9.6).
    *group.pending_leader_handoff.lock() = Some(PendingLeaderHandoff { term, peer_floor, peer_ceiling });
    replica.become_leader();
    // Step 9.8: stamp the proposing term so the propose leadership gate
    // accepts client proposals in this tenure.
    group.stamp_proposing_term(term);
    // Mirror the elected identity onto the group so observers (health,
    // topology, leader_endpoint forwarding) see this replica as leader
    // immediately, not at next external set_leader_id call.
    group.set_leader_id(replica.id);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::local_replica::{PxLocalReplica, PxLocalReplicaRole};
    use std::sync::Arc;

    fn test_cfg() -> PxElectionConfig {
        PxElectionConfig::for_tests()
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn election_driver_scaffold_starts_and_stops() {
        let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);
        let group = Arc::new(PxGroup::new(42, replica));
        let cancel = group.tenure_cancel();

        let weak = Arc::downgrade(&group);
        let handle = super::spawn(weak, test_cfg(), cancel.clone());

        // Advance past one election deadline so the driver records a tick.
        tokio::time::advance(Duration::from_millis(test_cfg().election_max_ms + 5)).await;
        assert!(!handle.is_finished(), "driver should still be running before cancel");

        cancel.cancel();
        let join = tokio::time::timeout(Duration::from_secs(1), handle).await;
        assert!(join.is_ok(), "driver did not exit within 1s of cancel");
        assert!(join.unwrap().is_ok(), "driver task panicked");
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn election_driver_exits_when_group_dropped() {
        let replica = PxLocalReplica::new(2, PxLocalReplicaRole::Follower);
        let group = Arc::new(PxGroup::new(7, replica));
        let weak = Arc::downgrade(&group);
        let cancel = group.tenure_cancel();
        let handle = super::spawn(weak, test_cfg(), cancel);

        drop(group);
        tokio::time::advance(Duration::from_millis(test_cfg().election_max_ms + 5)).await;

        let join = tokio::time::timeout(Duration::from_secs(1), handle).await;
        assert!(join.is_ok(), "driver did not notice dropped group within 1s");
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn single_voter_candidate_becomes_leader() {
        // Trivial-cluster fast path: local replica is the only voter, so
        // quorum = 1 and the candidate wins its election immediately.
        let replica = PxLocalReplica::new(5, PxLocalReplicaRole::Follower);
        let group = Arc::new(PxGroup::new(13, replica));
        let cancel = group.tenure_cancel();

        let handle = super::spawn(Arc::downgrade(&group), test_cfg(), cancel.clone());

        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(test_cfg().election_max_ms + 10)).await;
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }

        assert_eq!(group.local_replica().role(), PxLocalReplicaRole::Leader, "expected Leader after self-quorum election");
        assert_eq!(group.local_replica().current_term_snapshot(), 1, "term should be bumped to 1 by become_candidate");

        cancel.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(1), handle).await;
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn single_voter_with_prevote_enabled_becomes_leader() {
        // PreVote enabled in `for_tests`. Single voter wins the PreVote
        // trivially (self-grant), bumps term, wins RequestVote, and
        // becomes leader. Verifies the 9.4 path does not regress the
        // 9.3 outcome.
        let cfg = PxElectionConfig {
            prevote_enabled: true,
            ..PxElectionConfig::for_tests()
        };
        let replica = PxLocalReplica::new(9, PxLocalReplicaRole::Follower);
        let group = Arc::new(PxGroup::new(21, replica));
        let cancel = group.tenure_cancel();

        let handle = super::spawn(Arc::downgrade(&group), cfg, cancel.clone());
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(cfg.election_max_ms + 10)).await;
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }

        assert_eq!(
            group.local_replica().role(),
            PxLocalReplicaRole::Leader,
            "PreVote path should still reach Leader for a single-voter group"
        );
        assert_eq!(group.local_replica().current_term_snapshot(), 1);

        cancel.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(1), handle).await;
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn leader_heartbeat_tick_renews_lease() {
        // Single-voter cluster: become_leader, then the heartbeat ticker
        // bootstraps + extends the lease without sending RPCs.
        let cfg = PxElectionConfig::for_tests();
        let replica = PxLocalReplica::new(31, PxLocalReplicaRole::Follower);
        let group = Arc::new(PxGroup::new(99, replica));
        let cancel = group.tenure_cancel();

        let handle = super::spawn(Arc::downgrade(&group), cfg, cancel.clone());

        // First election deadline -> Leader.
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(cfg.election_max_ms + 10)).await;
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        assert_eq!(group.local_replica().role(), PxLocalReplicaRole::Leader);

        // Run several heartbeat ticks; lease_read_until should advance
        // past the moment of becoming leader.
        let lease_before = group.local_replica().lease_state_snapshot().lease_read_until;
        tokio::time::advance(Duration::from_millis(cfg.heartbeat_interval_ms * 4 + 5)).await;
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        let lease_after = group.local_replica().lease_state_snapshot().lease_read_until;
        assert!(
            lease_after > lease_before,
            "lease_read_until should be extended by heartbeat ticks; before={lease_before:?} after={lease_after:?}"
        );

        cancel.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(1), handle).await;
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn admin_step_down_drops_leader_to_follower() {
        use crate::cluster::replica::StepDownRequestPayload;

        let cfg = PxElectionConfig::for_tests();
        let replica = PxLocalReplica::new(17, PxLocalReplicaRole::Follower);
        let group = Arc::new(PxGroup::new(64, replica));
        let cancel = group.tenure_cancel();
        let handle = super::spawn(Arc::downgrade(&group), cfg, cancel.clone());

        // Single-voter election: Follower -> Leader.
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(cfg.election_max_ms + 10)).await;
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        assert_eq!(group.local_replica().role(), PxLocalReplicaRole::Leader);
        let term_before = group.local_replica().current_term_snapshot();

        // Admin step-down via the strict-fence handler.
        let reply = group.local_replica().handle_step_down(&StepDownRequestPayload {
            term: term_before,
            target_leader_id: group.local_replica().id,
            reason: "manual transfer".into(),
        });
        assert!(reply.accepted, "strict-fence StepDown should be accepted");

        // The handler flips role + signals the driver; driver runs the
        // canonical step-down sequence on its next wakeup.
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        assert_eq!(group.local_replica().role(), PxLocalReplicaRole::Follower);
        // Term is preserved on admin step-down.
        assert_eq!(group.local_replica().current_term_snapshot(), term_before);

        cancel.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(1), handle).await;
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn propose_after_admin_step_down_returns_not_leader() {
        use crate::cluster::replica::StepDownRequestPayload;
        use crate::cluster::ProposeResult;

        let cfg = PxElectionConfig::for_tests();
        let replica = PxLocalReplica::new(23, PxLocalReplicaRole::Follower);
        let group = Arc::new(PxGroup::new(77, replica));
        let cancel = group.tenure_cancel();
        let handle = super::spawn(Arc::downgrade(&group), cfg, cancel.clone());

        // Single-voter election: Follower -> Leader. Stamp_proposing_term
        // fires inside finalize_leader, so a proposal here would be admitted.
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(cfg.election_max_ms + 10)).await;
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        assert_eq!(group.local_replica().role(), PxLocalReplicaRole::Leader);
        let term = group.local_replica().current_term_snapshot();
        assert_eq!(group.proposing_term(), term, "proposing_term should match current_term after election win");

        // Admin step-down via the strict-fence handler.
        let reply = group.local_replica().handle_step_down(&StepDownRequestPayload {
            term,
            target_leader_id: group.local_replica().id,
            reason: "test step-down".into(),
        });
        assert!(reply.accepted);
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        assert_eq!(group.local_replica().role(), PxLocalReplicaRole::Follower);

        // Step 9.8: a fresh propose must short-circuit with NotLeader
        // because role != Leader (despite proposing_term still equalling
        // the stale tenure's term).
        match group.propose(b"after-stepdown".to_vec(), Some(1), Some(1)).await {
            ProposeResult::NotLeader { .. } => {}
            other => panic!("expected NotLeader after step-down, got {other:?}"),
        }

        cancel.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(1), handle).await;
    }

    #[test]
    fn xorshift_random_between_ms_stays_in_range() {
        let mut rng = XorShift64::new(0x1234_5678_9ABC_DEF0);
        for _ in 0..200 {
            let v = rng.random_between_ms(30, 60);
            assert!((30..=60).contains(&v), "v={v} out of [30, 60]");
        }
    }
}
