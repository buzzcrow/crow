// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#![allow(clippy::cast_possible_truncation)]

use crate::cluster::local_replica::{PxLocalReplica, PxLocalReplicaRole};
use crate::cluster::replica::{
    HeartbeatReply, HeartbeatRequestPayload, StepDownReply, StepDownRequestPayload, VoteReply,
    VoteRequestPayload,
};
use crate::paxos::PxTerm;
use crate::wal::record::WALRecord;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use tracing::{debug, info, trace};

impl PxLocalReplica {
    /// Transition to `Follower` and adopt `new_term` if higher.
    ///
    /// Resets `voted_for` on every term bump (Raft-style — one vote per
    /// term per peer). Expires leader lease state. Callers that want to
    /// reset the election deadline must do so on the election driver task.
    #[tracing::instrument(level = "info", skip_all, fields(replica_l_id = self.id, new_term = new_term))]
    pub fn become_follower(&self, new_term: PxTerm) {
        self.with_election_state(|s| {
            if new_term > s.current_term {
                s.current_term = new_term;
                s.voted_for = None;
            }
            s.role = PxLocalReplicaRole::Follower;
            // Stepping down: the believed leader is unknown until the
            // next heartbeat / vote round establishes one.
            s.leader_id = None;
        });
        self.role_atomic
            .store(PxLocalReplicaRole::Follower.as_u8(), Ordering::Release);
        // Lease is no longer meaningful as a non-leader. Expire it so any
        // stale read fast-path attempt rejects.
        self.reset_lease_to(Instant::now());
        info!(replica_l_id = self.id, current_term = new_term, "become_follower");
    }

    /// Transition to `PreCandidate`. Does not bump term (per Raft `PreVote`).
    #[tracing::instrument(level = "info", skip_all, fields(replica_l_id = self.id))]
    pub fn become_precandidate(&self) {
        self.with_election_state(|s| {
            s.role = PxLocalReplicaRole::PreCandidate;
        });
        self.role_atomic
            .store(PxLocalReplicaRole::PreCandidate.as_u8(), Ordering::Release);
        info!(
            replica_l_id = self.id,
            current_term = self.current_term_snapshot(),
            "become_precandidate"
        );
    }

    /// Transition to `Candidate`. Bumps term to `new_term`, votes for self.
    ///
    /// Caller is responsible for fanning out `RequestVote` to peers.
    #[tracing::instrument(level = "info", skip_all, fields(replica_l_id = self.id, new_term = new_term))]
    pub fn become_candidate(&self, new_term: PxTerm) {
        let lease = Duration::from_millis(self.lease_duration_ms());
        let now = Instant::now();
        self.with_election_state(|s| {
            s.current_term = new_term;
            s.voted_for = Some(self.id);
            s.role = PxLocalReplicaRole::Candidate;
            // Extend vote lockout on self-vote, consistent with
            // handle_request_vote which extends it on external grants.
            // Without this a leader that just won could immediately
            // grant PreVote/RequestVote to a challenger before it has
            // sent its first heartbeat round.
            s.vote_lockout_until = now + lease;
        });
        self.role_atomic
            .store(PxLocalReplicaRole::Candidate.as_u8(), Ordering::Release);
        // One election attempt initiated.
        if let Some(h) = self.election_handles.get() {
            h.elections.inc();
        }
        info!(
            replica_l_id = self.id,
            current_term = new_term,
            "become_candidate"
        );
    }

    /// Transition to `Leader`. Initializes lease state as already-expired so
    /// the first heartbeat round must extend it before the read fast-path
    /// becomes available.
    #[tracing::instrument(level = "info", skip_all, fields(replica_l_id = self.id))]
    pub fn become_leader(&self) {
        self.with_election_state(|s| {
            s.role = PxLocalReplicaRole::Leader;
            s.leader_id = Some(self.id);
        });
        self.role_atomic
            .store(PxLocalReplicaRole::Leader.as_u8(), Ordering::Release);
        self.reset_lease_to(Instant::now());
        info!(
            replica_l_id = self.id,
            current_term = self.current_term_snapshot(),
            "become_leader"
        );
    }

    pub async fn persist_current_vote(&self) {
        if let Some(wal) = &self.wal {
            let term = self.current_term_snapshot();
            let voted_for = self.voted_for().unwrap_or(self.id);
            let record = WALRecord::from_vote_granted(wal.group_id(), term, voted_for);
            if let Err(e) = wal.append(&record).await {
                tracing::error!(term, voted_for, error = %e, "WAL persist VoteGranted failed");
            }
        }
    }

    /// Candidate's durable acceptor log is at least as up-to-date as ours iff
    /// `(accepted_log_tip_term, accepted_log_tip_slot)` is lexicographically
    /// `>=` ours.
    fn candidate_log_up_to_date(&self, req: &VoteRequestPayload) -> bool {
        let (my_slot, my_term) = self.accepted_log_tip();
        (req.accepted_log_tip_term, req.accepted_log_tip_slot) >= (my_term, my_slot)
    }

    /// `PreVote` decision (no state mutation). Reply iff:
    /// - `req.term > current_term` (we'd vote in that term), AND
    /// - candidate's log is up-to-date, AND
    /// - `now >= vote_lockout_until`.
    pub(super) fn handle_pre_vote(&self, req: VoteRequestPayload) -> VoteReply {
        let now = Instant::now();
        let state = self.election_state.lock();
        let granted = req.term > state.current_term
            && self.candidate_log_up_to_date(&req)
            && now >= state.vote_lockout_until;
        let term = state.current_term;
        drop(state);
        let (contiguous_chosen, last_chosen_term, highest_seen_slot) = self.frontier_triple();
        debug!(
            replica_l_id = self.id,
            candidate_id = req.candidate_id,
            proposed_term = req.term,
            current_term = term,
            granted,
            "on_pre_vote"
        );
        VoteReply {
            term,
            granted,
            contiguous_chosen,
            last_chosen_term,
            highest_seen_slot,
        }
    }

    /// `RequestVote` decision (state-mutating). Same checks as `handle_pre_vote`
    /// plus a `voted_for ∈ {None, candidate_id}` check in `req.term`. On grant
    /// the responder adopts `req.term`, sets `voted_for = candidate_id`, and
    /// extends `vote_lockout_until`.
    pub(super) async fn handle_request_vote(&self, req: VoteRequestPayload) -> VoteReply {
        // Vote lockout extension reuses the configured lease duration so
        // followers honor the per-group profile (e.g. WAN) rather than the
        // hard-coded default.
        let lease = Duration::from_millis(self.lease_duration_ms());
        let now = Instant::now();
        let log_up_to_date = self.candidate_log_up_to_date(&req);

        let (granted, term) = {
            let mut state = self.election_state.lock();
            let lockout_ok = now >= state.vote_lockout_until;
            let term_ok = req.term > state.current_term
                || (req.term == state.current_term
                    && state.voted_for.map_or(true, |v| v == req.candidate_id));
            let mut granted = false;
            if term_ok && log_up_to_date && lockout_ok {
                if req.term > state.current_term {
                    state.current_term = req.term;
                    state.voted_for = None;
                    state.role = PxLocalReplicaRole::Follower;
                    state.leader_id = None;
                }
                state.voted_for = Some(req.candidate_id);
                state.vote_lockout_until = now + lease;
                granted = true;
            }
            (granted, state.current_term)
        };
        // Mirror the atomic snapshots if we mutated.
        if granted {
            self.current_term_atomic.store(term, Ordering::Release);
            self.role_atomic
                .store(PxLocalReplicaRole::Follower.as_u8(), Ordering::Release);
            self.persist_current_vote().await;
            // After granting a vote, reset our own election deadline so we
            // give the candidate a chance to win quorum and start sending
            // heartbeats before we time out and challenge it ourselves.
            self.deadline_reset_signal.notify_one();
        }
        let (contiguous_chosen, last_chosen_term, highest_seen_slot) = self.frontier_triple();
        debug!(
            replica_l_id = self.id,
            candidate_id = req.candidate_id,
            req_term = req.term,
            current_term = term,
            granted,
            "on_request_vote"
        );
        VoteReply {
            term,
            granted,
            contiguous_chosen,
            last_chosen_term,
            highest_seen_slot,
        }
    }

    /// `Heartbeat` handler. Adopts a higher term, records the leader id, bumps
    /// the vote-lockout window. Returns `success=true` iff the term is `>=`
    /// our own; on lower term we reply `false` so the stale leader can step
    /// down on its next bookkeeping check.
    pub(super) fn handle_heartbeat(&self, req: HeartbeatRequestPayload) -> HeartbeatReply {
        let lease = Duration::from_millis(self.lease_duration_ms());
        let now = Instant::now();
        let term;
        let success;
        {
            let mut state = self.election_state.lock();
            if req.term < state.current_term {
                term = state.current_term;
                success = false;
            } else {
                if req.term > state.current_term {
                    state.current_term = req.term;
                    state.voted_for = None;
                }
                state.role = PxLocalReplicaRole::Follower;
                state.leader_id = Some(req.leader_id);
                state.vote_lockout_until = now + lease;
                term = state.current_term;
                success = true;
            }
        }
        if success {
            self.current_term_atomic.store(term, Ordering::Release);
            self.role_atomic
                .store(PxLocalReplicaRole::Follower.as_u8(), Ordering::Release);
            // R63: reset the election deadline first (liveness). The follower
            // has confirmed the leader is alive (term check passed) and must
            // not time out regardless of how long the background apply takes.
            self.deadline_reset_signal.notify_one();
            // Timestamp the heartbeat so the metrics snapshot can
            // report `last_heartbeat_age_ms`.
            self.note_heartbeat_received();
            // R63: store the leader's commit point and wake the background
            // apply loop. The heartbeat reply returns immediately with the
            // current `contiguous_applied` (which may lag behind
            // `known_commit_slot`). The leader's catch-up replay handles
            // convergence; the background loop advances `contiguous_applied`
            // at its own pace.
            self.advance_known_commit_slot(req.committed_safe_slot);
            self.wake_apply_loop();
        }
        let (contiguous_chosen, last_chosen_term, highest_seen_slot) = self.frontier_triple();
        let contiguous_applied = self.contiguous_applied();
        // This replica's own durable-snapshot watermark (`WalEngine::snapshot_slot`,
        // set by `group_maintenance::run_pass` whenever `KVEngine::persist_snapshot`
        // advances) -- `0` when there is no WAL (e.g. testkit setups), matching the
        // always-safe default `KVEngine::persist_snapshot` itself documents. The
        // leader aggregates this across every voting peer's heartbeat reply into
        // `PxGroup::group_snapshot_slot`.
        let durable_snapshot_slot = self.wal().map_or(0, |w| w.snapshot_slot());
        trace!(
            replica_l_id = self.id,
            leader_id = req.leader_id,
            req_term = req.term,
            current_term = term,
            success,
            "on_heartbeat"
        );
        HeartbeatReply {
            term,
            success,
            contiguous_chosen,
            last_chosen_term,
            contiguous_applied,
            highest_seen_slot,
            durable_snapshot_slot,
        }
    }

    /// `StepDown` handler. Strict-fence policy:
    /// accept iff `self.is_leader && self.id == req.target_leader_id &&
    /// req.term == current_term`. On accept the replica becomes a follower in
    /// the same term; the election driver picks up the role change
    /// on its next tick and runs the full step-down sequence (cancel bulk
    /// Phase 1, stop heartbeats, drain proposals).
    pub fn handle_step_down(&self, req: &StepDownRequestPayload) -> StepDownReply {
        let snapshot = self.election_state_snapshot();
        let accepted = snapshot.role == PxLocalReplicaRole::Leader
            && self.id == req.target_leader_id
            && req.term == snapshot.current_term;
        if accepted {
            info!(
                replica_l_id = self.id,
                req_term = req.term,
                reason = %req.reason,
                "on_step_down accepted (strict fence)"
            );
            // Stay in the same term; only the role flips. The election
            // driver waits on `admin_step_down_signal` and runs the
            // canonical step-down sequence (cancel per-tenure
            // token, drain proposals, log reason=Admin) on its next
            // wakeup. We still flip the role here so any concurrent
            // proposer leadership check observes Follower without
            // needing the driver to advance first.
            self.become_follower(snapshot.current_term);
            self.admin_step_down_signal.notify_waiters();
        } else {
            debug!(
                replica_l_id = self.id,
                req_term = req.term,
                self_term = snapshot.current_term,
                self_role = ?snapshot.role,
                target_leader_id = req.target_leader_id,
                reason = %req.reason,
                "on_step_down rejected by strict fence"
            );
        }
        StepDownReply {
            accepted,
            current_term: snapshot.current_term,
            current_leader_id: snapshot.leader_id.unwrap_or(0),
        }
    }
}
