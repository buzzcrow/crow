// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#![allow(clippy::cast_possible_truncation)]

//! Candidate-state election methods of [`PxGroup`], split out from
//! [`crate::cluster::group_election`]: the election-attempt driver,
//! the `PreVote` round, and the `Candidate` `RequestVote` round.

use std::sync::Arc;

use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

use crate::cluster::group::PxGroup;
use crate::cluster::group_election::{LeaderElection, PreVoteOutcome};
use crate::cluster::replica::{PxReplicaError, ReplicaClient, VoteReply, VoteRequestPayload};
use crate::common::config::PxElectionConfig;
use crate::paxos::PxNodeId;

impl PxGroup {
    /// Drive one full election attempt: optional `PreVote` →
    /// `Candidate` `RequestVote` → `Leader` on win, otherwise stay
    /// `Follower` / `Candidate` until the next deadline.
    pub(crate) async fn run_election_attempt(
        self: &Arc<Self>,
        cfg: &PxElectionConfig,
        cancel: &CancellationToken,
    ) {
        let replica = self.local_replica();

        if cfg.prevote_enabled {
            replica.become_precandidate();
            match self.run_prevote_round(cancel).await {
                PreVoteOutcome::Won { proposed_term } => {
                    replica.become_candidate(proposed_term);
                    replica.persist_current_vote().await;
                    self.run_candidate_election(cancel).await;
                }
                PreVoteOutcome::HigherTerm(t) => {
                    replica.become_follower(t);
                }
                PreVoteOutcome::Lost => {
                    // Step back to Follower in the same term; next
                    // deadline will retry (with a fresh randomized jitter).
                    replica.become_follower(replica.current_term_snapshot());
                }
            }
        } else {
            let new_term = replica.current_term_snapshot() + 1;
            replica.become_candidate(new_term);
            replica.persist_current_vote().await;
            self.run_candidate_election(cancel).await;
        }
    }

    /// Run a single `PreVote` round.
    ///
    /// Fans out `PreVote(proposed_term)` without bumping `current_term`.
    pub(crate) async fn run_prevote_round(self: &Arc<Self>, cancel: &CancellationToken) -> PreVoteOutcome {
        let group_id = self.group_id;
        let replica = self.local_replica();
        let term = replica.current_term_snapshot();
        let proposed_term = term + 1;
        let candidate_id: PxNodeId = replica.id;
        let (accepted_log_tip_slot, accepted_log_tip_term) = replica.accepted_log_tip();

        let payload = VoteRequestPayload {
            term: proposed_term,
            candidate_id,
            accepted_log_tip_slot,
            accepted_log_tip_term,
        };

        let voting_peers = self.voting_remote_ids();
        let quorum = self.quorum();
        // Self-vote only counts if the local replica is itself a voting
        // member -- `quorum` is sized from voting members only, and a
        // non-voting replica has no vote to cast (mirrors the
        // remote-side `voting_remote_ids` filter just above).
        let mut grants: usize = usize::from(replica.voting);

        debug!(
            group_id,
            candidate_id,
            my_term = term,
            proposed_term,
            peer_count = voting_peers.len(),
            quorum,
            "precandidate fanning out PreVote"
        );

        // Trivial-cluster fast path: only win without contacting peers when the
        // group is truly a singleton (no configured voting peers) *and* the
        // local replica is itself voting -- a non-voting singleton has no
        // vote to cast and cannot self-elect.
        if voting_peers.is_empty() && replica.voting {
            return PreVoteOutcome::Won { proposed_term };
        }

        let mut joinset: JoinSet<(PxNodeId, Result<VoteReply, PxReplicaError>)> = JoinSet::new();
        for peer_id in voting_peers {
            let group_for_task = self.clone();
            let req = payload;
            joinset.spawn(async move {
                let result = if let Some(remote) = group_for_task.get_remote_replica(peer_id) {
                    remote.send_pre_vote(req, group_for_task.group_id).await
                } else {
                    Err(PxReplicaError::Internal(format!("peer {peer_id} not present")))
                };
                (peer_id, result)
            });
        }

        loop {
            let joined = tokio::select! {
                biased;
                () = cancel.cancelled() => {
                    joinset.abort_all();
                    return PreVoteOutcome::Lost;
                }
                joined = joinset.join_next() => joined,
            };
            let Some(joined) = joined else { break };
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

        info!(
            group_id,
            candidate_id, proposed_term, grants, quorum, "precandidate failed to gather pre-vote quorum"
        );
        PreVoteOutcome::Lost
    }

    /// Run a single round of `Candidate`-state vote gathering.
    ///
    /// 1. If any peer reply carries `term > my_term` →
    ///    `become_follower(term)` and return.
    /// 2. If `grants >= quorum` → [`Self::finalize_leader`] and return.
    /// 3. Otherwise → stay in `Candidate` and let the next election
    ///    deadline restart the election in `current_term + 1`.
    #[allow(clippy::too_many_lines)]
    pub(crate) async fn run_candidate_election(self: &Arc<Self>, cancel: &CancellationToken) {
        let group_id = self.group_id;
        let replica = self.local_replica();
        let term = replica.current_term_snapshot();
        let candidate_id: PxNodeId = replica.id;
        let (accepted_log_tip_slot, accepted_log_tip_term) = replica.accepted_log_tip();

        let payload = VoteRequestPayload {
            term,
            candidate_id,
            accepted_log_tip_slot,
            accepted_log_tip_term,
        };

        let voting_peers = self.voting_remote_ids();
        let quorum = self.quorum();
        // Local replica votes for itself in `become_candidate`, but only
        // if it is itself a voting member -- see the matching guard in
        // `run_prevote_round`.
        let mut grants: usize = usize::from(replica.voting);
        let mut peer_floor: u64 = replica.contiguous_chosen();
        let mut peer_ceiling: u64 = replica.highest_seen_slot();

        debug!(
            group_id,
            candidate_id,
            term,
            peer_count = voting_peers.len(),
            quorum,
            "candidate fanning out RequestVote"
        );

        // Trivial-cluster fast path: local replica alone constitutes quorum.
        // Only self-elect when there are no configured voting peers *and*
        // the local replica is itself voting (a non-voting replica has no
        // vote to cast and cannot self-elect, mirroring `run_prevote_round`).
        // A restarted node that has recovered a multi-replica persisted
        // config (W9) will have voting peers, so it must actually contact
        // them before becoming leader and running bulk Phase 1. This
        // prevents a quorum=1 self-election in the restore window from
        // overwriting committed data.
        if voting_peers.is_empty() && replica.voting {
            self.finalize_leader(term, peer_floor, peer_ceiling);
            return;
        }

        let mut joinset: JoinSet<(PxNodeId, Result<VoteReply, PxReplicaError>)> = JoinSet::new();
        for peer_id in voting_peers {
            let group_for_task = self.clone();
            let req = payload;
            joinset.spawn(async move {
                let result = if let Some(remote) = group_for_task.get_remote_replica(peer_id) {
                    remote.send_request_vote(req, group_for_task.group_id).await
                } else {
                    Err(PxReplicaError::Internal(format!("peer {peer_id} not present")))
                };
                (peer_id, result)
            });
        }

        loop {
            let joined = tokio::select! {
                biased;
                () = cancel.cancelled() => {
                    joinset.abort_all();
                    return;
                }
                joined = joinset.join_next() => joined,
            };
            let Some(joined) = joined else { break };
            let (peer_id, reply) = match joined {
                Ok(pair) => pair,
                Err(join_err) => {
                    error!(group_id, error = %join_err, "RequestVote task panicked");
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
                        debug!(
                            group_id,
                            candidate_id, term, peer_id, grants, quorum, "RequestVote granted"
                        );
                        if grants >= quorum {
                            joinset.abort_all();
                            self.finalize_leader(term, peer_floor, peer_ceiling);
                            return;
                        }
                    } else {
                        debug!(
                            group_id,
                            candidate_id,
                            term,
                            peer_id,
                            peer_term = vote.term,
                            "RequestVote rejected"
                        );
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
            candidate_id,
            term,
            grants,
            quorum,
            "candidate failed to gather quorum; will retry on next deadline"
        );
    }
}
