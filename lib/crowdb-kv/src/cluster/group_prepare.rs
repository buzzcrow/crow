// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#![allow(clippy::too_many_lines)]

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Weak};

use futures::stream::FuturesUnordered;
use futures::stream::StreamExt;
use tracing::{debug, error, warn, Instrument};

use crate::cluster::group::{PxGroup, RemoteFoldCtx, RemoteReplicaKind, ReplyFold};
use crate::cluster::local_replica::PxLocalReplica;
use crate::cluster::replica::{PxReplicaError, Replica, ReplicaClient, ReplicaHandler};
use crate::paxos::error::{PxPaxosError, PxPaxosPhase, PxRetryAction};
use crate::paxos::roles::{PxBallot, PxLogEntry, PxPrepareReply};
use crate::paxos::PxNodeId;

/// Outcome of a prepare-phase attempt for one slot.
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

/// Tagged reply from the prepare-phase fan-out. The local future and each
/// remote future produce one of these so a single `FuturesUnordered` can
/// fold them in arrival order (E1 quorum short-circuit). `remote_id` /
/// `endpoint` are captured at future-build time so the fold loop and the
/// detached drain do not need to re-look-up the remote (which may vanish
/// if the group is reconfigured mid-proposal).
enum TaggedPrepareReply {
    Local(Result<PxPrepareReply, PxReplicaError>),
    Remote {
        voting: bool,
        remote_id: PxNodeId,
        endpoint: String,
        reply: Result<PxPrepareReply, PxReplicaError>,
    },
}

impl PxGroup {
    #[tracing::instrument(level = "debug", name = "prepare_phase", skip_all, fields(s = self.log_store_id().unwrap_or(0), g = self.group_id, replica = self.local_replica().id, slot))]
    pub(crate) async fn run_prepare_phase(
        &self,
        replica: &PxLocalReplica,
        slot: u64,
        payload: bytes::Bytes,
        quorum: usize,
        min_round: u64,
    ) -> PrepareAttempt {
        let phase_start = std::time::Instant::now();
        let result = self
            .run_prepare_phase_impl(replica, slot, payload, quorum, min_round)
            .await;
        if let Some(h) = self.write_handles.get() {
            h.prepare_phase
                .observe(phase_start.elapsed().as_nanos().try_into().unwrap_or(u64::MAX));
        }
        result
    }

    async fn run_prepare_phase_impl(
        &self,
        replica: &PxLocalReplica,
        slot: u64,
        payload: bytes::Bytes,
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
        let mut entry = self.base_entry(slot, payload.clone());
        entry.ballot = ballot;
        let term = entry.term;

        let mut fold = ReplyFold::new();

        // E1: quorum short-circuit. When self_weak is set (all add_group
        // groups), fan out via FuturesUnordered with 'static futures
        // (capturing Arc<PxGroup>) so the proposer returns on quorum +
        // local-folded and detaches the remaining replies for side effects
        // (late TermStale → step down, late EpochMismatch → adopt epoch).
        // Fallback to join_all for groups without self_weak (unit-test
        // single-voter groups — no remotes, trivial).
        if let Some(group) = self.self_weak.get().and_then(Weak::upgrade) {
            let membership_epoch = self.membership_epoch();
            let mut futs: FuturesUnordered<
                Pin<Box<dyn Future<Output = TaggedPrepareReply> + Send + 'static>>,
            > = FuturesUnordered::new();
            // Local prepare (W6: counted before Proceed is returned).
            {
                let group = group.clone();
                futs.push(Box::pin(async move {
                    let r = <PxLocalReplica as ReplicaHandler>::on_prepare(
                        &group.local_replica,
                        slot,
                        ballot,
                        term,
                        group_id,
                    )
                    .await;
                    TaggedPrepareReply::Local(r)
                }));
            }
            // Remote prepares.
            for (idx, remote) in group.remote_replicas.iter().enumerate() {
                if let RemoteReplicaKind::Real(remote) = remote {
                    let voting = remote.voting;
                    let remote_id = remote.node_id;
                    let endpoint = remote.endpoint.clone();
                    let group = group.clone();
                    futs.push(Box::pin(async move {
                        let reply = match group.remote_replicas.get(idx) {
                            Some(RemoteReplicaKind::Real(r)) => {
                                r.send_prepare(slot, ballot, term, group_id, membership_epoch)
                                    .await
                            }
                            _ => Err(PxReplicaError::Internal(
                                "prepare: remote vanished mid-fanout".to_string(),
                            )),
                        };
                        TaggedPrepareReply::Remote {
                            voting,
                            remote_id,
                            endpoint,
                            reply,
                        }
                    }));
                }
            }
            while let Some(tagged) = futs.next().await {
                match tagged {
                    TaggedPrepareReply::Local(result) => match result {
                        Ok(reply) => fold.fold_prepare_local(replica.voting(), reply),
                        Err(error) => {
                            error!(
                                group_id,
                                slot,
                                peer = replica.id,
                                error = %error,
                                "local prepare handler failed"
                            );
                            fold.local_folded = true;
                        }
                    },
                    TaggedPrepareReply::Remote {
                        voting,
                        remote_id,
                        endpoint,
                        reply,
                        ..
                    } => {
                        let ctx = RemoteFoldCtx {
                            group_id,
                            slot,
                            remote_id,
                            proposer_epoch: membership_epoch,
                            phase: "prepare",
                        };
                        match reply {
                            Ok(reply) => fold.fold_prepare_remote(voting, &ctx, reply),
                            Err(error) => {
                                error!(
                                    slot,
                                    peer = remote_id,
                                    endpoint = %endpoint,
                                    error = %error,
                                    "prepare rpc failed"
                                );
                            }
                        }
                    }
                }
                // E1 short-circuit: quorum + local folded + no disqualifying
                // TermStale (prepare checks TermStale before quorum, matching
                // the existing failure-decision order). Detach remaining
                // replies for side effects, then break to the Proceed path.
                if fold.accepted >= quorum && fold.local_folded && fold.highest_seen_term.is_none() {
                    let cancel = group.tenure_cancel.clone();
                    let group_drain = group.clone();
                    tokio::spawn(
                        async move {
                            loop {
                                tokio::select! {
                                    biased;
                                    () = cancel.cancelled() => return,
                                    Some(tagged) = futs.next() =>
                                        prepare_drain_side_effect(&group_drain, slot, tagged),
                                    else => return,
                                }
                            }
                        }
                        .instrument(tracing::Span::current()),
                    );
                    break;
                }
            }
        } else {
            // Fallback: join_all (groups without self_weak — no remotes).
            let prepare_futs: Vec<_> = self
                .remote_replicas
                .iter()
                .filter_map(|remote| {
                    if let RemoteReplicaKind::Real(remote) = remote {
                        Some(remote.send_prepare(slot, ballot, term, group_id, self.membership_epoch()))
                    } else {
                        None
                    }
                })
                .collect();

            let (local_result, prepare_results) = tokio::join!(
                <PxLocalReplica as ReplicaHandler>::on_prepare(replica, slot, ballot, term, group_id),
                futures::future::join_all(prepare_futs),
            );

            match local_result {
                Ok(reply) => fold.fold_prepare_local(replica.voting(), reply),
                Err(error) => {
                    error!(
                        group_id,
                        slot,
                        peer = replica.id,
                        error = %error,
                        "local prepare handler failed"
                    );
                    fold.local_folded = true;
                }
            }

            for (remote, result) in self
                .remote_replicas
                .iter()
                .filter(|r| matches!(r, RemoteReplicaKind::Real(_)))
                .zip(prepare_results)
            {
                let RemoteReplicaKind::Real(remote) = remote else {
                    continue;
                };
                let ctx = RemoteFoldCtx {
                    group_id,
                    slot,
                    remote_id: remote.node_id,
                    proposer_epoch: self.membership_epoch(),
                    phase: "prepare",
                };
                match result {
                    Ok(reply) => fold.fold_prepare_remote(remote.voting, &ctx, reply),
                    Err(error) => {
                        error!(
                            slot,
                            peer = remote.node_id,
                            endpoint = remote.endpoint,
                            error = %error,
                            "prepare rpc failed"
                        );
                    }
                }
            }
        }

        let ReplyFold {
            accepted: promised,
            highest_rejected_round,
            highest_seen_term,
            epoch_mismatch,
            adopted,
            local_folded: _,
        } = fold;

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
            if let Some(responder_epoch) = epoch_mismatch {
                return PrepareAttempt::Fail {
                    error: PxPaxosError::MembershipEpochMismatch { responder_epoch },
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
            entry.payload = prev.payload;
        }
        PrepareAttempt::Proceed { entry, foreign_value }
    }
}

/// Apply a late prepare reply observed by the detached drain task (after the
/// proposer already short-circuited on quorum). Only `TermStale` and
/// `EpochMismatch` carry side effects the proposer must not lose; accepted /
/// rejected / error replies are no-ops here (the slot is already proceeding).
fn prepare_drain_side_effect(group: &Arc<PxGroup>, slot: u64, tagged: TaggedPrepareReply) {
    let group_id = group.group_id;
    let (remote_id, reply) = match tagged {
        TaggedPrepareReply::Local(_) => return,
        TaggedPrepareReply::Remote { remote_id, reply, .. } => (remote_id, reply),
    };
    match reply {
        Ok(PxPrepareReply::TermStale { new_term, .. }) => {
            warn!(
                group_id,
                slot, remote_id, new_term, "late prepare TermStale in drain; stepping down"
            );
            group.local_replica.become_follower(new_term);
        }
        Ok(PxPrepareReply::EpochMismatch { responder_epoch }) => {
            let adopted = group.adopt_membership_epoch(responder_epoch);
            warn!(
                group_id,
                slot,
                remote_id,
                responder_epoch,
                adopted_epoch = adopted,
                "late prepare EpochMismatch in drain; adopted responder epoch"
            );
        }
        _ => {}
    }
}
