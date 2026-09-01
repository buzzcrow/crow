// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#![allow(clippy::too_many_lines)]

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Weak};

use futures::future::join_all;
use futures::stream::FuturesUnordered;
use futures::stream::StreamExt;
use tracing::{error, trace, warn};

use crate::cluster::group::{PxGroup, RemoteFoldCtx, RemoteReplicaKind, ReplyFold};
use crate::cluster::local_replica::PxLocalReplica;
use crate::cluster::replica::{PxReplicaError, Replica, ReplicaClient, ReplicaHandler};
use crate::paxos::error::{PxPaxosError, PxPaxosPhase, PxRetryAction};
use crate::paxos::roles::{DedupTag, PxAcceptReply, PxBallot, PxLogEntry};
use crate::paxos::PxNodeId;

/// Outcome of an accept-phase attempt for one slot.
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

/// Tagged reply from the accept-phase fan-out. The R16b local path produces
/// an infallible `PxAcceptReply` (wrapped in `Ok` to normalize); R16a local
/// and all remotes produce `Result<PxAcceptReply, PxReplicaError>`.
enum TaggedAcceptReply {
    Local(Result<PxAcceptReply, PxReplicaError>),
    Remote {
        voting: bool,
        remote_id: PxNodeId,
        endpoint: String,
        reply: Result<PxAcceptReply, PxReplicaError>,
    },
}

impl PxGroup {
    pub(crate) async fn run_accept_phase(
        &self,
        replica: &PxLocalReplica,
        entry: &PxLogEntry,
        dedup_tags: &[DedupTag],
        quorum: usize,
    ) -> AcceptAttempt {
        let quorum_rpc_start = std::time::Instant::now();
        let mut fold = ReplyFold::new();
        let group_id = self.group_id;
        trace!(
            group_id,
            slot = entry.slot,
            round = entry.ballot.round,
            peer_count = self.valid_replica_count,
            quorum,
            "run accept phase"
        );

        // E1: quorum short-circuit. When self_weak is set, fan out via
        // FuturesUnordered with 'static futures (capturing Arc<PxGroup>) so
        // the proposer returns on quorum + local-folded and detaches the
        // remaining replies for side effects. Fallback to join_all for
        // groups without self_weak (unit-test single-voter groups).
        if let Some(group) = self.self_weak.get().and_then(Weak::upgrade) {
            let membership_epoch = self.membership_epoch();
            let entry_owned = entry.clone();
            let dedup_tags_owned = dedup_tags.to_vec();
            let slot = entry.slot;

            if self.config.wal_early_ack && self.cached_quorum > 1 {
                // R16b: split path — CAS only, persist deferred.
                let mut futs = build_accept_remote_futs(
                    &group,
                    &entry_owned,
                    &dedup_tags_owned,
                    group_id,
                    membership_epoch,
                );
                // Local CAS (infallible → wrapped in Ok to normalize).
                {
                    let group = group.clone();
                    let entry = entry_owned.clone();
                    futs.push(Box::pin(async move {
                        let r = group.local_replica.on_accept_inner(&entry).await;
                        TaggedAcceptReply::Local(Ok(r))
                    }));
                }
                let mut local_accepted = false;
                while let Some(tagged) = futs.next().await {
                    match tagged {
                        TaggedAcceptReply::Local(Ok(reply)) => {
                            local_accepted = matches!(reply, PxAcceptReply::Accepted { .. });
                            fold.fold_accept_local(replica.voting(), &reply);
                        }
                        TaggedAcceptReply::Local(Err(_)) => {
                            unreachable!("on_accept_inner is infallible")
                        }
                        TaggedAcceptReply::Remote {
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
                                phase: "accept",
                            };
                            match reply {
                                Ok(reply) => fold.fold_accept_remote(voting, &ctx, &reply),
                                Err(error) => {
                                    error!(
                                        group_id, slot, remote_id, endpoint = %endpoint,
                                        error = %error, "accept rpc failed"
                                    );
                                }
                            }
                        }
                    }
                    // R16b short-circuit: local CAS + quorum.
                    if local_accepted && fold.accepted >= quorum {
                        let cancel = group.tenure_cancel.clone();
                        let group_drain = group.clone();
                        tokio::spawn(async move {
                            loop {
                                tokio::select! {
                                    biased;
                                    () = cancel.cancelled() => return,
                                    Some(tagged) = futs.next() =>
                                        accept_drain_side_effect(&group_drain, slot, tagged),
                                    else => return,
                                }
                            }
                        });
                        if let Some(h) = self.write_handles.get() {
                            h.accept_quorum_rpc.observe(
                                quorum_rpc_start
                                    .elapsed()
                                    .as_nanos()
                                    .try_into()
                                    .unwrap_or(u64::MAX),
                            );
                        }
                        replica.spawn_accept_persist(entry.clone());
                        return AcceptAttempt::Chosen;
                    }
                }
            } else {
                // R16a: default path — local on_accept (CAS + WAL persist).
                let mut futs = build_accept_remote_futs(
                    &group,
                    &entry_owned,
                    &dedup_tags_owned,
                    group_id,
                    membership_epoch,
                );
                {
                    let group = group.clone();
                    let entry = entry_owned.clone();
                    futs.push(Box::pin(async move {
                        let r = <PxLocalReplica as ReplicaHandler>::on_accept(
                            &group.local_replica,
                            &entry,
                            group_id,
                        )
                        .await;
                        TaggedAcceptReply::Local(r)
                    }));
                }
                while let Some(tagged) = futs.next().await {
                    match tagged {
                        TaggedAcceptReply::Local(result) => match result {
                            Ok(reply) => fold.fold_accept_local(replica.voting(), &reply),
                            Err(error) => {
                                error!(
                                    group_id, slot, replica_id = replica.id,
                                    error = %error, "local accept handler failed"
                                );
                                fold.local_folded = true;
                            }
                        },
                        TaggedAcceptReply::Remote {
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
                                phase: "accept",
                            };
                            match reply {
                                Ok(reply) => fold.fold_accept_remote(voting, &ctx, &reply),
                                Err(error) => {
                                    error!(
                                        group_id, slot, remote_id, endpoint = %endpoint,
                                        error = %error, "accept rpc failed"
                                    );
                                }
                            }
                        }
                    }
                    // R16a short-circuit: quorum + local folded (W6).
                    if fold.accepted >= quorum && fold.local_folded {
                        let cancel = group.tenure_cancel.clone();
                        let group_drain = group.clone();
                        tokio::spawn(async move {
                            loop {
                                tokio::select! {
                                    biased;
                                    () = cancel.cancelled() => return,
                                    Some(tagged) = futs.next() =>
                                        accept_drain_side_effect(&group_drain, slot, tagged),
                                    else => return,
                                }
                            }
                        });
                        if let Some(h) = self.write_handles.get() {
                            h.accept_quorum_rpc.observe(
                                quorum_rpc_start
                                    .elapsed()
                                    .as_nanos()
                                    .try_into()
                                    .unwrap_or(u64::MAX),
                            );
                        }
                        break;
                    }
                }
            }
        } else {
            // Fallback: join_all (groups without self_weak — no remotes).
            let accept_futs: Vec<_> = self
                .remote_replicas
                .iter()
                .filter_map(|remote| {
                    if let RemoteReplicaKind::Real(remote) = remote {
                        Some(remote.send_accept(entry, dedup_tags, group_id, self.membership_epoch()))
                    } else {
                        None
                    }
                })
                .collect();

            if self.config.wal_early_ack && self.cached_quorum > 1 {
                let (local_result, accept_results) =
                    tokio::join!(replica.on_accept_inner(entry), join_all(accept_futs),);

                let local_accepted = matches!(&local_result, PxAcceptReply::Accepted { .. });
                fold.fold_accept_local(replica.voting(), &local_result);

                for (remote, result) in self
                    .remote_replicas
                    .iter()
                    .filter(|r| matches!(r, RemoteReplicaKind::Real(_)))
                    .zip(accept_results)
                {
                    let RemoteReplicaKind::Real(remote) = remote else {
                        continue;
                    };
                    let ctx = RemoteFoldCtx {
                        group_id,
                        slot: entry.slot,
                        remote_id: remote.node_id,
                        proposer_epoch: self.membership_epoch(),
                        phase: "accept",
                    };
                    match result {
                        Ok(reply) => fold.fold_accept_remote(remote.voting, &ctx, &reply),
                        Err(error) => {
                            error!(
                                group_id, slot = entry.slot, remote_id = remote.node_id,
                                endpoint = remote.endpoint, error = %error, "accept rpc failed"
                            );
                        }
                    }
                }

                if local_accepted && fold.accepted >= quorum {
                    replica.spawn_accept_persist(entry.clone());
                    return AcceptAttempt::Chosen;
                }
            } else {
                let (local_result, accept_results) = tokio::join!(
                    <PxLocalReplica as ReplicaHandler>::on_accept(replica, entry, group_id),
                    join_all(accept_futs),
                );

                match local_result {
                    Ok(reply) => fold.fold_accept_local(replica.voting(), &reply),
                    Err(error) => {
                        error!(
                            group_id, slot = entry.slot, replica_id = replica.id,
                            error = %error, "local accept handler failed"
                        );
                        fold.local_folded = true;
                    }
                }

                for (remote, result) in self
                    .remote_replicas
                    .iter()
                    .filter(|r| matches!(r, RemoteReplicaKind::Real(_)))
                    .zip(accept_results)
                {
                    let RemoteReplicaKind::Real(remote) = remote else {
                        continue;
                    };
                    let ctx = RemoteFoldCtx {
                        group_id,
                        slot: entry.slot,
                        remote_id: remote.node_id,
                        proposer_epoch: self.membership_epoch(),
                        phase: "accept",
                    };
                    match result {
                        Ok(reply) => fold.fold_accept_remote(remote.voting, &ctx, &reply),
                        Err(error) => {
                            error!(
                                group_id, slot = entry.slot, remote_id = remote.node_id,
                                endpoint = remote.endpoint, error = %error, "accept rpc failed"
                            );
                        }
                    }
                }
            }
        }

        let ReplyFold {
            accepted,
            highest_rejected_round,
            highest_seen_term,
            epoch_mismatch,
            adopted: _,
            local_folded: _,
        } = fold;

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
        if let Some(responder_epoch) = epoch_mismatch {
            return AcceptAttempt::Fail {
                error: PxPaxosError::MembershipEpochMismatch { responder_epoch },
            };
        }
        AcceptAttempt::Fail {
            error: PxPaxosError::QuorumUnavailable {
                phase: PxPaxosPhase::Accept,
            },
        }
    }
}

/// Apply a late accept reply observed by the detached drain task. Mirrors
/// [`prepare_drain_side_effect`] for the accept reply type.
fn accept_drain_side_effect(group: &Arc<PxGroup>, slot: u64, tagged: TaggedAcceptReply) {
    let group_id = group.group_id;
    let (remote_id, reply) = match tagged {
        TaggedAcceptReply::Local(_) => return,
        TaggedAcceptReply::Remote { remote_id, reply, .. } => (remote_id, reply),
    };
    match reply {
        Ok(PxAcceptReply::TermStale { new_term, .. }) => {
            warn!(
                group_id,
                slot, remote_id, new_term, "late accept TermStale in drain; stepping down"
            );
            group.local_replica.become_follower(new_term);
        }
        Ok(PxAcceptReply::EpochMismatch { responder_epoch }) => {
            let adopted = group.adopt_membership_epoch(responder_epoch);
            warn!(
                group_id,
                slot,
                remote_id,
                responder_epoch,
                adopted_epoch = adopted,
                "late accept EpochMismatch in drain; adopted responder epoch"
            );
        }
        _ => {}
    }
}

/// Build the `FuturesUnordered` of remote accept RPC futures for the E1
/// short-circuit path. Each future captures `Arc<PxGroup>` (so it is
/// `'static`) and resolves to a `TaggedAcceptReply::Remote`. Shared by the
/// R16b and R16a accept paths (only the local future differs).
fn build_accept_remote_futs(
    group: &Arc<PxGroup>,
    entry: &PxLogEntry,
    dedup_tags: &[DedupTag],
    group_id: u64,
    membership_epoch: u64,
) -> FuturesUnordered<Pin<Box<dyn Future<Output = TaggedAcceptReply> + Send + 'static>>> {
    let futs: FuturesUnordered<Pin<Box<dyn Future<Output = TaggedAcceptReply> + Send + 'static>>> =
        FuturesUnordered::new();
    for (idx, remote) in group.remote_replicas.iter().enumerate() {
        if let RemoteReplicaKind::Real(remote) = remote {
            let voting = remote.voting;
            let remote_id = remote.node_id;
            let endpoint = remote.endpoint.clone();
            let group = group.clone();
            let entry = entry.clone();
            let dedup_tags = dedup_tags.to_owned();
            futs.push(Box::pin(async move {
                let reply = match group.remote_replicas.get(idx) {
                    Some(RemoteReplicaKind::Real(r)) => {
                        r.send_accept(&entry, &dedup_tags, group_id, membership_epoch)
                            .await
                    }
                    _ => Err(PxReplicaError::Internal(
                        "accept: remote vanished mid-fanout".to_string(),
                    )),
                };
                TaggedAcceptReply::Remote {
                    voting,
                    remote_id,
                    endpoint,
                    reply,
                }
            }));
        }
    }
    futs
}
