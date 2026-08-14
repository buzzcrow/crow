// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::too_many_lines)]

use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::time::sleep;
use tracing::{debug, error, trace, warn};

use crate::cluster::group::{ProposeResult, PxGroup};
use crate::cluster::group_accept::AcceptAttempt;
use crate::cluster::group_election::XorShift64;
use crate::cluster::group_prepare::PrepareAttempt;
use crate::common::config::PaxosConfig;
use crate::paxos::error::PxPaxosError;
use crate::paxos::roles::DedupTag;

impl PxGroup {
    /// Propose an opaque payload through Paxos. Returns the slot if chosen,
    /// or an error string.
    ///
    /// When R45 coalescing is enabled (`coalesce_max_keys > 0` and the
    /// self-weak is set), the first op starts a Paxos round immediately
    /// and concurrent ops arriving during the round join the next batch
    /// (one slot, one quorum round per batch); each coalesced caller
    /// receives `ProposeResult::Chosen { slot }` for the shared slot.
    /// When coalescing is disabled (`coalesce_max_keys = 0`), this is the
    /// legacy one-proposal-per-key path.
    pub async fn propose(&self, payload: Vec<u8>, client_id: Option<u64>, seq: Option<u64>) -> ProposeResult {
        let replica = &self.local_replica;

        // Leadership gate. Checks BOTH:
        //   * role == Leader  -- captures the role atomic flipped by
        //     become_leader / become_follower.
        //   * current_term == proposing_term -- captures the case where
        //     the local replica advanced into a new term (became
        //     follower under HigherTerm, then re-elected) without the
        //     proposing tenure having stamped the new term yet.
        // Either miss surfaces as `NotLeader { hint: leader_endpoint }`
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
        // Paxos (exactly-once writes, idempotent retry). Checked before
        // window admission / coalescing so duplicates never consume a
        // window permit or enter a batch.
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

        let tag = dedup_tag(client_id, seq);

        // R45: event-driven coalescing. When `coalesce_max_keys > 0` and
        // self-weak is set, the first op starts a round immediately (no
        // timer) and ops arriving during the round join the next batch.
        let coalesce_on = self.config.paxos.coalesce_max_keys > 0 && self.self_weak.get().is_some();
        if coalesce_on {
            self.coalesce_enqueue(payload, tag).await
        } else {
            let tags: Vec<DedupTag> = tag.into_iter().collect();
            // `Bytes::from(Vec<u8>)` reuses the allocation (no copy) and
            // gives cheap `Clone` for the slot-retry loop and Accept fanout.
            self.propose_inner(bytes::Bytes::from(payload), &tags).await
        }
    }

    /// Drive one Paxos proposal (single- or multi-key) through to a chosen
    /// slot. Holds one inflight permit for the whole round, allocates one
    /// slot, and records every `dedup_tags` entry against the chosen slot
    /// on the local learner. The leadership gate is re-checked here so a
    /// step-down between coalescer batch collection and flush surfaces as
    /// `NotLeader` instead of racing into Paxos with stale identity.
    pub(super) async fn propose_inner(
        &self,
        payload: bytes::Bytes,
        dedup_tags: &[DedupTag],
    ) -> ProposeResult {
        let e2e_start = std::time::Instant::now();
        let result = self.propose_inner_impl(payload, dedup_tags).await;
        if let Some(h) = self.write_handles.get() {
            h.propose_e2e
                .observe(e2e_start.elapsed().as_nanos().try_into().unwrap_or(u64::MAX));
        }
        result
    }

    async fn propose_inner_impl(&self, payload: bytes::Bytes, dedup_tags: &[DedupTag]) -> ProposeResult {
        let replica = &self.local_replica;

        // Re-check the leadership gate (see `propose`).
        let role_is_leader = replica.role() == crate::cluster::local_replica::PxLocalReplicaRole::Leader;
        let current_term = replica.current_term_snapshot();
        let proposing_term = self.proposing_term.load(Ordering::Acquire);
        if !(role_is_leader && current_term == proposing_term) {
            return ProposeResult::NotLeader {
                leader_hint: self.leader_endpoint().unwrap_or_default(),
            };
        }

        // Sliding-window admission: cap concurrent in-flight proposals. The
        // permit is held for the whole proposal (released on drop at every
        // return path below). Depending on the admission policy, a full
        // window either fails fast with `Busy` (Reject) or blocks until a
        // permit is freed (Queue).
        let Some(_window_permit) = self.inflight.acquire_permit().await else {
            warn!(
                group_id = self.group_id,
                window = self.inflight.total_permits(),
                "inflight window full; rejecting proposal as Busy"
            );
            return ProposeResult::Busy;
        };

        let group_id = self.group_id;
        // Voting-only quorum (`self.quorum`/`cached_quorum`), *not*
        // `valid_replica_count + 1` -- the latter counts non-voting
        // catch-up members too and would inflate the threshold (and,
        // combined with unfiltered ack-counting, could also let
        // non-voting acks satisfy it -- see `run_accept_phase`'s
        // `remote.voting` guard).
        let quorum = self.quorum();
        let mut slot = self.next_slot.fetch_add(1, Ordering::Relaxed);
        let mut last_error = String::new();

        trace!(
            group_id,
            my_id = self.local_replica.id,
            dedup_tags = dedup_tags.len(),
            peer_count = self.valid_replica_count,
            quorum,
            "start paxos proposal"
        );

        'slot_retry: for _slot_attempt in 0..PaxosConfig::DEFAULT.max_slot_retries {
            let base_entry = self.base_entry(slot, payload.clone());
            let mut force_prepare = self.config.force_classic; // Classic: always prepare; Leader: Phase-2 only
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
                        .run_prepare_phase(replica, slot, payload.clone(), quorum, min_round)
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
                                self.watch_registry.clear();
                                replica.become_follower(*current_term);
                                return ProposeResult::NotLeader {
                                    leader_hint: self.leader_endpoint().unwrap_or_default(),
                                };
                            }
                            if let PxPaxosError::MembershipEpochMismatch { responder_epoch } = &error {
                                let adopted = self.adopt_membership_epoch(*responder_epoch);
                                warn!(
                                    group_id,
                                    slot,
                                    attempt,
                                    responder_epoch,
                                    adopted_epoch = adopted,
                                    "prepare epoch mismatch; adopted responder epoch, retrying same slot"
                                );
                                last_error = error.keyword().to_string();
                                sleep(Self::retry_backoff(attempt)).await;
                                continue;
                            }
                            last_error = error.keyword().to_string();
                            break;
                        }
                    }
                } else if min_round > entry.ballot.round {
                    entry.ballot.round = min_round;
                }

                match self.run_accept_phase(replica, &entry, dedup_tags, quorum).await {
                    AcceptAttempt::Chosen => {
                        // R17: when async_engine_apply is enabled, spawn
                        // the engine apply as a background task and return
                        // Chosen immediately. The fan_out_chosen_notice
                        // fires immediately too (non-blocking mpsc enqueue).
                        if self.config.async_engine_apply {
                            replica.spawn_learn_chosen(entry.clone(), dedup_tags);
                        } else {
                            replica.learn_chosen(&entry, dedup_tags).await;
                        }
                        self.fan_out_chosen_notice(&entry, group_id);
                        trace!(
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
                            self.watch_registry.clear();
                            replica.become_follower(*current_term);
                            return ProposeResult::NotLeader {
                                leader_hint: self.leader_endpoint().unwrap_or_default(),
                            };
                        }
                        if let PxPaxosError::MembershipEpochMismatch { responder_epoch } = &error {
                            let adopted = self.adopt_membership_epoch(*responder_epoch);
                            warn!(
                                group_id,
                                slot,
                                attempt,
                                responder_epoch,
                                adopted_epoch = adopted,
                                "accept epoch mismatch; adopted responder epoch, retrying same slot"
                            );
                            last_error = error.keyword().to_string();
                            sleep(Self::retry_backoff(attempt)).await;
                            continue;
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

    pub(super) fn retry_backoff(attempt: usize) -> Duration {
        let factor = 1u64 << attempt.min(6);
        let base = PaxosConfig::DEFAULT.retry_base_backoff_ms.saturating_mul(factor);
        // E4: ±50% jitter to decorrelate retry storms across replicas.
        // `jitter_mult` is in `[500, 1500]` (milli-units), so
        // `base * jitter_mult / 1000` gives `[base/2, base*3/2]`.
        let jitter_mult = retry_jitter_multiplier();
        Duration::from_millis(base.saturating_mul(jitter_mult) / 1000)
    }
}

thread_local! {
    static RETRY_JITTER_RNG: std::cell::RefCell<XorShift64> =
        std::cell::RefCell::new(XorShift64::new(seed_retry_jitter()));
}

fn seed_retry_jitter() -> u64 {
    // Seed from node id + monotonic nanos so two replicas starting
    // simultaneously get different sequences.
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos() as u64);
    nanos.wrapping_mul(0x9E37_79B9_7F4A_7C15)
}

fn retry_jitter_multiplier() -> u64 {
    RETRY_JITTER_RNG.with(|rng| {
        let r = rng.borrow_mut().next_u64() % 1001;
        500 + r
    })
}

/// Build a dedup tag from the client-supplied `(client_id, seq)` options.
/// `None` when either is absent or `client_id == 0` (the no-dedup sentinel
/// matching `PxLearner::record_dedup_tags`).
fn dedup_tag(client_id: Option<u64>, seq: Option<u64>) -> Option<DedupTag> {
    match (client_id, seq) {
        (Some(cid), Some(s)) if cid != 0 => Some(DedupTag {
            client_id: cid,
            seq: s,
        }),
        _ => None,
    }
}
