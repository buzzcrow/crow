// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#![allow(clippy::too_many_lines)]
#![allow(clippy::cast_possible_truncation)]

use std::sync::atomic::Ordering;
use std::sync::Weak;
use std::time::Duration;

use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::cluster::group::PxGroup;
use crate::paxos::roles::Acceptor;
use crate::paxos::PxGroupId;

/// R65: Follower-side `FetchGap` driver. Periodically checks the local
/// replica's `gap_slots` set and sends `FetchGap` to the leader for each
/// gap. On reply, overwrites the stale/missing value and wakes the apply
/// loop. Bounded by `MAX_INFLIGHT_FETCHGAP`.
pub(crate) async fn run_fetchgap_driver(
    group: Weak<PxGroup>,
    group_id: PxGroupId,
    cancel: CancellationToken,
) {
    let tick = Duration::from_millis(10);
    loop {
        if cancel.is_cancelled() {
            return;
        }
        if sleep_or_cancel(&cancel, tick).await {
            return;
        }
        let Some(group) = group.upgrade() else {
            return;
        };
        // R65: update replication gauges periodically.
        group.local_replica().update_replication_gauges();
        // Only followers need FetchGap — the leader is the source of
        // truth and has no gaps.
        if group.local_replica().is_leader() {
            continue;
        }
        let leader_id = group.leader_id();
        if leader_id == 0 {
            continue;
        }
        let Some(remote) = group.get_remote_replica(leader_id) else {
            continue;
        };
        // R65: snapshot fallback — if gap count exceeds the threshold,
        // skip FetchGap and log a warning. A full automatic snapshot
        // install for running replicas is a follow-up; for now the
        // follower will eventually catch up via ChosenNotice + heartbeat.
        let gap_count = group.local_replica().gap_slots.lock().len();
        let threshold = group.config.election.catchup_snapshot_threshold;
        if gap_count > threshold as usize {
            warn!(
                group_id,
                gap_count,
                threshold,
                "gap count exceeds snapshot threshold; FetchGap skipped (snapshot fallback not yet automatic)"
            );
            continue;
        }
        // Clone the learner stream Arc so the spawned task can send
        // FetchGap without holding a reference to the group.
        let stream = remote.learner_stream().clone();
        // R32: if the crow-rpc transport is set, clone it + the endpoint
        // so the spawned task can call `transport.send_fetch_gap()`
        // directly instead of going through the gRPC LearnerStream.
        let rpc_transport = remote.rpc_transport().cloned();
        let rpc_endpoint = remote.endpoint_str().to_string();
        let rpc_timeout = Duration::from_millis(group.config.election.learner_stream_rpc_timeout_ms);
        let replica = group.local_replica();
        let term = replica.current_term_snapshot();
        let gaps = replica.drain_gaps_for_fetchgap();
        if gaps.is_empty() {
            continue;
        }
        replica.incr_fetchgap_sent(gaps.len() as u64);
        // Clone the Arc fields needed by the spawned task.
        let fetchgap_inflight = replica.fetchgap_inflight.clone();
        let gap_slots = replica.gap_slots.clone();
        let learner = replica.learner.clone();
        let acceptor = replica.acceptor.clone();
        let apply_notify = replica.apply_notify.clone();
        let fetchgap_received = replica
            .replication_handles
            .get()
            .map(|h| h.fetchgap_received.clone());
        let fetchgap_failed = replica
            .replication_handles
            .get()
            .map(|h| h.fetchgap_failed.clone());
        // Spawn a task per gap to send FetchGap concurrently.
        for slot in gaps {
            let stream_clone = stream.clone();
            let rpc_transport_clone = rpc_transport.clone();
            let rpc_endpoint_clone = rpc_endpoint.clone();
            let rpc_timeout_clone = rpc_timeout;
            let fetchgap_inflight_clone = fetchgap_inflight.clone();
            let gap_slots_clone = gap_slots.clone();
            let learner_clone = learner.clone();
            let acceptor_clone = acceptor.clone();
            let apply_notify_clone = apply_notify.clone();
            let fetchgap_received_clone = fetchgap_received.clone();
            let fetchgap_failed_clone = fetchgap_failed.clone();
            tokio::spawn(async move {
                // R32: prefer crow-rpc transport when available; fall
                // back to gRPC LearnerStream otherwise.
                let result = if let Some(ref transport) = rpc_transport_clone {
                    tokio::time::timeout(
                        rpc_timeout_clone,
                        transport.send_fetch_gap(&rpc_endpoint_clone, group_id, slot, term, leader_id),
                    )
                    .await
                    .map_err(|_| {
                        crate::cluster::replica::PxReplicaError::Internal(format!(
                            "fetch_gap rpc timeout after {} ms at peer {}",
                            rpc_timeout_clone.as_millis(),
                            rpc_endpoint_clone
                        ))
                    })
                    .and_then(|r| r)
                } else {
                    let req = crate::rpc::FetchGapRequest {
                        version: 1,
                        group_id,
                        slot,
                        term,
                        leader_id,
                    };
                    stream_clone.send_fetch_gap(req).await.map(|resp| {
                        crate::cluster::replica::FetchGapReply {
                            group_id: resp.group_id,
                            slot: resp.slot,
                            term: resp.term,
                            ballot_round: resp.ballot_round,
                            leader_id: resp.leader_id,
                            payload: bytes::Bytes::from(resp.payload),
                        }
                    })
                };
                fetchgap_inflight_clone.fetch_sub(1, Ordering::AcqRel);
                match result {
                    Ok(resp) => {
                        if let Some(c) = &fetchgap_received_clone {
                            c.inc();
                        }
                        debug!(
                            group_id,
                            slot,
                            term = resp.term,
                            ballot_round = resp.ballot_round,
                            leader_id = resp.leader_id,
                            payload_len = resp.payload.len(),
                            "FetchGap reply received"
                        );
                        let entry = crate::paxos::roles::PxLogEntry {
                            slot: resp.slot,
                            ballot: crate::paxos::roles::PxBallot::new(resp.ballot_round, resp.leader_id),
                            term: resp.term,
                            payload: resp.payload,
                        };
                        acceptor_clone.accept(&entry).await;
                        learner_clone.update_chosen_frontier(resp.slot, resp.term);
                        apply_notify_clone.notify_one();
                    }
                    Err(err) => {
                        if let Some(c) = &fetchgap_failed_clone {
                            c.inc();
                        }
                        // Re-record the gap so the next tick retries.
                        gap_slots_clone.lock().insert(slot);
                        debug!(
                            group_id,
                            slot,
                            error = %err,
                            "FetchGap failed (will retry next tick)"
                        );
                    }
                }
            });
        }
        if sleep_or_cancel(&cancel, tick).await {
            return;
        }
    }
}

/// Returns `true` if cancelled during sleep, `false` if sleep completed.
async fn sleep_or_cancel(cancel: &CancellationToken, dur: Duration) -> bool {
    tokio::select! {
        () = cancel.cancelled() => true,
        () = tokio::time::sleep(dur) => false,
    }
}
