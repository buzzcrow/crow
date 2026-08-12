// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#![allow(clippy::cast_possible_truncation)]

use std::sync::atomic::Ordering;

use tokio::time::Duration;
use tokio_stream::StreamExt;
use tracing::{debug, error, info, warn};

use crate::cluster::group::{PxGroup, RemoteReplicaKind};
use crate::cluster::group_config::{PxGroupConfig, PxGroupMember};
use crate::cluster::local_replica::PxLocalReplica;
use crate::cluster::remote_replica::PxRemoteReplica;
use crate::cluster::replica::{Replica, StepDownReply, StepDownRequestPayload};
use crate::cluster::status::{GroupStatus, InflightStatus, ReadStateView, StatusLevel};
use crate::common::report::OperationReport;
use crate::paxos::roles::SlotIndex;
use crate::paxos::PxNodeId;

impl PxGroup {
    /// Seed the membership epoch from a persisted/prior value, without
    /// going through the bump logic in [`Self::add_remote_replica`] /
    /// [`Self::remove_remote_replica`]. Used by restart-from-disk restore
    /// (`apply_config`) and by the management-API rebuild pattern that
    /// reconstructs a fresh `PxGroup` and replays its prior remotes.
    pub fn set_membership_epoch(&self, epoch: u64) {
        self.membership_epoch.store(epoch, Ordering::Release);
    }

    /// Adopt `epoch` if it is higher than the current membership epoch,
    /// using a compare-and-swap loop so concurrent callers don't clobber
    /// each other. Returns the epoch now in effect.
    ///
    /// This is the "refresh its membership view" action the proto comment
    /// on `epoch_mismatch` prescribes: when a peer reports a higher epoch
    /// (because it observed a voting-set change we haven't seen yet), we
    /// converge upward so the next Prepare/Accept matches. We never lower
    /// the epoch — a stale proposer must not drag an acceptor backwards.
    pub fn adopt_membership_epoch(&self, epoch: u64) -> u64 {
        let mut current = self.membership_epoch.load(Ordering::Acquire);
        while epoch > current {
            match self
                .membership_epoch
                .compare_exchange(current, epoch, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => {
                    info!(
                        group_id = self.group_id,
                        old_epoch = current,
                        new_epoch = epoch,
                        "membership epoch adopted from peer (converging upward)"
                    );
                    return epoch;
                }
                Err(actual) => current = actual,
            }
        }
        current
    }

    /// Bump the membership epoch by exactly 1. Called from
    /// [`Self::add_remote_replica`] / [`Self::remove_remote_replica`]
    /// only when the mutation actually changes the *voting set* --
    /// see the field doc on `membership_epoch` for why non-voting
    /// changes must not bump it.
    pub(crate) fn bump_membership_epoch(&self) {
        let new_epoch = self.membership_epoch.fetch_add(1, Ordering::AcqRel) + 1;
        info!(
            group_id = self.group_id,
            new_epoch, "membership epoch bumped (voting set changed)"
        );
    }

    /// Ask the local replica to step down as leader, if it currently is
    /// one. Self-targeted and self-termed: reads the replica's own live
    /// role/term rather than trusting an external caller's snapshot of
    /// them (which could be stale by the time the call lands), so no
    /// term needs to be threaded through from outside this process.
    /// Delegates to the strict-fence `handle_step_down` — a no-op
    /// (`accepted: false`) if this replica is not currently leader.
    pub fn step_down_if_leader(&self, reason: &str) -> StepDownReply {
        let replica = self.local_replica();
        let term = replica.current_term_snapshot();
        replica.handle_step_down(&StepDownRequestPayload {
            term,
            target_leader_id: replica.id,
            reason: reason.to_string(),
        })
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

    /// Record a voting peer's reported `durable_snapshot_slot` and recompute
    /// the group's real "durable on leader + >=1 peer" snapshot watermark
    /// (`snapshot_slot`
    /// as `min(local durable_snapshot_slot, max(voting peer
    /// durable_snapshot_slot))`. A peer that has never reported is treated
    /// as `0` and simply never wins the max (same "absent = 0" convention as
    /// `note_peer_applied`) — unlike `group_safe_slot`, only *one* peer
    /// beyond the leader needs to have a slot durable, so the
    /// furthest-along peer alone is always a sufficient witness; a
    /// straggler peer must not hold this watermark back. Called from the
    /// leader heartbeat round, alongside `note_peer_applied`.
    pub(crate) fn note_peer_durable(&self, peer_id: PxNodeId, durable: SlotIndex) {
        let mut peers = self.peer_durable.lock();
        peers.insert(peer_id, durable);
        let mut best_peer = 0;
        for remote in &self.remote_replicas {
            if let RemoteReplicaKind::Real(r) = remote {
                if r.voting {
                    let peer_durable = peers.get(&r.node_id).copied().unwrap_or(0);
                    best_peer = best_peer.max(peer_durable);
                }
            }
        }
        drop(peers);
        let local_durable = self.local_replica.wal().map_or(0, |w| w.snapshot_slot());
        let snapshot = local_durable.min(best_peer);
        // Monotonic within a tenure, same rationale as group_safe_slot.
        self.group_snapshot_slot.fetch_max(snapshot, Ordering::AcqRel);
    }

    /// Clear all peer-applied/-durable tracking and reset the published
    /// group safe-slot and snapshot-slot to `0`. Called at the start of
    /// every leader tenure: both watermarks only ever advance (via
    /// `fetch_max`) *within* a tenure, so without this reset a freshly
    /// elected leader would inherit the previous tenure's elevated
    /// watermarks and stale per-peer state — overstating freshness for
    /// bounded-stale reads / GC eligibility until new heartbeats arrive.
    /// After the reset both watermarks stay at `0` until heartbeats
    /// re-establish them, which is the conservative guarantee their docs
    /// promise.
    pub(crate) fn reset_safe_slot_tracking(&self) {
        self.peer_applied.lock().clear();
        self.group_safe_slot.store(0, Ordering::Release);
        self.peer_durable.lock().clear();
        self.group_snapshot_slot.store(0, Ordering::Release);
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
        self.config.force_classic = force;
    }

    pub fn inherit_local_state_from(&mut self, prior: &Self) {
        self.local_replica = PxLocalReplica::new_inheriting_election_state(prior.local_replica());
        self.next_slot.store(
            self.next_slot
                .load(Ordering::Acquire)
                .max(prior.next_slot.load(Ordering::Acquire)),
            Ordering::Release,
        );
        self.proposing_term
            .store(prior.proposing_term.load(Ordering::Acquire), Ordering::Release);
        self.leader_read_ready
            .store(prior.leader_read_ready(), Ordering::Release);
        self.group_safe_slot
            .store(prior.group_safe_slot(), Ordering::Release);
    }

    pub fn set_next_slot(&self, next_slot: SlotIndex) {
        self.next_slot.store(next_slot.max(1), Ordering::Release);
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
                self.remote_replicas[idx] = RemoteReplicaKind::Real(Box::new(remote));
                self.valid_replica_count += 1;
            }
        }
        self.recompute_quorum();
    }

    /// Replace the endpoint of a remote replica, inserting it if the slot is
    /// a `Placeholder` or does not yet exist (upsert semantics).
    ///
    /// Returns the previous endpoint string when an existing `Real` replica
    /// was updated, or `None` when a new entry was inserted (from a
    /// `Placeholder` or by extending the vec).
    pub fn update_member_endpoint(
        &mut self,
        node_id: PxNodeId,
        endpoint: impl Into<String>,
    ) -> Option<String> {
        let endpoint = endpoint.into();
        let idx = node_id as usize;

        // Existing Real entry: update in place.
        if let Some(RemoteReplicaKind::Real(remote)) = self.remote_replicas.get_mut(idx) {
            let old_endpoint = remote.endpoint.clone();
            endpoint.clone_into(&mut remote.endpoint);
            return Some(old_endpoint);
        }

        // Placeholder or out-of-range: insert a new Real entry.
        warn!(
            group_id = self.group_id,
            node_id, "update_member_endpoint: inserting new remote (was placeholder or out-of-range)"
        );
        while idx >= self.remote_replicas.len() {
            self.remote_replicas.push(RemoteReplicaKind::Placeholder);
        }
        self.remote_replicas[idx] =
            RemoteReplicaKind::Real(Box::new(PxRemoteReplica::new(node_id, endpoint)));
        self.valid_replica_count = self
            .remote_replicas
            .iter()
            .filter(|r| matches!(r, RemoteReplicaKind::Real(_)))
            .count();
        self.recompute_quorum();
        None
    }

    /// Apply a persisted group configuration, wiring remote replicas from the
    /// durable config snapshot. The local replica is skipped.
    ///
    /// This is the restore-window seed (W9): a restarted node that loads a
    /// persisted config file starts with the same intended membership it had
    /// before the crash, instead of starting as a `quorum=1` singleton.
    pub fn apply_config(&mut self, config: &PxGroupConfig) {
        let local_id = self.local_replica().id;
        let remotes: Vec<PxRemoteReplica> = config
            .members
            .iter()
            .filter(|m| m.replica_id != local_id)
            .map(|m| PxRemoteReplica::new(m.replica_id, m.endpoint.clone()).with_voting(m.voting))
            .collect();
        self.set_remote_replicas(remotes);
        self.set_membership_epoch(config.membership_epoch);
        debug!(
            group_id = self.group_id,
            local_id,
            member_count = config.members.len(),
            membership_epoch = config.membership_epoch,
            "applied persisted group config"
        );
    }

    /// Persist the current group membership to a dedicated config file.
    ///
    /// Writes the local replica plus every real remote replica. Non-fatal on
    /// error: the group continues running but logs the failure.
    pub async fn persist_config(&self) {
        let local_id = self.local_replica().id;
        let term = self.local_replica().current_term_snapshot();
        let mut members = Vec::new();
        // Local replica's endpoint is set by the store when the group is
        // added, so all nodes share the same member list in the config file.
        members.push(PxGroupMember {
            replica_id: local_id,
            endpoint: self.local_replica().get_endpoint().unwrap_or_default(),
            voting: self.local_replica().voting(),
        });
        for remote in self.remote_replicas.iter().filter_map(|r| r.as_real()) {
            members.push(PxGroupMember {
                replica_id: remote.node_id,
                endpoint: remote.endpoint.clone(),
                voting: remote.voting,
            });
        }
        let config = PxGroupConfig {
            group_id: self.group_id,
            term,
            members,
            membership_epoch: self.membership_epoch(),
        };
        if let Some((store, sid, _gid)) = &self.node_config_store {
            if let Err(e) = store.save_group(*sid, &config, local_id).await {
                error!(
                    group_id = self.group_id,
                    term,
                    error = %e,
                    "persist group config to node-config.json failed"
                );
            } else {
                info!(
                    group_id = self.group_id,
                    term,
                    replica_count = config.members.len(),
                    "persisted group config to node-config.json"
                );
            }
        } else if let Some(store) = &self.config_store {
            if let Err(e) = store.save(&config).await {
                error!(
                    group_id = self.group_id,
                    term,
                    error = %e,
                    "persist group config failed"
                );
            } else {
                info!(
                    group_id = self.group_id,
                    term,
                    replica_count = config.members.len(),
                    "persisted group config to file"
                );
            }
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

        let read_state = self.read_handles.get().map(|h| ReadStateView {
            lease_valid: h.lease_valid.snapshot(),
            contiguous_applied: h.contiguous_applied.snapshot(),
            safe_slot: h.safe_slot.snapshot(),
        });

        GroupStatus {
            group_id: self.group_id,
            leader_id: self.leader_id(),
            local_replica_id: local_replica.id,
            force_classic: self.config.force_classic,
            status,
            messages,
            local_replica,
            remotes,
            inflight: Some(InflightStatus {
                window: self.inflight.window,
                policy: self.inflight.policy.label().to_string(),
                occupied: self.inflight.occupied(),
                waiting: self.inflight.waiting.load(Ordering::Relaxed),
                total_enqueued: self.inflight.total_enqueued.load(Ordering::Relaxed),
                total_wait_us: self.inflight.total_wait_us.load(Ordering::Relaxed),
            }),
            read_state,
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

        // 0. Cancel the per-tenure token and await the election driver +
        //    engine maintenance loop (both share this same cancel source).
        //    Both are cooperative; a 100 ms scaffold tick is well within
        //    `per_layer_timeout`.
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
        if let Some(handle) = self.maintenance_handle.lock().await.take() {
            match tokio::time::timeout(per_layer_timeout, handle).await {
                Ok(Ok(())) => {}
                Ok(Err(join_err)) => {
                    warn!(
                        group_id = self.group_id,
                        error = %join_err,
                        "engine maintenance task panicked during shutdown"
                    );
                }
                Err(_) => {
                    warn!(
                        group_id = self.group_id,
                        timeout_ms = per_layer_timeout.as_millis() as u64,
                        "engine maintenance task did not exit within per-layer timeout"
                    );
                }
            }
        }
        // R65: stop the FetchGap driver.
        if let Some(handle) = self.fetchgap_handle.lock().await.take() {
            let _ = handle.await;
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

    // ── New-member snapshot join ────────────────────

    /// New-member snapshot join: pull a snapshot for this group from
    /// `peer_endpoint`'s [`crate::rpc::SnapshotService`], import it into
    /// the local engine, and seed the local learner's frontier so the
    /// group's normal repair/heartbeat catch-up only needs to stream the
    /// WAL tail above the snapshot's `at_slot` -- instead of replaying full
    /// Paxos history from slot 1.
    ///
    /// **Precondition:** must be called before this replica is wired into
    /// any group's topology (before any peer can send it `Accept`/
    /// `Heartbeat` RPCs) -- mirrors [`crate::paxos::learner::PxLearner::seed_resume_frontier`]'s
    /// "before any `learn` call" precondition. Intended for a
    /// freshly-constructed, still-empty local replica only; never call this
    /// on a replica with existing local state.
    ///
    /// Returns the snapshot's `at_slot` on success (the frontier this
    /// replica's learner was seeded to).
    ///
    /// # Errors
    /// Returns an error string on any transport, decode, or engine-import
    /// failure.
    pub async fn join_via_snapshot(&self, peer_endpoint: &str) -> Result<u64, String> {
        use crate::rpc::snapshot_service_client::SnapshotServiceClient;
        use crate::rpc::{snapshot_stream_item, SnapshotRequest};

        let mut client = SnapshotServiceClient::connect(format!("http://{peer_endpoint}"))
            .await
            .map_err(|e| format!("snapshot join: connect to {peer_endpoint} failed: {e}"))?;

        let mut stream = client
            .stream_snapshot(SnapshotRequest {
                group_id: self.group_id,
            })
            .await
            .map_err(|e| format!("snapshot join: StreamSnapshot rpc failed: {e}"))?
            .into_inner();

        let mut header: Option<(u64, u64)> = None;
        let mut bytes = Vec::new();
        while let Some(item) = stream.next().await {
            let item = item.map_err(|e| format!("snapshot join: stream error: {e}"))?;
            match item.payload {
                Some(snapshot_stream_item::Payload::Header(h)) => {
                    header = Some((h.term_at_slot, h.membership_epoch));
                }
                Some(snapshot_stream_item::Payload::Data(chunk)) => {
                    bytes.extend_from_slice(&chunk);
                }
                None => {}
            }
        }
        let (term_at_slot, membership_epoch) =
            header.ok_or_else(|| "snapshot join: stream ended without a header".to_string())?;

        let at_slot = self
            .local_replica
            .learner
            .engine()
            .snapshot_import(&bytes)
            .map_err(|e| format!("snapshot join: engine import failed: {e}"))?;

        info!(
            group_id = self.group_id,
            peer_endpoint,
            at_slot,
            term_at_slot,
            membership_epoch,
            stream_bytes = bytes.len(),
            "snapshot join: imported snapshot, seeding learner frontier"
        );
        self.local_replica
            .learner
            .seed_resume_frontier(at_slot, term_at_slot);
        // Seed the epoch fence from the exporting peer's current value --
        // without this, a fresh join always starts at epoch 0 and could
        // never receive a Prepare/Accept (even as a non-voting catch-up
        // learner) from a group that has ever had a membership change.
        self.set_membership_epoch(membership_epoch);
        Ok(at_slot)
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

        // Snapshot whether this node_id was already a voting member,
        // to decide below whether this mutation changes the voting set.
        let was_voting = matches!(&self.remote_replicas[idx], RemoteReplicaKind::Real(prev) if prev.voting);
        let new_voting = remote.voting;

        // Check if this was a placeholder before
        if matches!(self.remote_replicas[idx], RemoteReplicaKind::Placeholder) {
            self.valid_replica_count += 1;
        }

        self.remote_replicas[idx] = RemoteReplicaKind::Real(Box::new(remote));
        self.recompute_quorum();

        // Bump the epoch iff this mutation actually changes the voting
        // set (new voting member, promotion, demotion) -- not for a
        // plain non-voting add/re-add or an idempotent re-add at the
        // same voting status (e.g. an endpoint-only update).
        if was_voting != new_voting {
            self.bump_membership_epoch();
        }
    }

    /// Remove a remote replica by node ID. Returns true if it was present.
    pub fn remove_remote_replica(&mut self, node_id: PxNodeId) -> bool {
        let idx = node_id as usize;
        let was_voting = match self.remote_replicas.get(idx) {
            Some(RemoteReplicaKind::Real(prev)) => prev.voting,
            _ => return false,
        };
        info!(
            group_id = self.group_id,
            remote_id = node_id,
            "removed remote replica from group"
        );
        self.remote_replicas[idx] = RemoteReplicaKind::Placeholder;
        self.valid_replica_count -= 1;
        self.recompute_quorum();
        // Removing a non-voting member never changes the voting set.
        if was_voting {
            self.bump_membership_epoch();
        }
        true
    }

    /// Return info about all real remote replicas: `(node_id, endpoint,
    /// voting)`. Callers that rebuild a group (management-API remote
    /// add/remove) must carry `voting` through to the rebuilt
    /// [`PxRemoteReplica`] -- dropping it would silently re-promote a
    /// non-voting (e.g. still-catching-up
    /// remote back to voting on the next unrelated rebuild.
    pub fn remote_replica_info(&self) -> Vec<(PxNodeId, &str, bool)> {
        self.remote_replicas
            .iter()
            .filter_map(|r| match r {
                RemoteReplicaKind::Real(remote) => {
                    Some((remote.node_id, remote.endpoint.as_str(), remote.voting))
                }
                RemoteReplicaKind::Placeholder => None,
            })
            .collect()
    }
}
