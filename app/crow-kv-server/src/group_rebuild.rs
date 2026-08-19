// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Rebuild a `PxGroup` with new remote replicas.
//!
//! Shared by the management API (`mgmt/replica_ops.rs`) and the
//! group-0 reconcile fallback (`reconcile.rs`). Rebuilding — rather
//! than mutating a running group — is required because remote replicas
//! cannot be added to a live group without re-establishing the election
//! state consistently. [`rebuild_group_with_same_config`] inherits the
//! prior replica's election persistent state (`current_term`,
//! `voted_for`, `role`, `leader_id`, vote lockout) via
//! `PxLocalReplica::new_inheriting_election_state` so a rebuild does
//! not reset the cluster's election term and trigger a fresh race.

use crow_kv::cluster::group::PxGroup;
use crow_kv::cluster::group_election::LeaderElection;
use crow_kv::cluster::local_replica::PxLocalReplica;
use crow_kv::cluster::remote_replica::PxRemoteReplica;

/// Rebuild `group` with `new_remotes` merged into its remote list,
/// applying the membership-epoch bump correctly.
///
/// If `group` currently has **no** remotes at all — a freshly-loaded
/// replica whose `node-config.json` was missing (the reconcile fallback
/// case), or a freshly-joined replica's first-ever wiring — every entry
/// in `new_remotes` is folded in via the bulk, non-bumping
/// `set_remote_replicas`: bootstrapping to match an already-agreed
/// cluster state is not a membership *change*, and bumping here would
/// desync the epoch from peers who never bump for those adds.
///
/// Otherwise, each entry goes through the bump-aware
/// `add_remote_replica`, so only genuine voting-set changes (new
/// member, promotion, demotion) bump the epoch.
pub(crate) fn rebuild_group_with_new_remotes(
    group: &PxGroup,
    new_remotes: &[(u64, String, bool)],
) -> PxGroup {
    let mut new_group = rebuild_group_with_same_config(group);
    let existing = group.remote_replica_info();
    if existing.is_empty() {
        new_group.set_remote_replicas(
            new_remotes
                .iter()
                .map(|(id, endpoint, voting)| {
                    PxRemoteReplica::new(*id, endpoint.clone()).with_voting(*voting)
                })
                .collect(),
        );
    } else {
        new_group.set_remote_replicas(
            existing
                .into_iter()
                .map(|(id, endpoint, voting)| {
                    PxRemoteReplica::new(id, endpoint.to_string()).with_voting(voting)
                })
                .collect(),
        );
        for (id, endpoint, voting) in new_remotes {
            new_group.add_remote_replica(PxRemoteReplica::new(*id, endpoint.clone()).with_voting(*voting));
        }
    }
    new_group
}

/// Rebuild a `PxGroup` with the same config (`group_id`,
/// `local_replica`, `leader_id`, election state, membership epoch,
/// config stores) but no remote replicas. Caller re-adds remotes.
pub(crate) fn rebuild_group_with_same_config(group: &PxGroup) -> PxGroup {
    let lr = group.local_replica();
    let local_replica = PxLocalReplica::new_inheriting_election_state(lr);
    let mut new_group = PxGroup::new(group.group_id(), local_replica);
    new_group.set_from_config(group.config());
    new_group.stamp_proposing_term(group.proposing_term());
    new_group.set_membership_epoch(group.membership_epoch());
    if let Some(store) = group.config_store() {
        new_group.set_config_store(store.clone());
    }
    if let Some(node_store) = group.node_config_store() {
        let sid = group.node_config_store_sid().unwrap_or(0);
        new_group.set_node_config_store(node_store.clone(), sid, new_group.group_id());
    }
    new_group
}
