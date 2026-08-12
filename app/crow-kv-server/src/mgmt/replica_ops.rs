// Copyright 2026-present buzzcrow <buzzcrow::126.com>
// Licensed under the Apache License, Version 2.0.

//! Replica management endpoints: list, add, remove, batch-add remote replicas.

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use tracing::{debug, info};

use crow_kv::cluster::group::PxGroup;
use crow_kv::cluster::group_election::LeaderElection;
use crow_kv::cluster::local_replica::PxLocalReplica;
use crow_kv::cluster::remote_replica::PxRemoteReplica;
use crow_protocol::mgmt::{RemoteListResponse, RemoteReplicaInfo, TopologyResponse};

use super::{err_json, ErrorResponse, RegistryArc};

/// Rebuild `group` with `new_remotes` merged into its remote list,
/// applying the membership-epoch bump correctly.
/// Shared by `add_remote_replicas` and
/// `batch_add_remote_replicas` -- both need the exact same bootstrap
/// handling, and having it live in one place (rather than duplicated
/// per-handler) is what actually keeps them consistent; a previous
/// version of this fix only special-cased one of the two call sites and
/// broke a real multi-node test that fans out through the other.
///
/// Existing remotes are carried over via the bulk, non-bumping
/// `set_remote_replicas` -- never through the bump-aware
/// `add_remote_replica`, which would treat every replay of an
/// already-known member as a fresh voting-set change and bump once per
/// *existing* member on every single mutation call.
///
/// If `group` currently has **no** remotes at all -- a freshly-joined
/// replica's first-ever wiring (`join_group_via_snapshot`'s step 1:
/// "wire the group's existing members as this replica's remotes") --
/// every entry in `new_remotes` is folded into that same bulk seed
/// instead of going through `add_remote_replica`: bootstrapping a brand
/// new replica to match an already-agreed cluster state is not a
/// membership *change*, no matter what `voting` flags it carries, and
/// bumping here would desync its epoch from peers who never bump for a
/// non-voting add of that same replica. This only checks "is the
/// **target's own** remote list currently empty", so the caller is
/// responsible for landing a freshly-joined replica's entire bootstrap
/// wiring in one call (as `crow-console/web/src/mgmt.rs::http_add_replica`
/// already does) -- splitting it into several single-entry calls would
/// only protect the first one.
///
/// Otherwise, each entry in `new_remotes` goes through the bump-aware
/// `add_remote_replica`, so only genuine voting-set changes (new
/// member, promotion, demotion) bump the epoch.
fn rebuild_group_with_new_remotes(group: &PxGroup, new_remotes: &[(u64, String, bool)]) -> PxGroup {
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

/// Rebuild a `PxGroup` with the same config (`group_id`, `local_replica`,
/// `leader_id`, `force_classic`, `election_cfg`) but no remote replicas. Caller is
/// responsible for re-adding remote replicas.
///
/// **Important:** the new `PxLocalReplica` inherits the prior replica's
/// election persistent state (`current_term`, `voted_for`, `role`,
/// `leader_id`, `vote_lockout_until`) via
/// [`PxLocalReplica::new_inheriting_election_state`]. Without this, every
/// `add_remote_replicas` / `remove_remote_replica` rebuild would reset the cluster's
/// election term back to 0 and trigger a fresh election round, which
/// prevents leadership from converging when a multi-replica group is
/// being built up incrementally (each remote-add kills the elected
/// leader and starts a new race).
fn rebuild_group_with_same_config(group: &PxGroup) -> PxGroup {
    let lr = group.local_replica();
    let local_replica = PxLocalReplica::new_inheriting_election_state(lr);
    let mut new_group = PxGroup::new(group.group_id, local_replica);
    // `new_inheriting_election_state` already copies a consistent
    // (role, leader_id) snapshot from the prior replica under the mutex,
    // so `set_leader_id` is redundant here. The `role_atomic` and
    // `believed_leader_id` on the new replica already match.
    // Carry the unified config wholesale — replaces the former per-flag carry blocks.
    new_group.set_from_config(group.config());
    // Preserve `proposing_term` so the new group's leadership gate
    // (`role == Leader && current_term == proposing_term`) passes for
    // an already-elected leader that didn't have to re-stamp the term
    // on the rebuild path. Otherwise the gate fails with `NotLeader`
    // even though the replica is still Leader, because the fresh
    // `PxGroup` starts with `proposing_term = 0`.
    new_group.stamp_proposing_term(group.proposing_term());
    // Carry the membership epoch forward. Without this, every rebuild
    // (every add/remove/promote call) would silently reset it to 0 and
    // re-bump from there, making the epoch reflect only "did the last
    // mutation change the voting set" instead of a true count across
    // the group's whole history -- defeating the exact-match fence the
    // very next time two mutations land close together.
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

#[utoipa::path(
        get,
        path = "/stores/{sid}/groups/{gid}/remotes",
        tag = "management",
        params(
            ("sid" = u64, Path, description = "Store id"),
            ("gid" = u64, Path, description = "Group id")
        ),
        responses(
            (status = 200, description = "Remote replicas", body = RemoteListResponse),
            (status = 404, description = "Store or group not found", body = ErrorResponse)
        )
    )]
pub(super) async fn list_remote_replicas(
    State(state): State<RegistryArc>,
    Path((sid, gid)): Path<(u64, u64)>,
) -> Result<Json<RemoteListResponse>, (StatusCode, Json<ErrorResponse>)> {
    let store = state
        .get_store(sid)
        .ok_or_else(|| err_json(StatusCode::NOT_FOUND, format!("store {sid} not found")))?;
    let group = store.get_group(gid).ok_or_else(|| {
        err_json(
            StatusCode::NOT_FOUND,
            format!("group {gid} not found in store {sid}"),
        )
    })?;

    let remotes: Vec<RemoteReplicaInfo> = group
        .remote_replica_info()
        .into_iter()
        .map(|(id, endpoint, voting)| RemoteReplicaInfo {
            replica_id: id,
            endpoint: endpoint.to_string(),
            voting,
        })
        .collect();

    Ok(Json(RemoteListResponse { remotes }))
}

#[utoipa::path(
        post,
        path = "/stores/{sid}/groups/{gid}/remotes",
        tag = "management",
        params(
            ("sid" = u64, Path, description = "Store id"),
            ("gid" = u64, Path, description = "Group id")
        ),
        request_body = Vec<RemoteReplicaInfo>,
        responses(
            (status = 200, description = "Remote replicas added"),
            (status = 400, description = "Invalid remote replica", body = ErrorResponse),
            (status = 404, description = "Store or group not found", body = ErrorResponse)
        )
    )]
pub(super) async fn add_remote_replicas(
    State(state): State<RegistryArc>,
    Path((sid, gid)): Path<(u64, u64)>,
    Json(remotes): Json<Vec<RemoteReplicaInfo>>,
) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
    let store = state
        .get_store(sid)
        .ok_or_else(|| err_json(StatusCode::NOT_FOUND, format!("store {sid} not found")))?;
    let group = store.get_group(gid).ok_or_else(|| {
        err_json(
            StatusCode::NOT_FOUND,
            format!("group {gid} not found in store {sid}"),
        )
    })?;

    let local_id = group.local_replica().id;
    for r in &remotes {
        if r.replica_id == local_id {
            return Err(err_json(
                StatusCode::BAD_REQUEST,
                format!(
                    "cannot add local replica {} as remote; local replicas are managed with the group",
                    r.replica_id
                ),
            ));
        }
    }

    debug!(
        store_id = sid,
        group_id = gid,
        count = remotes.len(),
        "adding remote replicas via management API"
    );
    for r in &remotes {
        debug!(
            store_id = sid,
            group_id = gid,
            remote_id = r.replica_id,
            endpoint = %r.endpoint,
            voting = r.voting,
            "adding remote replica"
        );
    }
    let new_remotes: Vec<(u64, String, bool)> = remotes
        .iter()
        .map(|r| (r.replica_id, r.endpoint.clone(), r.voting))
        .collect();
    let new_group = rebuild_group_with_new_remotes(&group, &new_remotes);
    store.add_group(new_group);
    // Re-persist after add_group so the local replica's endpoint is set.
    if let Some(g) = store.get_group(gid) {
        g.persist_config().await;
    }

    info!(
        store_id = sid,
        group_id = gid,
        count = remotes.len(),
        "remote replicas added via management API"
    );
    Ok(StatusCode::OK)
}

#[utoipa::path(
        delete,
        path = "/stores/{sid}/groups/{gid}/remotes/{rid}",
        tag = "management",
        params(
            ("sid" = u64, Path, description = "Store id"),
            ("gid" = u64, Path, description = "Group id"),
            ("rid" = u64, Path, description = "Remote replica id")
        ),
        responses(
            (status = 200, description = "Remote replica removed"),
            (status = 400, description = "Local replica cannot be removed as remote", body = ErrorResponse),
            (status = 404, description = "Store, group, or remote replica not found", body = ErrorResponse)
        )
    )]
pub(super) async fn remove_remote_replica(
    State(state): State<RegistryArc>,
    Path((sid, gid, rid)): Path<(u64, u64, u64)>,
) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
    let store = state
        .get_store(sid)
        .ok_or_else(|| err_json(StatusCode::NOT_FOUND, format!("store {sid} not found")))?;
    let group = store.get_group(gid).ok_or_else(|| {
        err_json(
            StatusCode::NOT_FOUND,
            format!("group {gid} not found in store {sid}"),
        )
    })?;

    let local_id = group.local_replica().id;
    if rid == local_id {
        return Err(err_json(
            StatusCode::BAD_REQUEST,
            "cannot remove local replica; local replicas are managed with the group",
        ));
    }

    // Check if remote exists
    let exists = group.remote_replica_info().iter().any(|(id, _, _)| *id == rid);
    if !exists {
        return Err(err_json(
            StatusCode::NOT_FOUND,
            format!("remote replica {rid} not found in group {gid}"),
        ));
    }

    info!(
        store_id = sid,
        group_id = gid,
        remote_id = rid,
        "removing remote replica via management API"
    );
    // Reconstruct group without this remote, preserving voting flags.
    // Carry every existing remote over verbatim (bulk, non-bumping
    // `set_remote_replicas`, including `rid` itself for now), then remove
    // `rid` through the bump-aware `remove_remote_replica` -- see the
    // matching comment in `add_remote_replicas` for why a loop of
    // `add_remote_replica` calls over the *surviving* members would bump
    // the epoch once per survivor instead of once for the actual removal.
    let mut new_group = rebuild_group_with_same_config(&group);
    new_group.set_remote_replicas(
        group
            .remote_replica_info()
            .into_iter()
            .map(|(id, endpoint, voting)| PxRemoteReplica::new(id, endpoint.to_string()).with_voting(voting))
            .collect(),
    );
    new_group.remove_remote_replica(rid);
    let current_term = group.local_replica().current_term_snapshot();
    if new_group.quorum() == 1 {
        new_group.local_replica().become_leader();
        new_group.local_replica().persist_current_vote().await;
        new_group.stamp_proposing_term(current_term);
    } else if group.leader_id() == rid {
        new_group.local_replica().become_follower(current_term);
        new_group.local_replica().clear_vote_lockout();
    }
    store.add_group(new_group);
    // Re-persist after add_group so the local replica's endpoint is set.
    if let Some(g) = store.get_group(gid) {
        g.persist_config().await;
    }

    info!(
        store_id = sid,
        group_id = gid,
        remote_id = rid,
        "remote replica removed via management API"
    );
    Ok(StatusCode::OK)
}

#[utoipa::path(
        post,
        path = "/stores/{sid}/groups/{gid}/remotes/batch",
        tag = "management",
        params(
            ("sid" = u64, Path, description = "Store id"),
            ("gid" = u64, Path, description = "Group id")
        ),
        request_body = TopologyResponse,
        responses(
            (status = 200, description = "Remote replicas added from topology"),
            (status = 404, description = "Store or group not found", body = ErrorResponse)
        )
    )]
pub(super) async fn batch_add_remote_replicas(
    State(state): State<RegistryArc>,
    Path((sid, gid)): Path<(u64, u64)>,
    Json(topology): Json<TopologyResponse>,
) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
    let store = state
        .get_store(sid)
        .ok_or_else(|| err_json(StatusCode::NOT_FOUND, format!("store {sid} not found")))?;
    let group = store.get_group(gid).ok_or_else(|| {
        err_json(
            StatusCode::NOT_FOUND,
            format!("group {gid} not found in store {sid}"),
        )
    })?;

    let local_id = group.local_replica().id;
    let mut new_remotes = Vec::new();

    for topo_store in &topology.stores {
        let Some(addr) = topo_store.listen_addr.as_ref() else {
            continue;
        };
        let addr = addr.clone();
        for topo_group in &topo_store.groups {
            if topo_group.group_id == gid && topo_group.local_replica_id != local_id {
                new_remotes.push(RemoteReplicaInfo {
                    replica_id: topo_group.local_replica_id,
                    endpoint: addr.clone(),
                    voting: true,
                });
            }
        }
    }

    if new_remotes.is_empty() {
        info!(
            store_id = sid,
            group_id = gid,
            "batch add remotes: no new remotes to add"
        );
        return Ok(StatusCode::OK);
    }

    debug!(
        store_id = sid,
        group_id = gid,
        count = new_remotes.len(),
        "batch adding remote replicas via management API"
    );
    for r in &new_remotes {
        debug!(
            store_id = sid,
            group_id = gid,
            remote_id = r.replica_id,
            endpoint = %r.endpoint,
            voting = r.voting,
            "batch adding remote replica"
        );
    }
    let remotes_tuple: Vec<(u64, String, bool)> = new_remotes
        .iter()
        .map(|r| (r.replica_id, r.endpoint.clone(), r.voting))
        .collect();
    let new_group = rebuild_group_with_new_remotes(&group, &remotes_tuple);
    store.add_group(new_group);
    // Re-persist after add_group so the local replica's endpoint is set.
    if let Some(g) = store.get_group(gid) {
        g.persist_config().await;
    }

    info!(
        store_id = sid,
        group_id = gid,
        count = new_remotes.len(),
        "batch remote replicas added via management API"
    );
    Ok(StatusCode::OK)
}
