// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Replica management endpoints: list, add, remove, batch-add remote replicas.

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use tracing::{debug, info};

use crowdb_kv::cluster::group_election::LeaderElection;
use crowdb_kv::cluster::remote_replica::PxRemoteReplica;
use crowdb_protocol::mgmt::{RemoteListResponse, RemoteReplicaInfo, TopologyResponse};

use super::{err_json, ErrorResponse, RegistryArc};
use crate::group_rebuild::{rebuild_group_with_new_remotes, rebuild_group_with_same_config};

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
        s = sid,
        g = gid,
        count = remotes.len(),
        "adding remote replicas via management API"
    );
    for r in &remotes {
        debug!(
            s = sid,
            g = gid,
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
        s = sid,
        g = gid,
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
        s = sid,
        g = gid,
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
        s = sid,
        g = gid,
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
        info!(s = sid, g = gid, "batch add remotes: no new remotes to add");
        return Ok(StatusCode::OK);
    }

    debug!(
        s = sid,
        g = gid,
        count = new_remotes.len(),
        "batch adding remote replicas via management API"
    );
    for r in &new_remotes {
        debug!(
            s = sid,
            g = gid,
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
        s = sid,
        g = gid,
        count = new_remotes.len(),
        "batch remote replicas added via management API"
    );
    Ok(StatusCode::OK)
}
