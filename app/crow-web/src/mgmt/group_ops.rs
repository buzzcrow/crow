// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! A6: Logical group plane — orchestrated group create/delete.

use crate::error::{err_400, err_409, err_500, err_502, ErrorBody};
use crate::expand::Recursive;
use crate::mgmt::{
    build_server_client, cluster_initialized, mgmt_url_for_node, refresh_node_cache, rpc_endpoint_for_node,
};
use crate::state::AppState;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use crow_console_shared::cluster::{GroupSummary, GroupView, NodeId};
use crow_console_shared::config::ReplicaEntry;
use crow_console_shared::mgmt::{AddGroupInitialRole, AddGroupRequest, RemoteReplicaInfo};
use serde::Deserialize;

/// `GET /api/stores/:store_id/groups`. List groups from cache.
///
/// # Errors
/// Returns `404` if the store is not found.
pub(crate) async fn http_list_groups(
    State(state): State<AppState>,
    Path(sid): Path<u64>,
    Recursive(_depth): Recursive,
) -> Result<Json<Vec<GroupSummary>>, (StatusCode, Json<ErrorBody>)> {
    let view = state.monitor_cache.resolve_store(sid).await.ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorBody {
                error: format!("store {sid} not found"),
            }),
        )
    })?;
    Ok(Json(view.groups))
}

#[derive(Debug, Deserialize)]
pub(crate) struct CreateGroupBody {
    pub group_id: u64,
    pub replica_id: u64,
    pub nodes: Vec<NodeId>,
}

/// `POST /api/stores/:store_id/groups`. Create a group across the listed
/// nodes. Orchestrated: creates a local `PxGroup` on each node and wires
/// remote-replica entries. Rolls back on partial failure.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns an error if the nodes list is empty or any upstream RPC fails.
pub(crate) async fn http_add_group(
    State(state): State<AppState>,
    Path(sid): Path<u64>,
    Json(body): Json<CreateGroupBody>,
) -> Result<(StatusCode, Json<serde_json::Value>), (StatusCode, Json<ErrorBody>)> {
    if sid != 0 && !cluster_initialized(&state).await {
        return Err(err_409(
            "cluster not initialized; call POST /api/cluster/init first",
        ));
    }
    if body.nodes.is_empty() {
        return Err(err_400("nodes list must not be empty"));
    }

    // Phase 1: create the group on each node.
    let mut succeeded: Vec<(NodeId, u64)> = Vec::new(); // (node_id, replica_id)
    let base_rid = body.replica_id;
    for (i, nid) in body.nodes.iter().enumerate() {
        let url = mgmt_url_for_node(&state, *nid)?;
        let client = build_server_client(url)?;
        let rid = base_rid + i as u64;
        let req = AddGroupRequest {
            group_id: body.group_id,
            replica_id: rid,
            initial_role: Some(if i == 0 {
                AddGroupInitialRole::Leader
            } else {
                AddGroupInitialRole::Follower
            }),
            // Multi-node groups defer the driver until Phase-2 wires remotes;
            // single-node groups start it now.
            start_election: Some(body.nodes.len() <= 1),
        };
        match client.add_group(sid, &req).await {
            Ok(()) => succeeded.push((*nid, rid)),
            Err(e) => {
                for (ok_nid, _) in &succeeded {
                    if let Ok(u) = mgmt_url_for_node(&state, *ok_nid) {
                        if let Ok(c) = build_server_client(u) {
                            let _ = c.remove_group(sid, body.group_id).await;
                        }
                    }
                }
                return Err(err_502(format!("group create failed on node {nid}: {e}")));
            }
        }
    }

    // Refresh the cache for each node before wiring remotes so the per-store
    // `listen_addr` (each `PxKvStore` binds its own port) is known to the
    // monitor cache used by `rpc_endpoint_for_node`.
    for (nid, _) in &succeeded {
        refresh_node_cache(&state, *nid).await;
    }

    // Phase 2: wire remote replicas. Each node gets every other node's
    // replica as a remote.
    for (i, (nid, _rid)) in succeeded.iter().enumerate() {
        let Ok(url) = mgmt_url_for_node(&state, *nid) else {
            continue;
        };
        let Ok(client) = build_server_client(url) else {
            continue;
        };
        let mut remotes: Vec<RemoteReplicaInfo> = Vec::new();
        for (j, (peer_nid, peer_rid)) in succeeded.iter().enumerate() {
            if j == i {
                continue;
            }
            if let Some(ep) = rpc_endpoint_for_node(&state, *peer_nid, sid).await {
                remotes.push(RemoteReplicaInfo {
                    replica_id: *peer_rid,
                    endpoint: ep,
                    voting: true,
                });
            }
        }
        if !remotes.is_empty() {
            let _ = client.add_remote_replicas(sid, body.group_id, &remotes).await;
        }
    }

    for (nid, _) in &succeeded {
        refresh_node_cache(&state, *nid).await;
    }

    {
        let mut cfg = state.config.write().unwrap();
        cfg.record_group(
            sid,
            body.group_id,
            succeeded
                .iter()
                .map(|(node_id, replica_id)| ReplicaEntry {
                    replica_id: *replica_id,
                    node_id: *node_id,
                })
                .collect(),
        );
        for (node_id, _) in &succeeded {
            cfg.ensure_store_node(sid, *node_id);
        }
    }
    state
        .persist()
        .map_err(|e| err_500(format!("persist config: {e}")))?;

    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({
            "store_id": sid,
            "group_id": body.group_id,
            "nodes": body.nodes,
        })),
    ))
}

/// `GET /api/stores/:store_id/groups/:group_id`. Aggregated group view.
///
/// # Errors
/// Returns `404` if the group is not found.
pub(crate) async fn http_get_group(
    State(state): State<AppState>,
    Path((sid, gid)): Path<(u64, u64)>,
    Recursive(_depth): Recursive,
) -> Result<Json<GroupView>, (StatusCode, Json<ErrorBody>)> {
    // Refresh the cache for every node currently believed to host this
    // store so role / leader info reflects the most recent topology
    // (elections happen asynchronously after the last write that
    // refreshed the cache; without this read-side refresh the response
    // would otherwise show stale `Follower` roles for the actual
    // leader). Bounded by the number of nodes hosting the store.
    let node_ids: Vec<NodeId> = {
        let snap = state.monitor_cache.snapshot().await;
        snap.iter()
            .filter_map(|(nid, rec)| {
                if rec.stores.contains_key(&sid) {
                    Some(*nid)
                } else {
                    None
                }
            })
            .collect()
    };
    for nid in &node_ids {
        refresh_node_cache(&state, *nid).await;
    }
    state
        .monitor_cache
        .resolve_group(sid, gid)
        .await
        .map(Json)
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ErrorBody {
                    error: format!("group {gid} in store {sid} not found"),
                }),
            )
        })
}

/// `DELETE /api/stores/:store_id/groups/:group_id`. Delete the group
/// across every hosting node.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns `404` if the group is not found in the cache.
pub(crate) async fn http_remove_group(
    State(state): State<AppState>,
    Path((sid, gid)): Path<(u64, u64)>,
) -> Result<StatusCode, (StatusCode, Json<ErrorBody>)> {
    if sid == 0 && gid == 0 {
        return Err(err_409(
            "group 0 in store 0 is the system group; use POST /api/cluster/reset to tear down the entire cluster",
        ));
    }
    let view = state.monitor_cache.resolve_group(sid, gid).await.ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorBody {
                error: format!("group {gid} in store {sid} not found"),
            }),
        )
    })?;
    let node_ids: Vec<NodeId> = view.replicas.iter().map(|r| r.node_id).collect();
    for nid in &node_ids {
        if let Ok(url) = mgmt_url_for_node(&state, *nid) {
            if let Ok(client) = build_server_client(url) {
                let _ = client.remove_group(sid, gid).await;
            }
        }
    }
    for nid in &node_ids {
        refresh_node_cache(&state, *nid).await;
    }
    {
        let mut cfg = state.config.write().unwrap();
        cfg.remove_group_record(sid, gid);
    }
    state
        .persist()
        .map_err(|e| err_500(format!("persist config: {e}")))?;
    Ok(StatusCode::NO_CONTENT)
}
