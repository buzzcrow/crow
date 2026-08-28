// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! A7: Logical replica plane — add/remove with bidirectional wiring + rollback.

use crate::error::{err_500, err_502, ErrorBody};
use crate::expand::Recursive;
use crate::mgmt::{
    build_server_client, mgmt_url_for_node, refresh_node_cache, rpc_endpoint_for_node, wait_for_new_leader,
};
use crate::state::AppState;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use crow_console_shared::cluster::{NodeId, ReplicaView};
use crow_console_shared::config::ReplicaEntry;
use crow_console_shared::mgmt::{
    AddGroupInitialRole, AddGroupRequest, AddStoreRequest, RemoteReplicaInfo, StepDownRequest,
};
use serde::Deserialize;
use std::time::Duration;
use tracing::warn;

/// Roll back a partially-wired replica: deregister from `peer_nodes`,
/// then delete the local group on `target_node`. If
/// `created_store_on_target` is true, also drop the store on
/// `target_node` (we created it atomically in step 1).
async fn rollback_replica(
    state: &AppState,
    sid: u64,
    gid: u64,
    rid: u64,
    peer_nodes: &[NodeId],
    target_node: NodeId,
    created_store_on_target: bool,
) {
    for nid in peer_nodes {
        if let Ok(u) = mgmt_url_for_node(state, *nid) {
            if let Ok(c) = build_server_client(u) {
                let _ = c.remove_remote_replica(sid, gid, rid).await;
            }
        }
    }
    if let Ok(u) = mgmt_url_for_node(state, target_node) {
        if let Ok(c) = build_server_client(u) {
            if created_store_on_target {
                // `remove_store` cascades the group on that node.
                let _ = c.remove_store(sid).await;
            } else {
                let _ = c.remove_group(sid, gid).await;
            }
        }
    }
}

/// `GET /api/stores/:s/groups/:g/replicas`. Unified replica list from
/// the monitor cache.
///
/// # Errors
/// Returns `404` if the group is not found.
pub(crate) async fn http_list_replicas(
    State(state): State<AppState>,
    Path((sid, gid)): Path<(u64, u64)>,
    Recursive(_depth): Recursive,
) -> Result<Json<Vec<ReplicaView>>, (StatusCode, Json<ErrorBody>)> {
    let view = state.monitor_cache.resolve_group(sid, gid).await.ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorBody {
                error: format!("group {gid} in store {sid} not found"),
            }),
        )
    })?;
    Ok(Json(view.replicas))
}

/// `GET /api/stores/:s/groups/:g/replicas/:rid`. Single replica detail
/// (logical view).
///
/// # Errors
/// Returns `404` if the group or replica is not found.
pub(crate) async fn http_get_replica(
    State(state): State<AppState>,
    Path((sid, gid, rid)): Path<(u64, u64, u64)>,
    Recursive(_depth): Recursive,
) -> Result<Json<ReplicaView>, (StatusCode, Json<ErrorBody>)> {
    let view = state.monitor_cache.resolve_group(sid, gid).await.ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorBody {
                error: format!("group {gid} in store {sid} not found"),
            }),
        )
    })?;
    let replica = view
        .replicas
        .iter()
        .find(|r| r.replica_id == rid)
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ErrorBody {
                    error: format!("replica {rid} not found in group {gid}"),
                }),
            )
        })?;
    Ok(Json(replica.clone()))
}

#[derive(Debug, Deserialize)]
pub(crate) struct AddReplicaBody {
    pub node_id: NodeId,
    #[serde(default)]
    pub replica_id: Option<u64>,
}

/// `POST /api/stores/:s/groups/:g/replicas`. Add a replica to an
/// existing group. Orchestrated per §6.4.3:
///
/// 1. Create a local `PxGroup` on the target `node_id`.
/// 2. Register the new replica as a remote on every existing peer.
/// 3. Register every existing peer as a remote on the new replica.
/// 4. On any step-2/3 failure, deregister and delete to roll back.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns an error if the group doesn't exist, the node has no server,
/// or any upstream RPC fails.
#[allow(clippy::too_many_lines)]
pub(crate) async fn http_add_replica(
    State(state): State<AppState>,
    Path((sid, gid)): Path<(u64, u64)>,
    Json(body): Json<AddReplicaBody>,
) -> Result<(StatusCode, Json<serde_json::Value>), (StatusCode, Json<ErrorBody>)> {
    // Resolve existing group to get the current replica set.
    let view = state.monitor_cache.resolve_group(sid, gid).await.ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorBody {
                error: format!("group {gid} in store {sid} not found"),
            }),
        )
    })?;

    // Auto-assign replica_id if not provided: max existing + 1.
    let new_rid = body
        .replica_id
        .unwrap_or_else(|| view.replicas.iter().map(|r| r.replica_id).max().unwrap_or(0) + 1);

    // Check for conflict.
    if view.replicas.iter().any(|r| r.replica_id == new_rid) {
        return Err((
            StatusCode::CONFLICT,
            Json(ErrorBody {
                error: format!("replica {new_rid} already exists in group {gid}"),
            }),
        ));
    }

    let target_node = &body.node_id;

    // Step 1: ensure the target node hosts the store, then place the
    // new replica's local PxGroup on it.
    let target_has_store = state
        .monitor_cache
        .snapshot()
        .await
        .get(target_node)
        .is_some_and(|rec| rec.stores.contains_key(&sid));

    let url = mgmt_url_for_node(&state, *target_node)?;
    let client = build_server_client(url)?;
    let created_store_on_target = if target_has_store {
        let req = AddGroupRequest {
            group_id: gid,
            replica_id: new_rid,
            initial_role: Some(AddGroupInitialRole::Follower),
            start_election: Some(false),
        };
        client
            .add_group(sid, &req)
            .await
            .map_err(|e| err_502(format!("create local group on node {target_node}: {e}")))?;
        false
    } else {
        let req = AddStoreRequest {
            store_id: sid,
            port: None,
        };
        client
            .add_store(&req)
            .await
            .map_err(|e| err_502(format!("create local store on node {target_node}: {e}")))?;
        let req = AddGroupRequest {
            group_id: gid,
            replica_id: new_rid,
            initial_role: Some(AddGroupInitialRole::Follower),
            start_election: Some(false),
        };
        client
            .add_group(sid, &req)
            .await
            .map_err(|e| err_502(format!("create local group on node {target_node}: {e}")))?;
        true
    };

    // Refresh the target node's cache so the new store's `listen_addr` is
    // known before we wire it as a remote on existing peers.
    refresh_node_cache(&state, *target_node).await;

    // Step 2: Register the new replica as a remote on every existing peer.
    let Some(new_endpoint) = rpc_endpoint_for_node(&state, *target_node, sid).await else {
        return Err(err_502(format!(
            "could not determine crow-rpc endpoint for new replica on node {target_node}"
        )));
    };
    let new_remote = RemoteReplicaInfo {
        replica_id: new_rid,
        endpoint: new_endpoint,
        voting: true,
    };
    let mut wired_peers: Vec<NodeId> = Vec::new();
    for existing in &view.replicas {
        let Ok(peer_url) = mgmt_url_for_node(&state, existing.node_id) else {
            continue;
        };
        let Ok(peer_client) = build_server_client(peer_url) else {
            continue;
        };
        if let Err(e) = peer_client
            .add_remote_replicas(sid, gid, std::slice::from_ref(&new_remote))
            .await
        {
            rollback_replica(
                &state,
                sid,
                gid,
                new_rid,
                &wired_peers,
                *target_node,
                created_store_on_target,
            )
            .await;
            return Err(err_502(format!(
                "wire new replica on peer {}: {e}",
                existing.node_id
            )));
        }
        wired_peers.push(existing.node_id);
    }

    // Step 3: Register every existing peer as a remote on the new replica.
    let mut existing_remotes: Vec<RemoteReplicaInfo> = Vec::with_capacity(view.replicas.len());
    for r in &view.replicas {
        if let Some(ep) = rpc_endpoint_for_node(&state, r.node_id, sid).await {
            existing_remotes.push(RemoteReplicaInfo {
                replica_id: r.replica_id,
                endpoint: ep,
                voting: true,
            });
        }
    }
    if !existing_remotes.is_empty() {
        let new_url = mgmt_url_for_node(&state, *target_node)?;
        let new_client = build_server_client(new_url)?;
        if let Err(e) = new_client.add_remote_replicas(sid, gid, &existing_remotes).await {
            let all_peers: Vec<NodeId> = view.replicas.iter().map(|r| r.node_id).collect();
            rollback_replica(
                &state,
                sid,
                gid,
                new_rid,
                &all_peers,
                *target_node,
                created_store_on_target,
            )
            .await;
            return Err(err_502(format!("wire existing peers on new replica: {e}")));
        }
    }

    // Refresh cache for all affected nodes.
    refresh_node_cache(&state, *target_node).await;
    for existing in &view.replicas {
        refresh_node_cache(&state, existing.node_id).await;
    }

    {
        let mut cfg = state.config.write().unwrap();
        cfg.ensure_store_node(sid, *target_node);
        cfg.add_group_replica(
            sid,
            gid,
            ReplicaEntry {
                replica_id: new_rid,
                node_id: *target_node,
            },
        );
    }
    state
        .persist()
        .map_err(|e| err_500(format!("persist config: {e}")))?;

    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({
            "store_id": sid,
            "group_id": gid,
            "replica_id": new_rid,
            "node_id": target_node,
        })),
    ))
}

/// `DELETE /api/stores/:s/groups/:g/replicas/:rid`. Remove a replica:
///
/// 1. Deregister it as a remote from every other replica's node.
/// 2. Delete the local `PxGroup` on the hosting node.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns `404` if the group or replica is not found.
pub(crate) async fn http_remove_replica(
    State(state): State<AppState>,
    Path((sid, gid, rid)): Path<(u64, u64, u64)>,
) -> Result<StatusCode, (StatusCode, Json<ErrorBody>)> {
    let view = state.monitor_cache.resolve_group(sid, gid).await.ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorBody {
                error: format!("group {gid} in store {sid} not found"),
            }),
        )
    })?;

    let target = view
        .replicas
        .iter()
        .find(|r| r.replica_id == rid)
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ErrorBody {
                    error: format!("replica {rid} not found in group {gid}"),
                }),
            )
        })?;
    let target_node = target.node_id;

    // Step 0: if the replica being removed is currently the leader, ask
    // it to step down first and wait (bounded) for a survivor to win a
    // fresh election, instead of leaving the group leaderless until the
    // old leader's lease expires. Best-effort: a timeout here doesn't
    // block the removal -- the lease-expiry fallback still applies.
    if view.leader_id() == Some(rid) {
        if let Ok(url) = mgmt_url_for_node(&state, target_node) {
            if let Ok(client) = build_server_client(url) {
                match client
                    .step_down(
                        sid,
                        gid,
                        &StepDownRequest {
                            reason: format!("replica {rid} removal"),
                        },
                    )
                    .await
                {
                    Ok(result) if result.accepted => {
                        if let Some(survivor) = view.replicas.iter().find(|r| r.replica_id != rid) {
                            if let Ok(survivor_url) = mgmt_url_for_node(&state, survivor.node_id) {
                                if !wait_for_new_leader(&survivor_url, sid, gid, rid, Duration::from_secs(5))
                                    .await
                                {
                                    warn!(
                                        store_id = sid,
                                        group_id = gid,
                                        replica_id = rid,
                                        "leader step-down accepted but no new leader observed within timeout; proceeding anyway"
                                    );
                                }
                            }
                        }
                    }
                    Ok(_) => {}
                    Err(e) => {
                        warn!(
                            store_id = sid,
                            group_id = gid,
                            replica_id = rid,
                            error = %e,
                            "step-down request failed; proceeding with removal, leader-less window will close via lease expiry"
                        );
                    }
                }
            }
        }
    }

    // Step 1: Deregister this replica as a remote from every peer.
    for peer in &view.replicas {
        if peer.replica_id == rid {
            continue;
        }
        if let Ok(url) = mgmt_url_for_node(&state, peer.node_id) {
            if let Ok(client) = build_server_client(url) {
                let _ = client.remove_remote_replica(sid, gid, rid).await;
            }
        }
    }

    // Step 2: Delete the local group on the target node.
    if let Ok(url) = mgmt_url_for_node(&state, target_node) {
        if let Ok(client) = build_server_client(url) {
            let _ = client.remove_group(sid, gid).await;
        }
    }

    // Refresh all affected nodes.
    refresh_node_cache(&state, target_node).await;
    for peer in &view.replicas {
        if peer.replica_id != rid {
            refresh_node_cache(&state, peer.node_id).await;
        }
    }

    {
        let mut cfg = state.config.write().unwrap();
        cfg.remove_group_replica(sid, gid, rid);
    }
    state
        .persist()
        .map_err(|e| err_500(format!("persist config: {e}")))?;

    Ok(StatusCode::NO_CONTENT)
}
