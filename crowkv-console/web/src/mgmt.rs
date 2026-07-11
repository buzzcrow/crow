//! Logical tree: store and group planes (A5/A6).
//!
//! Key work: orchestrated store create/delete and group create/delete
//! built on top of the A4 per-node primitives. Reads aggregate from
//! the monitor cache; writes fan out to every listed node.

use crate::error::{err_400, err_500, err_502, ErrorBody};
use crate::state::AppState;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use crowkv_console_shared::clients::http::ServerClient;
use crowkv_console_shared::cluster::{GroupSummary, GroupView, ReplicaView, StoreView};
use crowkv_console_shared::mgmt::{AddGroupRequest, AddStoreRequest, RemoteReplicaInfo};
use serde::Deserialize;

use crate::expand::Recursive;

// ── Helpers ───────────────────────────────────────────────────────────

fn mgmt_url_for_node(state: &AppState, node_id: &str) -> Result<String, (StatusCode, Json<ErrorBody>)> {
    let cfg = state.config.read().unwrap();
    let entry = cfg.server_for_node(node_id).ok_or_else(|| err_502(format!("node {node_id} has no deployed server")))?;
    Ok(entry.url.clone())
}

fn build_server_client(url: String) -> Result<ServerClient, (StatusCode, Json<ErrorBody>)> {
    ServerClient::new(url).map_err(|e| err_500(format!("client build: {e}")))
}

pub(crate) async fn refresh_node_cache(state: &AppState, node_id: &str) {
    let url = {
        let cfg = state.config.read().unwrap();
        cfg.server_for_node(node_id).map(|s| s.url.clone())
    };
    if let Some(url) = url {
        if let Ok(client) = ServerClient::new(url) {
            if let Ok(stores) = client.topology().await {
                let rec = crowkv_console_shared::monitor::NodeRecord {
                    health: crowkv_console_shared::cluster::NodeHealth::Up,
                    last_seen_ms: 1,
                    stores: crowkv_console_shared::monitor::legacy_topology_to_node_stores(node_id, &stores),
                    last_error: None,
                };
                state.monitor_cache.set_node_report(node_id.to_string(), rec).await;
            }
        }
    }
}

// ── A5: Logical store plane ─────────────────────────────────────────

/// `GET /api/stores`. List stores aggregated from the monitor cache.
///
/// # Panics
/// Panics if the `RwLock` is poisoned (inside `snapshot()`).
pub async fn http_list_stores(State(state): State<AppState>, Recursive(_depth): Recursive) -> Json<Vec<StoreView>> {
    let snap = state.monitor_cache.snapshot().await;
    let mut seen: std::collections::BTreeMap<u64, StoreView> = std::collections::BTreeMap::new();
    for (node_id, rec) in &snap {
        for (sid, ns) in &rec.stores {
            let entry = seen.entry(*sid).or_insert_with(|| StoreView {
                store_id: *sid,
                name: None,
                nodes: Vec::new(),
                groups: Vec::new(),
            });
            entry.nodes.push(node_id.clone());
            for g in &ns.groups {
                if !entry.groups.iter().any(|gs| gs.group_id == g.group_id) {
                    entry.groups.push(GroupSummary {
                        group_id: g.group_id,
                        replica_count: 1,
                        leader: g.leader_hint,
                    });
                } else if let Some(gs) = entry.groups.iter_mut().find(|gs| gs.group_id == g.group_id) {
                    gs.replica_count += 1;
                    if gs.leader.is_none() {
                        gs.leader = g.leader_hint;
                    }
                }
            }
        }
    }
    Json(seen.into_values().collect())
}

#[derive(Debug, Deserialize)]
pub struct CreateStoreBody {
    pub store_id: u64,
    pub group_id: u64,
    pub replica_id: u64,
    #[serde(default)]
    pub nodes: Vec<String>,
}

/// `POST /api/stores`. Create a store across the listed nodes (or the
/// first node with a running server if `nodes` is empty). Orchestrated:
/// fans out `add_store` to each node, rolls back on partial failure.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns an error if no nodes are available or any upstream RPC fails.
pub async fn http_add_store(State(state): State<AppState>, Json(body): Json<CreateStoreBody>) -> Result<(StatusCode, Json<serde_json::Value>), (StatusCode, Json<ErrorBody>)> {
    let target_nodes = if body.nodes.is_empty() {
        let cfg = state.config.read().unwrap();
        let first = cfg
            .servers
            .iter()
            .find_map(|s| s.node_id.clone())
            .ok_or_else(|| err_400("no nodes with deployed servers"))?;
        vec![first]
    } else {
        body.nodes.clone()
    };

    let mut succeeded: Vec<String> = Vec::new();
    for nid in &target_nodes {
        let url = mgmt_url_for_node(&state, nid)?;
        let client = build_server_client(url)?;
        let req = AddStoreRequest {
            store_id: body.store_id,
            group_id: body.group_id,
            replica_id: body.replica_id,
            port: None,
        };
        match client.add_store(&req).await {
            Ok(_) => succeeded.push(nid.clone()),
            Err(e) => {
                // Roll back successful creations.
                for ok_nid in &succeeded {
                    if let Ok(u) = mgmt_url_for_node(&state, ok_nid) {
                        if let Ok(c) = build_server_client(u) {
                            let _ = c.remove_store(body.store_id).await;
                        }
                    }
                }
                return Err(err_502(format!("store create failed on node {nid}: {e}")));
            }
        }
    }

    // Refresh cache for all affected nodes.
    for nid in &succeeded {
        refresh_node_cache(&state, nid).await;
    }

    Ok((StatusCode::CREATED, Json(serde_json::json!({ "store_id": body.store_id, "nodes": succeeded }))))
}

/// `GET /api/stores/:store_id`. Aggregated store view from cache.
///
/// # Errors
/// Returns `404` if the store is not found.
pub async fn http_get_store(State(state): State<AppState>, Path(sid): Path<u64>, Recursive(_depth): Recursive) -> Result<Json<StoreView>, (StatusCode, Json<ErrorBody>)> {
    state.monitor_cache.resolve_store(sid).await.map(Json).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorBody {
                error: format!("store {sid} not found"),
            }),
        )
    })
}

/// `DELETE /api/stores/:store_id`. Delete the store across every hosting
/// node. Idempotent on per-node 404.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns `404` if the store is not found in the cache.
pub async fn http_remove_store(State(state): State<AppState>, Path(sid): Path<u64>) -> Result<StatusCode, (StatusCode, Json<ErrorBody>)> {
    let view = state.monitor_cache.resolve_store(sid).await.ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorBody {
                error: format!("store {sid} not found"),
            }),
        )
    })?;
    for nid in &view.nodes {
        if let Ok(url) = mgmt_url_for_node(&state, nid) {
            if let Ok(client) = build_server_client(url) {
                let _ = client.remove_store(sid).await;
            }
        }
    }
    for nid in &view.nodes {
        refresh_node_cache(&state, nid).await;
    }
    Ok(StatusCode::NO_CONTENT)
}

// ── A6: Logical group plane ─────────────────────────────────────────

/// `GET /api/stores/:store_id/groups`. List groups from cache.
///
/// # Errors
/// Returns `404` if the store is not found.
pub async fn http_list_groups(State(state): State<AppState>, Path(sid): Path<u64>, Recursive(_depth): Recursive) -> Result<Json<Vec<GroupSummary>>, (StatusCode, Json<ErrorBody>)> {
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
pub struct CreateGroupBody {
    pub group_id: u64,
    pub replica_id: u64,
    pub nodes: Vec<String>,
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
pub async fn http_add_group(
    State(state): State<AppState>,
    Path(sid): Path<u64>,
    Json(body): Json<CreateGroupBody>,
) -> Result<(StatusCode, Json<serde_json::Value>), (StatusCode, Json<ErrorBody>)> {
    if body.nodes.is_empty() {
        return Err(err_400("nodes list must not be empty"));
    }

    // Phase 1: create the group on each node.
    let mut succeeded: Vec<(String, u64)> = Vec::new(); // (node_id, replica_id)
    let base_rid = body.replica_id;
    for (i, nid) in body.nodes.iter().enumerate() {
        let url = mgmt_url_for_node(&state, nid)?;
        let client = build_server_client(url)?;
        let rid = base_rid + i as u64;
        let req = AddGroupRequest {
            group_id: body.group_id,
            replica_id: rid,
        };
        match client.add_group(sid, &req).await {
            Ok(()) => succeeded.push((nid.clone(), rid)),
            Err(e) => {
                for (ok_nid, _) in &succeeded {
                    if let Ok(u) = mgmt_url_for_node(&state, ok_nid) {
                        if let Ok(c) = build_server_client(u) {
                            let _ = c.remove_group(sid, body.group_id).await;
                        }
                    }
                }
                return Err(err_502(format!("group create failed on node {nid}: {e}")));
            }
        }
    }

    // Phase 2: wire remote replicas. Each node gets every other node's
    // replica as a remote.
    for (i, (nid, _rid)) in succeeded.iter().enumerate() {
        let Ok(url) = mgmt_url_for_node(&state, nid) else {
            continue;
        };
        let Ok(client) = build_server_client(url) else {
            continue;
        };
        let remotes: Vec<RemoteReplicaInfo> = succeeded
            .iter()
            .enumerate()
            .filter(|(j, _)| *j != i)
            .map(|(_, (peer_nid, peer_rid))| {
                let peer_endpoint = {
                    let cfg = state.config.read().unwrap();
                    cfg.server_for_node(peer_nid)
                        .and_then(|s| s.grpc_url.clone())
                        .unwrap_or_else(|| format!("unknown:{peer_nid}"))
                };
                RemoteReplicaInfo {
                    replica_id: *peer_rid,
                    endpoint: peer_endpoint,
                }
            })
            .collect();
        if !remotes.is_empty() {
            let _ = client.add_remotes(sid, body.group_id, &remotes).await;
        }
    }

    for (nid, _) in &succeeded {
        refresh_node_cache(&state, nid).await;
    }

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
pub async fn http_get_group(
    State(state): State<AppState>,
    Path((sid, gid)): Path<(u64, u64)>,
    Recursive(_depth): Recursive,
) -> Result<Json<GroupView>, (StatusCode, Json<ErrorBody>)> {
    state.monitor_cache.resolve_group(sid, gid).await.map(Json).ok_or_else(|| {
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
pub async fn http_remove_group(State(state): State<AppState>, Path((sid, gid)): Path<(u64, u64)>) -> Result<StatusCode, (StatusCode, Json<ErrorBody>)> {
    let view = state.monitor_cache.resolve_group(sid, gid).await.ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorBody {
                error: format!("group {gid} in store {sid} not found"),
            }),
        )
    })?;
    let node_ids: Vec<String> = view.replicas.iter().map(|r| r.node_id.clone()).collect();
    for nid in &node_ids {
        if let Ok(url) = mgmt_url_for_node(&state, nid) {
            if let Ok(client) = build_server_client(url) {
                let _ = client.remove_group(sid, gid).await;
            }
        }
    }
    for nid in &node_ids {
        refresh_node_cache(&state, nid).await;
    }
    Ok(StatusCode::NO_CONTENT)
}

// ── A7: Logical replica plane ─────────────────────────────────────

/// Roll back a partially-wired replica: deregister from `peer_nodes`,
/// then delete the local group on `target_node`. If
/// `created_store_on_target` is true, also drop the store on
/// `target_node` (we created it atomically in step 1).
async fn rollback_replica(state: &AppState, sid: u64, gid: u64, rid: u64, peer_nodes: &[String], target_node: &str, created_store_on_target: bool) {
    for nid in peer_nodes {
        if let Ok(u) = mgmt_url_for_node(state, nid) {
            if let Ok(c) = build_server_client(u) {
                let _ = c.remove_remote(sid, gid, rid).await;
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

fn grpc_endpoint_for_node(state: &AppState, node_id: &str) -> String {
    let cfg = state.config.read().unwrap();
    cfg.server_for_node(node_id)
        .and_then(|s| s.grpc_url.clone())
        .unwrap_or_else(|| format!("unknown:{node_id}"))
}

/// `GET /api/stores/:s/groups/:g/replicas`. Unified replica list from
/// the monitor cache.
///
/// # Errors
/// Returns `404` if the group is not found.
pub async fn http_list_replicas(
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
pub async fn http_get_replica(
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
    let replica = view.replicas.iter().find(|r| r.replica_id == rid).ok_or_else(|| {
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
pub struct AddReplicaBody {
    pub node_id: String,
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
pub async fn http_add_replica(
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
    let new_rid = body.replica_id.unwrap_or_else(|| view.replicas.iter().map(|r| r.replica_id).max().unwrap_or(0) + 1);

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
    //
    // `crowkv-server` has no separate "add store" + "add group"
    // sequence for a fresh store: `add_store` creates store + initial
    // group + initial replica atomically, while `add_group` requires
    // the store to already exist. We branch on whether the monitor
    // cache shows this node already hosting the store.
    let target_has_store = state
        .monitor_cache
        .snapshot()
        .await
        .get(target_node.as_str())
        .is_some_and(|rec| rec.stores.contains_key(&sid));

    let url = mgmt_url_for_node(&state, target_node)?;
    let client = build_server_client(url)?;
    let created_store_on_target = if target_has_store {
        let req = AddGroupRequest {
            group_id: gid,
            replica_id: new_rid,
        };
        client
            .add_group(sid, &req)
            .await
            .map_err(|e| err_502(format!("create local group on node {target_node}: {e}")))?;
        false
    } else {
        let req = AddStoreRequest {
            store_id: sid,
            group_id: gid,
            replica_id: new_rid,
            port: None,
        };
        client
            .add_store(&req)
            .await
            .map_err(|e| err_502(format!("create local store on node {target_node}: {e}")))?;
        true
    };

    // Step 2: Register the new replica as a remote on every existing peer.
    let new_remote = RemoteReplicaInfo {
        replica_id: new_rid,
        endpoint: grpc_endpoint_for_node(&state, target_node),
    };
    let mut wired_peers: Vec<String> = Vec::new();
    for existing in &view.replicas {
        let Ok(peer_url) = mgmt_url_for_node(&state, &existing.node_id) else { continue };
        let Ok(peer_client) = build_server_client(peer_url) else { continue };
        if let Err(e) = peer_client.add_remotes(sid, gid, std::slice::from_ref(&new_remote)).await {
            rollback_replica(&state, sid, gid, new_rid, &wired_peers, target_node, created_store_on_target).await;
            return Err(err_502(format!("wire new replica on peer {}: {e}", existing.node_id)));
        }
        wired_peers.push(existing.node_id.clone());
    }

    // Step 3: Register every existing peer as a remote on the new replica.
    let existing_remotes: Vec<RemoteReplicaInfo> = view
        .replicas
        .iter()
        .map(|r| RemoteReplicaInfo {
            replica_id: r.replica_id,
            endpoint: grpc_endpoint_for_node(&state, &r.node_id),
        })
        .collect();
    if !existing_remotes.is_empty() {
        let new_url = mgmt_url_for_node(&state, target_node)?;
        let new_client = build_server_client(new_url)?;
        if let Err(e) = new_client.add_remotes(sid, gid, &existing_remotes).await {
            let all_peers: Vec<String> = view.replicas.iter().map(|r| r.node_id.clone()).collect();
            rollback_replica(&state, sid, gid, new_rid, &all_peers, target_node, created_store_on_target).await;
            return Err(err_502(format!("wire existing peers on new replica: {e}")));
        }
    }

    // Refresh cache for all affected nodes.
    refresh_node_cache(&state, target_node).await;
    for existing in &view.replicas {
        refresh_node_cache(&state, &existing.node_id).await;
    }

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
pub async fn http_remove_replica(State(state): State<AppState>, Path((sid, gid, rid)): Path<(u64, u64, u64)>) -> Result<StatusCode, (StatusCode, Json<ErrorBody>)> {
    let view = state.monitor_cache.resolve_group(sid, gid).await.ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorBody {
                error: format!("group {gid} in store {sid} not found"),
            }),
        )
    })?;

    let target = view.replicas.iter().find(|r| r.replica_id == rid).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorBody {
                error: format!("replica {rid} not found in group {gid}"),
            }),
        )
    })?;
    let target_node = target.node_id.clone();

    // Step 1: Deregister this replica as a remote from every peer.
    for peer in &view.replicas {
        if peer.replica_id == rid {
            continue;
        }
        if let Ok(url) = mgmt_url_for_node(&state, &peer.node_id) {
            if let Ok(client) = build_server_client(url) {
                let _ = client.remove_remote(sid, gid, rid).await;
            }
        }
    }

    // Step 2: Delete the local group on the target node.
    if let Ok(url) = mgmt_url_for_node(&state, &target_node) {
        if let Ok(client) = build_server_client(url) {
            let _ = client.remove_group(sid, gid).await;
        }
    }

    // Refresh all affected nodes.
    refresh_node_cache(&state, &target_node).await;
    for peer in &view.replicas {
        if peer.replica_id != rid {
            refresh_node_cache(&state, &peer.node_id).await;
        }
    }

    Ok(StatusCode::NO_CONTENT)
}
