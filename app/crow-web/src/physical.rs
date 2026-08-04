// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Per-node store / group / remote primitives (A4).
//!
//! Key work: read endpoints served from the monitor cache, low-level
//! mutators that proxy to the upstream `crow-kv-server` management API
//! and invalidate the affected node in the monitor cache on success.

use crate::error::{err_500, map_err, ErrorBody};
use crate::expand::Recursive;
use crate::state::AppState;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use crow_console_shared::clients::http::ServerClient;
use crow_console_shared::cluster::{NodeGroup, NodeStore};
use crow_console_shared::mgmt::{AddGroupRequest, AddStoreRequest, RemoteReplicaInfo};

// ── Helpers ──────────────────────────────────────────────────────────

fn mgmt_url_for_node(state: &AppState, node_id: &str) -> Result<String, (StatusCode, Json<ErrorBody>)> {
    let cfg = state.config.read().unwrap();
    let entry = cfg.server_for_node(node_id).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorBody {
                error: format!("no server deployed on node {node_id}"),
            }),
        )
    })?;
    Ok(entry.url.clone())
}

fn build_server_client(url: String) -> Result<ServerClient, (StatusCode, Json<ErrorBody>)> {
    ServerClient::new(url).map_err(|e| err_500(format!("client build: {e}")))
}

// ── Read endpoints (from monitor cache) ──────────────────────────────

/// `GET /api/nodes/:node_id/stores`. List per-node stores from the
/// monitor cache.
///
/// # Errors
/// Returns `404` if the node is not in the cache.
pub async fn http_list_node_stores(
    State(state): State<AppState>,
    Path(node_id): Path<String>,
    Recursive(_depth): Recursive,
) -> Result<Json<Vec<NodeStore>>, (StatusCode, Json<ErrorBody>)> {
    let snap = state.monitor_cache.snapshot().await;
    let rec = snap.get(&node_id).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorBody {
                error: format!("node {node_id} not in cache"),
            }),
        )
    })?;
    Ok(Json(rec.stores.values().cloned().collect()))
}

/// `GET /api/nodes/:node_id/stores/:store_id`. Per-node store detail.
///
/// # Errors
/// Returns `404` if the node or store is not in the cache.
pub async fn http_get_node_store(
    State(state): State<AppState>,
    Path((node_id, store_id)): Path<(String, u64)>,
    Recursive(_depth): Recursive,
) -> Result<Json<NodeStore>, (StatusCode, Json<ErrorBody>)> {
    let snap = state.monitor_cache.snapshot().await;
    let rec = snap.get(&node_id).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorBody {
                error: format!("node {node_id} not in cache"),
            }),
        )
    })?;
    let ns = rec.stores.get(&store_id).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorBody {
                error: format!("store {store_id} not found on node {node_id}"),
            }),
        )
    })?;
    Ok(Json(ns.clone()))
}

/// `GET /api/nodes/:node_id/stores/:store_id/groups`. Per-node group list.
///
/// # Errors
/// Returns `404` if the node or store is not in the cache.
pub async fn http_list_node_groups(
    State(state): State<AppState>,
    Path((node_id, store_id)): Path<(String, u64)>,
    Recursive(_depth): Recursive,
) -> Result<Json<Vec<NodeGroup>>, (StatusCode, Json<ErrorBody>)> {
    let snap = state.monitor_cache.snapshot().await;
    let rec = snap.get(&node_id).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorBody {
                error: format!("node {node_id} not in cache"),
            }),
        )
    })?;
    let ns = rec.stores.get(&store_id).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorBody {
                error: format!("store {store_id} not found on node {node_id}"),
            }),
        )
    })?;
    Ok(Json(ns.groups.clone()))
}

/// `GET /api/nodes/:node_id/stores/:store_id/groups/:group_id`.
/// Per-node group detail (local replica + remotes + leader hint).
///
/// # Errors
/// Returns `404` if the node, store, or group is not in the cache.
pub async fn http_get_node_group(
    State(state): State<AppState>,
    Path((node_id, store_id, group_id)): Path<(String, u64, u64)>,
    Recursive(_depth): Recursive,
) -> Result<Json<NodeGroup>, (StatusCode, Json<ErrorBody>)> {
    let snap = state.monitor_cache.snapshot().await;
    let rec = snap.get(&node_id).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorBody {
                error: format!("node {node_id} not in cache"),
            }),
        )
    })?;
    let ns = rec.stores.get(&store_id).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorBody {
                error: format!("store {store_id} not found on node {node_id}"),
            }),
        )
    })?;
    let ng = ns.groups.iter().find(|g| g.group_id == group_id).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorBody {
                error: format!("group {group_id} not found in store {store_id} on node {node_id}"),
            }),
        )
    })?;
    Ok(Json(ng.clone()))
}

// ── Mutator endpoints (proxy to upstream, invalidate cache) ──────────

/// `POST /api/nodes/:node_id/stores`. Create a local `PxStore` on the node.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns an error if the node has no server or the upstream RPC fails.
pub async fn http_add_node_store(
    State(state): State<AppState>,
    Path(node_id): Path<String>,
    Json(req): Json<AddStoreRequest>,
) -> Result<StatusCode, (StatusCode, Json<ErrorBody>)> {
    let url = mgmt_url_for_node(&state, &node_id)?;
    let client = build_server_client(url)?;
    client.add_store(&req).await.map_err(map_err)?;
    state.monitor_cache.drop_node(&node_id).await;
    Ok(StatusCode::CREATED)
}

/// `DELETE /api/nodes/:node_id/stores/:store_id`. Delete a local `PxStore`.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns an error if the node has no server or the upstream RPC fails.
pub async fn http_remove_node_store(
    State(state): State<AppState>,
    Path((node_id, store_id)): Path<(String, u64)>,
) -> Result<StatusCode, (StatusCode, Json<ErrorBody>)> {
    let url = mgmt_url_for_node(&state, &node_id)?;
    let client = build_server_client(url)?;
    client.remove_store(store_id).await.map_err(map_err)?;
    state.monitor_cache.drop_node(&node_id).await;
    Ok(StatusCode::NO_CONTENT)
}

/// `POST /api/nodes/:node_id/stores/:store_id/groups`. Create a local
/// `PxGroup` on the node.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns an error if the node has no server or the upstream RPC fails.
pub async fn http_add_node_group(
    State(state): State<AppState>,
    Path((node_id, store_id)): Path<(String, u64)>,
    Json(req): Json<AddGroupRequest>,
) -> Result<StatusCode, (StatusCode, Json<ErrorBody>)> {
    let url = mgmt_url_for_node(&state, &node_id)?;
    let client = build_server_client(url)?;
    client.add_group(store_id, &req).await.map_err(map_err)?;
    state.monitor_cache.drop_node(&node_id).await;
    Ok(StatusCode::NO_CONTENT)
}

/// `DELETE /api/nodes/:node_id/stores/:store_id/groups/:group_id`.
/// Delete a local `PxGroup`.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns an error if the node has no server or the upstream RPC fails.
pub async fn http_remove_node_group(
    State(state): State<AppState>,
    Path((node_id, store_id, group_id)): Path<(String, u64, u64)>,
) -> Result<StatusCode, (StatusCode, Json<ErrorBody>)> {
    let url = mgmt_url_for_node(&state, &node_id)?;
    let client = build_server_client(url)?;
    client.remove_group(store_id, group_id).await.map_err(map_err)?;
    state.monitor_cache.drop_node(&node_id).await;
    Ok(StatusCode::NO_CONTENT)
}

/// `POST /api/nodes/:node_id/stores/:store_id/groups/:group_id/remotes`.
/// Add a remote-replica entry.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns an error if the node has no server or the upstream RPC fails.
pub async fn http_add_node_remote(
    State(state): State<AppState>,
    Path((node_id, store_id, group_id)): Path<(String, u64, u64)>,
    Json(remotes): Json<Vec<RemoteReplicaInfo>>,
) -> Result<StatusCode, (StatusCode, Json<ErrorBody>)> {
    let url = mgmt_url_for_node(&state, &node_id)?;
    let client = build_server_client(url)?;
    client
        .add_remote_replicas(store_id, group_id, &remotes)
        .await
        .map_err(map_err)?;
    state.monitor_cache.drop_node(&node_id).await;
    Ok(StatusCode::NO_CONTENT)
}

/// `DELETE /api/nodes/:node_id/stores/:store_id/groups/:group_id/remotes/:replica_id`.
/// Remove a remote-replica entry.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns an error if the node has no server or the upstream RPC fails.
pub async fn http_remove_node_remote(
    State(state): State<AppState>,
    Path((node_id, store_id, group_id, replica_id)): Path<(String, u64, u64, u64)>,
) -> Result<StatusCode, (StatusCode, Json<ErrorBody>)> {
    let url = mgmt_url_for_node(&state, &node_id)?;
    let client = build_server_client(url)?;
    client
        .remove_remote_replica(store_id, group_id, replica_id)
        .await
        .map_err(map_err)?;
    state.monitor_cache.drop_node(&node_id).await;
    Ok(StatusCode::NO_CONTENT)
}
