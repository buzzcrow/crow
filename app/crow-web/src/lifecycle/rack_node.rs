// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Physical tree: rack / node lifecycle (A3).

use crate::error::{err_500, map_config_err, map_persist_err, ErrorBody};
use crate::expand::Recursive;
use crate::physical_view::PhysicalBuilder;
use crate::state::AppState;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use crow_console_shared::cluster::RackId;
use crow_console_shared::config::{NodeEntry, RackEntry};
use crow_console_shared::expand::RecursiveDepth;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
pub(crate) struct AddRackBody {
    id: RackId,
    #[serde(default)]
    name: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct NodeQuery {
    /// Optional `?rack_id=<id>` filter for `GET /api/nodes`.
    #[serde(default)]
    rack_id: Option<RackId>,
}

/// List all racks.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
pub(crate) async fn http_list_racks(
    State(state): State<AppState>,
    Recursive(depth): Recursive,
) -> Json<serde_json::Value> {
    if matches!(depth, RecursiveDepth::None) {
        let cfg = state.config.read().unwrap();
        return Json(serde_json::to_value(&cfg.racks).expect("serialize racks"));
    }
    let snap = state.monitor_cache.snapshot().await;
    let cfg = state.config.read().unwrap();
    let pids = state.kv_pid_snapshot();
    let mut builder = PhysicalBuilder::new(&cfg, &snap, &pids);
    let limit = depth.effective();
    let racks: Vec<_> = cfg.racks.iter().map(|r| builder.build_rack(r, limit)).collect();
    let trunc = builder.into_truncation();
    Json(serde_json::json!({
        "items": racks,
        "truncated_at": trunc.paths,
    }))
}

/// Add a new rack.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns an error if rack addition or config persistence fails.
pub(crate) async fn http_add_rack(
    State(state): State<AppState>,
    Json(body): Json<AddRackBody>,
) -> Result<(StatusCode, Json<RackEntry>), (StatusCode, Json<ErrorBody>)> {
    let entry = RackEntry {
        id: body.id,
        name: body.name,
    };
    {
        let mut cfg = state.config.write().unwrap();
        cfg.add_rack(entry.clone()).map_err(map_config_err)?;
    }
    state.persist().map_err(map_persist_err)?;
    Ok((StatusCode::CREATED, Json(entry)))
}

/// Remove a rack.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns an error if rack removal or config persistence fails.
pub(crate) async fn http_remove_rack(
    State(state): State<AppState>,
    Path(id): Path<u64>,
) -> Result<StatusCode, (StatusCode, Json<ErrorBody>)> {
    {
        let mut cfg = state.config.write().unwrap();
        cfg.remove_rack(id).map_err(map_config_err)?;
    }
    state.persist().map_err(map_persist_err)?;
    Ok(StatusCode::NO_CONTENT)
}

/// List nodes, optionally filtered by rack ID.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
pub(crate) async fn http_list_nodes(
    State(state): State<AppState>,
    Query(q): Query<NodeQuery>,
    Recursive(_depth): Recursive,
) -> Json<Vec<NodeEntry>> {
    let cfg = state.config.read().unwrap();
    let nodes: Vec<NodeEntry> = match q.rack_id {
        Some(rack_id) => cfg
            .nodes
            .iter()
            .filter(|n| n.rack_id == rack_id)
            .cloned()
            .collect(),
        None => cfg.nodes.clone(),
    };
    Json(nodes)
}

/// Add a new node.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns an error if node addition or config persistence fails.
pub(crate) async fn http_add_node(
    State(state): State<AppState>,
    Json(entry): Json<NodeEntry>,
) -> Result<(StatusCode, Json<NodeEntry>), (StatusCode, Json<ErrorBody>)> {
    {
        let mut cfg = state.config.write().unwrap();
        cfg.add_node(entry.clone()).map_err(map_config_err)?;
    }
    state.persist().map_err(map_persist_err)?;
    state
        .prepare_node_workspace(entry.id.to_string())
        .map_err(|e| err_500(e.to_string()))?;
    Ok((StatusCode::CREATED, Json(entry)))
}

/// Remove a node.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns an error if node removal or config persistence fails.
pub(crate) async fn http_remove_node(
    State(state): State<AppState>,
    Path(id): Path<u64>,
) -> Result<StatusCode, (StatusCode, Json<ErrorBody>)> {
    {
        let mut cfg = state.config.write().unwrap();
        cfg.remove_node(id).map_err(map_config_err)?;
    }
    state.persist().map_err(map_persist_err)?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Debug, Serialize)]
pub(crate) struct PingResult {
    /// `true` when the SSH handshake (or local-loopback equivalent) succeeded.
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

/// `POST /api/nodes/:id/ping`. For SSH-enabled nodes runs the
/// `crow_console_shared::ssh::probe` handshake; for local-fork nodes (`ssh_user=""`)
/// the probe is a no-op success since `lifecycle::deploy_local` does
/// not require any reachability.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns an error if the node is not found.
pub(crate) async fn http_ping_node(
    State(state): State<AppState>,
    Path(id): Path<u64>,
) -> Result<Json<PingResult>, (StatusCode, Json<ErrorBody>)> {
    let node = {
        let cfg = state.config.read().unwrap();
        cfg.node(id).cloned().ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ErrorBody {
                    error: format!("node {id} not found"),
                }),
            )
        })?
    };
    // For local-fork nodes, check whether a KV server process is
    // actually running (has a tracked PID). SSH reachability is a
    // no-op for local nodes, so without this check the health badge
    // always shows Healthy even after the server is stopped.
    if !node.ssh_enabled() {
        let has_kv_pid = state.runtime_pid(id).is_some();
        let has_ddb_pid = state.diskdb_runtime_pid(id).is_some();
        return Ok(Json(PingResult {
            ok: has_kv_pid || has_ddb_pid,
            error: if has_kv_pid || has_ddb_pid {
                None
            } else {
                Some("no running server process".to_string())
            },
        }));
    }
    match crow_console_shared::ssh::probe(&node).await {
        Ok(()) => Ok(Json(PingResult {
            ok: true,
            error: None,
        })),
        Err(e) => Ok(Json(PingResult {
            ok: false,
            error: Some(format!("{e}")),
        })),
    }
}

/// `GET /api/racks/:rack_id`. Rack detail.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns `404` if the rack does not exist.
pub(crate) async fn http_get_rack(
    State(state): State<AppState>,
    Path(id): Path<u64>,
    Recursive(depth): Recursive,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorBody>)> {
    let rack_id = id;
    if matches!(depth, RecursiveDepth::None) {
        let cfg = state.config.read().unwrap();
        let rack = cfg.racks.iter().find(|r| r.id == rack_id).ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ErrorBody {
                    error: format!("rack {id} not found"),
                }),
            )
        })?;
        let node_ids: Vec<u64> = cfg
            .nodes
            .iter()
            .filter(|n| n.rack_id == rack_id)
            .map(|n| n.id)
            .collect();
        return Ok(Json(serde_json::json!({
            "id": rack.id,
            "name": rack.name,
            "nodes": node_ids,
        })));
    }
    let snap = state.monitor_cache.snapshot().await;
    let cfg = state.config.read().unwrap();
    let rack = cfg
        .racks
        .iter()
        .find(|r| r.id == rack_id)
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ErrorBody {
                    error: format!("rack {id} not found"),
                }),
            )
        })?
        .clone();
    let pids = state.kv_pid_snapshot();
    let mut builder = PhysicalBuilder::new(&cfg, &snap, &pids);
    let view = builder.build_rack(&rack, depth.effective());
    let trunc = builder.into_truncation();
    let mut body = serde_json::to_value(&view).expect("serialize rack view");
    body["truncated_at"] = serde_json::to_value(&trunc.paths).expect("serialize truncated_at");
    Ok(Json(body))
}

/// `GET /api/racks/:rack_id/nodes`. List nodes under a specific rack.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns `404` if the rack does not exist.
pub(crate) async fn http_list_rack_nodes(
    State(state): State<AppState>,
    Path(rack_id): Path<u64>,
    Recursive(depth): Recursive,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorBody>)> {
    let rack_id_num = rack_id;
    if matches!(depth, RecursiveDepth::None) {
        let cfg = state.config.read().unwrap();
        if !cfg.racks.iter().any(|r| r.id == rack_id_num) {
            return Err((
                StatusCode::NOT_FOUND,
                Json(ErrorBody {
                    error: format!("rack {rack_id} not found"),
                }),
            ));
        }
        let nodes: Vec<NodeEntry> = cfg
            .nodes
            .iter()
            .filter(|n| n.rack_id == rack_id_num)
            .cloned()
            .collect();
        return Ok(Json(serde_json::to_value(&nodes).expect("serialize nodes")));
    }
    let snap = state.monitor_cache.snapshot().await;
    let cfg = state.config.read().unwrap();
    if !cfg.racks.iter().any(|r| r.id == rack_id_num) {
        return Err((
            StatusCode::NOT_FOUND,
            Json(ErrorBody {
                error: format!("rack {rack_id} not found"),
            }),
        ));
    }
    let nodes: Vec<NodeEntry> = cfg
        .nodes
        .iter()
        .filter(|n| n.rack_id == rack_id_num)
        .cloned()
        .collect();
    let pids = state.kv_pid_snapshot();
    let mut builder = PhysicalBuilder::new(&cfg, &snap, &pids);
    let limit = depth.effective();
    let views: Vec<_> = nodes.iter().map(|n| builder.build_node(n, limit)).collect();
    let trunc = builder.into_truncation();
    Ok(Json(serde_json::json!({
        "items": views,
        "truncated_at": trunc.paths,
    })))
}

/// `POST /api/racks/:rack_id/nodes`. Create a node under a specific rack.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns an error if node creation or config persistence fails.
pub(crate) async fn http_add_rack_node(
    State(state): State<AppState>,
    Path(rack_id): Path<u64>,
    Json(mut entry): Json<NodeEntry>,
) -> Result<(StatusCode, Json<NodeEntry>), (StatusCode, Json<ErrorBody>)> {
    entry.rack_id = rack_id;
    {
        let mut cfg = state.config.write().unwrap();
        cfg.add_node(entry.clone()).map_err(map_config_err)?;
    }
    state.persist().map_err(map_persist_err)?;
    state
        .prepare_node_workspace(entry.id.to_string())
        .map_err(|e| err_500(e.to_string()))?;
    Ok((StatusCode::CREATED, Json(entry)))
}

/// `GET /api/nodes/:node_id`. Node detail including server status.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns `404` if the node does not exist.
pub(crate) async fn http_get_node(
    State(state): State<AppState>,
    Path(id): Path<u64>,
    Recursive(_depth): Recursive,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorBody>)> {
    let node_id_num = id;
    let snap = state.monitor_cache.snapshot().await;
    let cfg = state.config.read().unwrap();
    let node = cfg.node(node_id_num).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorBody {
                error: format!("node {id} not found"),
            }),
        )
    })?;
    let server = cfg
        .server_for_node(node_id_num)
        .map(|entry| super::live_server_process_with_pid(&state, entry, snap.get(&node_id_num)));
    Ok(Json(serde_json::json!({
        "id": node.id,
        "rack_id": node.rack_id,
        "host": node.host,
        "ssh_user": node.ssh_user,
        "ssh_port": node.ssh_port,
        "has_server": server.is_some(),
        "server": server,
    })))
}
