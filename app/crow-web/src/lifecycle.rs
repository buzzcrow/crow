// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

use crate::error::{err_400, err_500, err_502, map_config_err, map_persist_err, ErrorBody};
use crate::expand::Recursive;
use crate::physical_view::PhysicalBuilder;
use crate::state::AppState;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use crow_console_shared::cluster::{DiskGroupId, NodeHealth, NodeId, ProcState, RackId, ServerProcess};
use crow_console_shared::config::{DiskEntry, DiskGroupEntry, NodeEntry, RackEntry, ServerEntry};
use crow_console_shared::expand::RecursiveDepth;
use crow_console_shared::monitor::NodeRecord;
use serde::{Deserialize, Serialize};
use std::time::Duration;

fn live_server_process(entry: &ServerEntry, rec: Option<&NodeRecord>) -> ServerProcess {
    let health = rec.map_or(NodeHealth::Unknown, |node| node.health);
    let state = match health {
        NodeHealth::Up => ProcState::Running,
        NodeHealth::Down => ProcState::Failed,
        NodeHealth::Unknown => ProcState::Unknown,
    };
    ServerProcess {
        mgmt_url: entry.url.clone(),
        grpc_url: entry.grpc_url.clone().unwrap_or_default(),
        pid: None,
        state,
        health,
        last_seen_ms: rec.map_or(0, |node| node.last_seen_ms),
    }
}

fn live_server_process_with_pid(
    state: &AppState,
    entry: &ServerEntry,
    rec: Option<&NodeRecord>,
) -> ServerProcess {
    let mut proc = live_server_process(entry, rec);
    if let Some(node_id) = entry.node_id {
        proc.pid = state.runtime_pid(node_id.to_string());
    }
    proc
}

// ── Physical tree: rack / node / server lifecycle (A3) ──────────────
//
// All handlers mutate the in-memory `ConsoleConfig` under `state.config`
// and persist via `state.persist()` before returning. Lock is held only
// for the synchronous mutation; the persist call writes a small TOML
// file (atomic rename) and runs without holding the lock.

#[derive(Debug, Deserialize)]
pub struct AddRackBody {
    id: RackId,
    #[serde(default)]
    name: String,
}

#[derive(Debug, Deserialize)]
pub struct NodeQuery {
    /// Optional `?rack_id=<id>` filter for `GET /api/nodes`.
    #[serde(default)]
    rack_id: Option<RackId>,
}

/// List all racks.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns `400` if `?recursive=` is malformed or out of range.
///
/// At `recursive=0` (or absent) returns a flat `Vec<RackEntry>`. At
/// `recursive>=1` returns a wrapper `{ items, truncated_at }` where
/// each item carries an optional `nodes` collection inflated up to the
/// requested depth (rack → node → store → group).
pub async fn http_list_racks(
    State(state): State<AppState>,
    Recursive(depth): Recursive,
) -> Json<serde_json::Value> {
    if matches!(depth, RecursiveDepth::None) {
        let cfg = state.config.read().unwrap();
        return Json(serde_json::to_value(&cfg.racks).expect("serialize racks"));
    }
    let snap = state.monitor_cache.snapshot().await;
    let cfg = state.config.read().unwrap();
    let mut builder = PhysicalBuilder::new(&cfg, &snap);
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
pub async fn http_add_rack(
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

    // Sync group-0 sysdata. Best-effort: config TOML is the source of
    // truth; sysdata is derived. A later cluster_init re-run reconciles.
    if let Some(hw) = crate::mgmt::build_hardware_client(&state).await {
        let value = crow_protocol::common::RackValue {
            status: crow_protocol::common::HwStatus::Up as i32,
            node_ids: Vec::new(),
        };
        if let Err(e) = hw.add_rack(entry.id, &value).await {
            tracing::warn!(rack_id = entry.id, error = %e, "sysdata sync: add_rack failed");
        }
    }

    Ok((StatusCode::CREATED, Json(entry)))
}

/// Remove a rack.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns an error if rack removal or config persistence fails.
pub async fn http_remove_rack(
    State(state): State<AppState>,
    Path(id): Path<u64>,
) -> Result<StatusCode, (StatusCode, Json<ErrorBody>)> {
    {
        let mut cfg = state.config.write().unwrap();
        cfg.remove_rack(id).map_err(map_config_err)?;
    }
    state.persist().map_err(map_persist_err)?;

    // Cascade-remove group-0 sysdata (rack + child nodes + their disk-groups).
    if let Some(hw) = crate::mgmt::build_hardware_client(&state).await {
        if let Err(e) = hw.remove_rack_cascade(id).await {
            tracing::warn!(rack_id = id, error = %e, "sysdata sync: remove_rack_cascade failed");
        }
    }

    Ok(StatusCode::NO_CONTENT)
}

/// List nodes, optionally filtered by rack ID.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
pub async fn http_list_nodes(
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
pub async fn http_add_node(
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

    // Sync group-0 sysdata. Best-effort.
    if let Some(hw) = crate::mgmt::build_hardware_client(&state).await {
        let value = crow_protocol::common::NodeValue {
            status: crow_protocol::common::HwStatus::Up as i32,
            last_used_dg_id: 0,
            disk_group_ids: Vec::new(),
            status_changed_at_ms: 0,
            temp_failure_since_ms: None,
        };
        if let Err(e) = hw.add_node(entry.rack_id, entry.id, &value).await {
            tracing::warn!(node_id = entry.id, error = %e, "sysdata sync: add_node failed");
        }
    }

    Ok((StatusCode::CREATED, Json(entry)))
}

/// Remove a node.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns an error if node removal or config persistence fails.
pub async fn http_remove_node(
    State(state): State<AppState>,
    Path(id): Path<u64>,
) -> Result<StatusCode, (StatusCode, Json<ErrorBody>)> {
    // Capture rack_id before removing from config (needed for sysdata cascade).
    let rack_id = {
        let cfg = state.config.read().unwrap();
        cfg.node(id).map(|n| n.rack_id)
    };
    {
        let mut cfg = state.config.write().unwrap();
        cfg.remove_node(id).map_err(map_config_err)?;
    }
    state.persist().map_err(map_persist_err)?;

    // Cascade-remove group-0 sysdata (node + child disk-groups + disks).
    if let (Some(hw), Some(rack_id)) = (crate::mgmt::build_hardware_client(&state).await, rack_id) {
        if let Err(e) = hw.remove_node_cascade(rack_id, id).await {
            tracing::warn!(node_id = id, error = %e, "sysdata sync: remove_node_cascade failed");
        }
    }

    Ok(StatusCode::NO_CONTENT)
}

#[derive(Debug, Serialize)]
pub struct PingResult {
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
pub async fn http_ping_node(
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
    if !node.ssh_enabled() {
        return Ok(Json(PingResult {
            ok: true,
            error: None,
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

// ── Rack detail ──────────────────────────────────────────────────────

/// `GET /api/racks/:rack_id`. Rack detail.
///
/// At `recursive=0` (or absent) the legacy shape `{id, name, nodes: [node_ids]}`
/// is preserved. At `recursive>=1` the response shape changes to
/// `{id, name, nodes: [<NodeView>], truncated_at: [...]}` where each
/// node inlines stores / groups up to the requested depth.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns `404` if the rack does not exist.
pub async fn http_get_rack(
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
    let mut builder = PhysicalBuilder::new(&cfg, &snap);
    let view = builder.build_rack(&rack, depth.effective());
    let trunc = builder.into_truncation();
    let mut body = serde_json::to_value(&view).expect("serialize rack view");
    body["truncated_at"] = serde_json::to_value(&trunc.paths).expect("serialize truncated_at");
    Ok(Json(body))
}

/// `GET /api/racks/:rack_id/nodes`. List nodes under a specific rack.
///
/// At `recursive=0` returns a flat `Vec<NodeEntry>` (legacy shape). At
/// `recursive>=1` returns `{ items: [NodeView], truncated_at: [...] }`
/// with the per-node store / group tree inflated up to the depth cap.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns `404` if the rack does not exist.
pub async fn http_list_rack_nodes(
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
    let mut builder = PhysicalBuilder::new(&cfg, &snap);
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
pub async fn http_add_rack_node(
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

    // Sync group-0 sysdata. Best-effort.
    if let Some(hw) = crate::mgmt::build_hardware_client(&state).await {
        let value = crow_protocol::common::NodeValue {
            status: crow_protocol::common::HwStatus::Up as i32,
            last_used_dg_id: 0,
            disk_group_ids: Vec::new(),
            status_changed_at_ms: 0,
            temp_failure_since_ms: None,
        };
        if let Err(e) = hw.add_node(rack_id, entry.id, &value).await {
            tracing::warn!(node_id = entry.id, error = %e, "sysdata sync: add_node failed");
        }
    }

    Ok((StatusCode::CREATED, Json(entry)))
}

// ── Node detail ──────────────────────────────────────────────────────

/// `GET /api/nodes/:node_id`. Node detail including server status.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns `404` if the node does not exist.
pub async fn http_get_node(
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
        .map(|entry| live_server_process_with_pid(&state, entry, snap.get(&node_id_num)));
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

// ── Server lifecycle (node-addressed) ────────────────────────────────

/// One row of `GET /api/servers`: a deployed `crow-kv-server` projected
/// from the persisted config plus the live monitor cache.
#[derive(Debug, Serialize)]
pub struct ServerSummary {
    /// Owning node id (`None` for plain externally-registered servers).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_id: Option<u64>,
    pub mgmt_url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub grpc_url: Option<String>,
    /// Live pid if the console currently tracks the process.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pid: Option<u32>,
    /// Latest health from the monitor cache (`unknown` until probed).
    pub health: NodeHealth,
}

/// `GET /api/servers`. Cluster-wide list of deployed servers, one row
/// per `ServerEntry`, with health from the monitor cache and the live
/// pid when tracked. The CLI's `server list` renders this directly.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
pub async fn http_list_servers(State(state): State<AppState>) -> Json<Vec<ServerSummary>> {
    let snap = state.monitor_cache.snapshot().await;
    let cfg = state.config.read().unwrap();
    let rows = cfg
        .servers
        .iter()
        .map(|s| {
            let health = s
                .node_id
                .and_then(|n| snap.get(&n))
                .map_or(NodeHealth::Unknown, |rec| rec.health);
            let pid = s.node_id.and_then(|n| state.runtime_pid(n.to_string()));
            ServerSummary {
                node_id: s.node_id,
                mgmt_url: s.url.clone(),
                grpc_url: s.grpc_url.clone(),
                pid,
                health,
            }
        })
        .collect();
    Json(rows)
}

#[derive(Debug, Deserialize)]
pub struct DeployNodeServerBody {
    mgmt_port: u16,
    grpc_port: u16,
    #[serde(default)]
    binary: Option<String>,
    #[serde(default)]
    election_profile: Option<String>,
    /// `--kv-backend` value (e.g. `"file"`, `"block"`, `"mem-block"`).
    #[serde(default)]
    kv_backend: Option<String>,
    /// `--wal-backend` value (e.g. `"file"`, `"mem-block"`, `"block-device"`).
    #[serde(default)]
    wal_backend: Option<String>,
    /// Sets `--no-fsync` on the spawned server when `true`.
    #[serde(default)]
    no_fsync: bool,
    /// `--metrics-interval` value in seconds.
    #[serde(default)]
    metrics_interval: Option<u64>,
    /// `--max-inflight` value for the proposal admission window.
    #[serde(default)]
    max_inflight: Option<usize>,
    /// `--coalesce-max-keys` value for R45 proposal coalescing.
    #[serde(default)]
    coalesce_max_keys: Option<usize>,
    /// `--coalesce-drain-threshold` value for R45b drain heuristic.
    #[serde(default)]
    coalesce_drain_threshold: Option<usize>,
    /// Optional `--config` JSON path passed to the spawned `crow-kv-server`.
    #[serde(default)]
    config: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct DeployResult {
    node_id: NodeId,
    mgmt_url: String,
    grpc_url: String,
    pid: u32,
}

/// `GET /api/nodes/:node_id/server`. Runtime info; 404 if not deployed.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns `404` if no server is deployed on this node.
pub async fn http_get_node_server(
    State(state): State<AppState>,
    Path(node_id): Path<u64>,
    Recursive(_depth): Recursive,
) -> Result<Json<ServerEntry>, (StatusCode, Json<ErrorBody>)> {
    let mut entry = {
        let cfg = state.config.read().unwrap();
        cfg.server_for_node(node_id).cloned().ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ErrorBody {
                    error: format!("no server deployed on node {node_id}"),
                }),
            )
        })?
    };
    entry.pid = state.runtime_pid(node_id);
    Ok(Json(entry))
}

/// `POST /api/nodes/:node_id/server/deploy`. Spawn `crow-kv-server` on
/// the node (local fork for `ssh_user=""`, SSH otherwise), wait for
/// health, persist the deployment record.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns an error if deployment, config persistence, or node lookup fails.
pub async fn http_deploy_node_server(
    State(state): State<AppState>,
    Path(node_id): Path<u64>,
    Json(body): Json<DeployNodeServerBody>,
) -> Result<(StatusCode, Json<DeployResult>), (StatusCode, Json<ErrorBody>)> {
    use crow_console_shared::lifecycle::{self, DeployRequest};

    let node = {
        let cfg = state.config.read().unwrap();
        if cfg.server_for_node(node_id).is_some() {
            return Err((
                StatusCode::CONFLICT,
                Json(ErrorBody {
                    error: format!("node {node_id} already hosts a deployed server"),
                }),
            ));
        }
        cfg.node(node_id).cloned().ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ErrorBody {
                    error: format!("node {node_id} not found"),
                }),
            )
        })?
    };

    let req = DeployRequest {
        server_id: node_id.to_string(),
        mgmt_port: body.mgmt_port,
        grpc_port: body.grpc_port,
        election_profile: body
            .election_profile
            .clone()
            .or_else(|| std::env::var("CROW_KV_SERVER_ELECTION_PROFILE").ok()),
        binary: body.binary.clone().map(std::path::PathBuf::from),
        kv_backend: body.kv_backend.clone(),
        wal_backend: body.wal_backend.clone(),
        no_fsync: body.no_fsync,
        metrics_interval: body.metrics_interval,
        max_inflight: body.max_inflight,
        coalesce_max_keys: body.coalesce_max_keys,
        coalesce_drain_threshold: body.coalesce_drain_threshold,
        config: body.config.clone().map(std::path::PathBuf::from),
    };

    let deployed = if node.ssh_enabled() {
        let server_bin = body.binary.clone().unwrap_or_else(|| {
            std::env::var("CROW_KV_SERVER_BIN").unwrap_or_else(|_| "crow-kv-server".to_string())
        });
        crow_console_shared::ssh::deploy_via_ssh(&req, &node, &server_bin)
            .await
            .map_err(|e| err_502(format!("ssh deploy: {e}")))?
    } else {
        let workspace_dir = state
            .prepare_node_workspace(node_id)
            .map_err(|e| err_500(e.to_string()))?;
        lifecycle::deploy_local_in_dir(&req, &node, &workspace_dir)
            .await
            .map_err(|e| err_502(format!("local deploy: {e}")))?
    };

    let entry = ServerEntry {
        id: node_id.to_string(),
        url: deployed.mgmt_url.clone(),
        node_id: Some(node_id),
        grpc_url: Some(deployed.grpc_url.clone()),
        mgmt_port: Some(body.mgmt_port),
        grpc_port: Some(body.grpc_port),
        auto_start: true,
        binary: body.binary.clone(),
        election_profile: body
            .election_profile
            .clone()
            .or_else(|| std::env::var("CROW_KV_SERVER_ELECTION_PROFILE").ok()),
        pid: None,
    };
    state.set_runtime_pid(node_id, deployed.pid);
    {
        let mut cfg = state.config.write().unwrap();
        cfg.add_server(entry).map_err(map_config_err)?;
    }
    state.persist().map_err(map_persist_err)?;
    crate::mgmt::refresh_node_cache(&state, node_id).await;
    Ok((
        StatusCode::CREATED,
        Json(DeployResult {
            node_id,
            mgmt_url: deployed.mgmt_url,
            grpc_url: deployed.grpc_url,
            pid: deployed.pid,
        }),
    ))
}

/// `POST /api/nodes/:node_id/server/restart`. Stop the tracked
/// `crow-kv-server` process on this node (if any) and immediately
/// re-deploy on the same ports recorded in the `ServerEntry`. The
/// binary path falls back to `CROW_KV_SERVER_BIN` / `"crow-kv-server"`
/// the same way the initial deploy does when no `binary` override
/// is supplied. Returns the new `DeployResult`.
///
/// Idempotent in the sense that calling it when no process is
/// currently running still performs a deploy (so an operator can
/// recover from an out-of-band crash).
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns `404` if no server is registered for this node, `502` if
/// the SSH/local restart cycle fails.
pub async fn http_restart_node_server(
    State(state): State<AppState>,
    Path(node_id): Path<u64>,
) -> Result<Json<DeployResult>, (StatusCode, Json<ErrorBody>)> {
    use crow_console_shared::lifecycle::{self, DeployRequest};

    let (entry, node) = {
        let cfg = state.config.read().unwrap();
        let entry = cfg.server_for_node(node_id).cloned().ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ErrorBody {
                    error: format!("no server registered on node {node_id}"),
                }),
            )
        })?;
        let node = cfg.node(node_id).cloned().ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ErrorBody {
                    error: format!("node {node_id} not found"),
                }),
            )
        })?;
        (entry, node)
    };

    // Extract ports from the persisted URLs. The deploy path stamped
    // them in originally as host:port; if either fails to parse we
    // surface a 500 since that's a console-state-corruption case.
    let mgmt_port = port_of(&entry.url)
        .ok_or_else(|| err_500(format!("server entry has malformed mgmt_url: {}", entry.url)))?;
    let grpc_port = entry.grpc_url.as_deref().and_then(port_of).ok_or_else(|| {
        err_500(format!(
            "server entry has malformed grpc_url: {:?}",
            entry.grpc_url
        ))
    })?;

    if let Some(pid) = state.runtime_pid(node_id) {
        let _sent = match &node {
            n if n.ssh_enabled() => crow_console_shared::ssh::stop_via_ssh(n, pid)
                .await
                .map_err(|e| err_502(format!("ssh stop (restart): {e}")))?,
            _ => tokio::task::spawn_blocking(move || lifecycle::stop_pid(pid))
                .await
                .map_err(|e| err_500(format!("spawn_blocking (restart): {e}")))?
                .unwrap_or(false),
        };
    }

    let req = DeployRequest {
        server_id: node_id.to_string(),
        mgmt_port,
        grpc_port,
        election_profile: entry
            .election_profile
            .clone()
            .or_else(|| std::env::var("CROW_KV_SERVER_ELECTION_PROFILE").ok()),
        binary: None,
        ..Default::default()
    };
    let deployed = if node.ssh_enabled() {
        let server_bin = std::env::var("CROW_KV_SERVER_BIN").unwrap_or_else(|_| "crow-kv-server".to_string());
        crow_console_shared::ssh::deploy_via_ssh(&req, &node, &server_bin)
            .await
            .map_err(|e| err_502(format!("ssh redeploy (restart): {e}")))?
    } else {
        let workspace_dir = state
            .prepare_node_workspace(node_id)
            .map_err(|e| err_500(e.to_string()))?;
        lifecycle::deploy_local_in_dir(&req, &node, &workspace_dir)
            .await
            .map_err(|e| err_502(format!("local redeploy (restart): {e}")))?
    };

    let new_entry = ServerEntry {
        id: node_id.to_string(),
        url: deployed.mgmt_url.clone(),
        node_id: Some(node_id),
        grpc_url: Some(deployed.grpc_url.clone()),
        mgmt_port: entry.mgmt_port,
        grpc_port: entry.grpc_port,
        auto_start: entry.auto_start,
        binary: entry.binary.clone(),
        election_profile: entry.election_profile.clone(),
        pid: None,
    };
    state.set_runtime_pid(node_id, deployed.pid);
    {
        let mut cfg = state.config.write().unwrap();
        // The old entry is still keyed by node_id; replace it.
        let _ = cfg.remove_server_for_node(node_id);
        cfg.add_server(new_entry).map_err(map_config_err)?;
    }
    state.persist().map_err(map_persist_err)?;
    crate::mgmt::restore_persisted_topology_for_node(&state, node_id)
        .await
        .map_err(|e| err_502(format!("restore topology after restart: {e}")))?;

    Ok(Json(DeployResult {
        node_id,
        mgmt_url: deployed.mgmt_url,
        grpc_url: deployed.grpc_url,
        pid: deployed.pid,
    }))
}

/// Parse `port` out of a URL like `http://host:9910` or `host:9910`.
/// Returns `None` on any shape we don't recognise.
fn port_of(url: &str) -> Option<u16> {
    // Strip scheme if present.
    let stripped = url
        .strip_prefix("http://")
        .or_else(|| url.strip_prefix("https://"))
        .unwrap_or(url);
    let host_port = stripped.split('/').next().unwrap_or(stripped);
    let port_str = host_port.rsplit(':').next()?;
    port_str.parse::<u16>().ok()
}

#[derive(Debug, Serialize)]
pub struct StopResult {
    sent: bool,
}

/// `POST /api/nodes/:node_id/server/stop`. Stop the server on this node
/// but keep the deployment record so the console can restart / restore it.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns an error if the server is not found or has no tracked pid.
pub async fn http_stop_node_server(
    State(state): State<AppState>,
    Path(node_id): Path<u64>,
) -> Result<Json<StopResult>, (StatusCode, Json<ErrorBody>)> {
    use crow_console_shared::lifecycle;

    let node = {
        let cfg = state.config.read().unwrap();
        let _entry = cfg.server_for_node(node_id).cloned().ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ErrorBody {
                    error: format!("no server deployed on node {node_id}"),
                }),
            )
        })?;
        cfg.node(node_id).cloned()
    };
    let Some(pid) = state.runtime_pid(node_id) else {
        return Err(err_400(format!("server on node {node_id} has no tracked pid")));
    };
    let sent = match node {
        Some(n) if n.ssh_enabled() => crow_console_shared::ssh::stop_via_ssh(&n, pid)
            .await
            .map_err(|e| err_502(format!("ssh stop: {e}")))?,
        _ => tokio::task::spawn_blocking(move || lifecycle::stop_pid(pid))
            .await
            .map_err(|e| err_500(format!("spawn_blocking: {e}")))?
            .map_err(|e| err_500(format!("stop_pid: {e}")))?,
    };
    state.clear_runtime_pid(node_id);
    state.monitor_cache.drop_node(&node_id).await;
    Ok(Json(StopResult { sent }))
}

/// `DELETE /api/nodes/:node_id/server`. Stop and remove the deployment
/// record. Returns 204 on success, 404 if no server is deployed.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns `404` if no server is deployed on this node.
pub async fn http_delete_node_server(
    State(state): State<AppState>,
    Path(node_id): Path<u64>,
) -> Result<StatusCode, (StatusCode, Json<ErrorBody>)> {
    use crow_console_shared::lifecycle;

    let node = {
        let cfg = state.config.read().unwrap();
        let _entry = cfg.server_for_node(node_id).cloned().ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ErrorBody {
                    error: format!("no server deployed on node {node_id}"),
                }),
            )
        })?;
        cfg.node(node_id).cloned()
    };
    if let Some(pid) = state.runtime_pid(node_id) {
        let _ = match node {
            Some(n) if n.ssh_enabled() => crow_console_shared::ssh::stop_via_ssh(&n, pid)
                .await
                .unwrap_or(false),
            _ => {
                matches!(
                    tokio::task::spawn_blocking(move || lifecycle::stop_pid(pid)).await,
                    Ok(Ok(true))
                )
            }
        };
    }
    {
        let mut cfg = state.config.write().unwrap();
        let _ = cfg.remove_server_for_node(node_id);
        cfg.purge_node_topology(node_id);
    }
    state.clear_runtime_pid(node_id);
    state.persist().map_err(map_persist_err)?;
    Ok(StatusCode::NO_CONTENT)
}

// ── Per-node OpenAPI proxy ───────────────────────────────────────────

const OPENAPI_CACHE_TTL: Duration = Duration::from_secs(300);

/// `GET /api/nodes/:node_id/openapi.json`. Reverse-proxy the node's
/// management API `/openapi.json` endpoint. Uses the same TTL cache as
/// the old global proxy.
///
/// # Panics
/// Panics if the `RwLock` or `Mutex` is poisoned.
///
/// # Errors
/// Returns an error if no server is deployed or the upstream request fails.
pub async fn http_node_openapi_proxy(
    State(state): State<AppState>,
    Path(node_id): Path<u64>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorBody>)> {
    let mgmt_url = {
        let cfg = state.config.read().unwrap();
        let entry = cfg.server_for_node(node_id).ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ErrorBody {
                    error: format!("no server deployed on node {node_id}"),
                }),
            )
        })?;
        entry.url.clone()
    };

    // Check cache
    {
        let cache = state.openapi_cache.lock().unwrap();
        if let Some((value, timestamp)) = cache.get(&node_id) {
            if timestamp.elapsed() < OPENAPI_CACHE_TTL {
                return Ok(Json(value.clone()));
            }
        }
    }

    let url = format!("{}/openapi.json", mgmt_url.trim_end_matches('/'));
    let cid = crow_console_shared::corr_id::current_or_new();
    let started = std::time::Instant::now();
    let resp = reqwest::Client::new()
        .get(&url)
        .header(crow_console_shared::corr_id::HEADER, &cid)
        .send()
        .await
        .map_err(|e| {
            crow_console_shared::ops_log::append_http(
                &cid,
                "GET",
                &url,
                0,
                started.elapsed().as_millis(),
                Some(&format!("transport error: {e}")),
            );
            err_502(format!("openapi proxy: {e}"))
        })?;
    let upstream_status = resp.status();
    crow_console_shared::ops_log::append_http(
        &cid,
        "GET",
        &url,
        upstream_status.as_u16(),
        started.elapsed().as_millis(),
        None,
    );
    if !upstream_status.is_success() {
        return Err(err_502(format!("openapi proxy: upstream {upstream_status}")));
    }
    let value = resp
        .json::<serde_json::Value>()
        .await
        .map_err(|e| err_502(format!("openapi proxy: parse: {e}")))?;

    {
        let mut cache = state.openapi_cache.lock().unwrap();
        cache.insert(node_id, (value.clone(), std::time::Instant::now()));
    }

    Ok(Json(value))
}

/// `POST /internal/reset`. Tear down the entire cluster in dependency
/// order: groups → stores → server processes → nodes → racks, then
/// clear workspace dirs and caches. Intended for E2E test fixtures;
/// never exposed in the public API surface.
///
/// # Panics
/// Panics if the `RwLock` or `Mutex` is poisoned.
///
/// # Errors
/// Returns an error if workspace cleanup or config persistence fails.
pub async fn http_internal_reset(
    State(state): State<AppState>,
) -> Result<Json<ResetResult>, (StatusCode, Json<ErrorBody>)> {
    use crow_console_shared::lifecycle;

    // 1. List all stores from the monitor cache.
    let stores: Vec<u64> = {
        let snap = state.monitor_cache.snapshot().await;
        let mut ids: Vec<u64> = snap.values().flat_map(|rec| rec.stores.keys().copied()).collect();
        ids.sort_unstable();
        ids.dedup();
        ids
    };

    let mut stopped: Vec<String> = Vec::new();

    // 2. For each store: list & remove all groups, then remove the store.
    for sid in &stores {
        // List groups for this store.
        if let Some(view) = state.monitor_cache.resolve_store(*sid).await {
            let group_ids: Vec<u64> = view.groups.iter().map(|g| g.group_id).collect();
            for gid in group_ids {
                // Remove group: RPC to each node + config cleanup.
                if let Some(gv) = state.monitor_cache.resolve_group(*sid, gid).await {
                    let node_ids: Vec<NodeId> = gv.replicas.iter().map(|r| r.node_id).collect();
                    for nid in &node_ids {
                        if let Ok(url) = crate::mgmt::mgmt_url_for_node(&state, *nid) {
                            if let Ok(client) = crate::mgmt::build_server_client(url) {
                                let _ = client.remove_group(*sid, gid).await;
                            }
                        }
                    }
                    for nid in &node_ids {
                        crate::mgmt::refresh_node_cache(&state, *nid).await;
                    }
                }
                {
                    let mut cfg = state.config.write().unwrap();
                    cfg.remove_group_record(*sid, gid);
                }
            }
        }

        // Remove the store from each node.
        if let Some(view) = state.monitor_cache.resolve_store(*sid).await {
            for nid in &view.nodes {
                if let Ok(url) = crate::mgmt::mgmt_url_for_node(&state, *nid) {
                    if let Ok(client) = crate::mgmt::build_server_client(url) {
                        let _ = client.remove_store(*sid).await;
                    }
                }
            }
            for nid in &view.nodes {
                crate::mgmt::refresh_node_cache(&state, *nid).await;
            }
        }
        {
            let mut cfg = state.config.write().unwrap();
            cfg.remove_store_record(*sid);
        }
    }

    // 3. List all nodes, stop their servers, then remove them.
    let node_ids: Vec<NodeId> = {
        let cfg = state.config.read().unwrap();
        cfg.nodes.iter().map(|n| n.id).collect()
    };

    for nid in &node_ids {
        // Stop the server process if a PID is tracked.
        if let Some(pid) = state.runtime_pid(nid) {
            let ssh = state
                .config
                .read()
                .unwrap()
                .node(*nid)
                .is_some_and(crow_console_shared::config::NodeEntry::ssh_enabled);
            let sent = if ssh {
                false
            } else {
                matches!(
                    tokio::task::spawn_blocking(move || lifecycle::stop_pid(pid)).await,
                    Ok(Ok(true))
                )
            };
            if sent {
                stopped.push(nid.to_string());
            }
            state.clear_runtime_pid(nid);
        }

        // Remove the server entry + purge topology from config.
        {
            let mut cfg = state.config.write().unwrap();
            let _ = cfg.remove_server_for_node(*nid);
            cfg.purge_node_topology(*nid);
            let _ = cfg.remove_node(*nid);
        }

        state.monitor_cache.drop_node(nid).await;
    }

    // 4. Remove all racks.
    let rack_ids: Vec<String> = {
        let cfg = state.config.read().unwrap();
        cfg.racks.iter().map(|r| r.id.to_string()).collect()
    };
    {
        let mut cfg = state.config.write().unwrap();
        for rid in &rack_ids {
            let _ = cfg.remove_rack(rid.parse().unwrap());
        }
    }

    // 5. Clear caches and workspace directories.
    state.openapi_cache.lock().unwrap().clear();
    state
        .clear_workspaces()
        .map_err(|e| err_500(format!("clear workspaces: {e}")))?;
    state.persist().map_err(map_persist_err)?;

    Ok(Json(ResetResult { stopped }))
}

#[derive(Serialize)]
pub struct ResetResult {
    pub stopped: Vec<String>,
}

// ── Disk-group lifecycle ──────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct AddDiskGroupBody {
    id: DiskGroupId,
    #[serde(default)]
    name: String,
}

/// `GET /api/nodes/:node_id/disk-groups`.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns `404` if the node does not exist.
pub async fn http_list_node_disk_groups(
    State(state): State<AppState>,
    Path(node_id): Path<NodeId>,
) -> Result<Json<Vec<DiskGroupEntry>>, (StatusCode, Json<ErrorBody>)> {
    let cfg = state.config.read().unwrap();
    if !cfg.nodes.iter().any(|n| n.id == node_id) {
        return Err((
            StatusCode::NOT_FOUND,
            Json(ErrorBody {
                error: format!("node {node_id} not found"),
            }),
        ));
    }
    Ok(Json(
        cfg.disk_groups
            .iter()
            .filter(|dg| dg.node_id == node_id)
            .cloned()
            .collect(),
    ))
}

/// `GET /api/nodes/:node_id/disk-groups/:dg_id`.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns `404` if the disk-group does not exist.
pub async fn http_get_node_disk_group(
    State(state): State<AppState>,
    Path((node_id, dg_id)): Path<(NodeId, DiskGroupId)>,
) -> Result<Json<DiskGroupEntry>, (StatusCode, Json<ErrorBody>)> {
    let cfg = state.config.read().unwrap();
    let dg = cfg
        .disk_groups
        .iter()
        .find(|dg| dg.node_id == node_id && dg.id == dg_id)
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ErrorBody {
                    error: format!("disk-group {dg_id} not found on node {node_id}"),
                }),
            )
        })?;
    Ok(Json(dg.clone()))
}

/// `POST /api/nodes/:node_id/disk-groups`.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns an error if disk-group addition or config persistence fails.
pub async fn http_add_node_disk_group(
    State(state): State<AppState>,
    Path(node_id): Path<NodeId>,
    Json(body): Json<AddDiskGroupBody>,
) -> Result<(StatusCode, Json<DiskGroupEntry>), (StatusCode, Json<ErrorBody>)> {
    // Resolve rack_id from the node.
    let rack_id = {
        let cfg = state.config.read().unwrap();
        cfg.node(node_id).map(|n| n.rack_id).ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ErrorBody {
                    error: format!("node {node_id} not found"),
                }),
            )
        })?
    };
    let entry = DiskGroupEntry {
        id: body.id,
        rack_id,
        node_id,
        name: body.name,
    };
    {
        let mut cfg = state.config.write().unwrap();
        cfg.add_disk_group(entry.clone()).map_err(map_config_err)?;
    }
    state.persist().map_err(map_persist_err)?;

    // Sync group-0 sysdata. Best-effort.
    if let Some(hw) = crate::mgmt::build_hardware_client(&state).await {
        let value = crow_protocol::diskdb::rpc::DiskGroupValue {
            status: crow_protocol::common::HwStatus::Up as i32,
            disk_ids: Vec::new(),
        };
        if let Err(e) = hw.add_disk_group(rack_id, node_id, entry.id, &value).await {
            tracing::warn!(disk_group_id = entry.id, error = %e, "sysdata sync: add_disk_group failed");
        }
    }

    Ok((StatusCode::CREATED, Json(entry)))
}

/// `DELETE /api/nodes/:node_id/disk-groups/:dg_id`.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns an error if disk-group removal or config persistence fails.
pub async fn http_remove_node_disk_group(
    State(state): State<AppState>,
    Path((node_id, dg_id)): Path<(NodeId, DiskGroupId)>,
) -> Result<StatusCode, (StatusCode, Json<ErrorBody>)> {
    // Capture rack_id before removing.
    let rack_id = {
        let cfg = state.config.read().unwrap();
        cfg.disk_groups
            .iter()
            .find(|dg| dg.node_id == node_id && dg.id == dg_id)
            .map(|dg| dg.rack_id)
    };
    {
        let mut cfg = state.config.write().unwrap();
        cfg.remove_disk_group(dg_id).map_err(map_config_err)?;
    }
    state.persist().map_err(map_persist_err)?;

    // Cascade-remove group-0 sysdata.
    if let (Some(hw), Some(rack_id)) = (crate::mgmt::build_hardware_client(&state).await, rack_id) {
        if let Err(e) = hw.remove_disk_group_cascade(rack_id, node_id, dg_id).await {
            tracing::warn!(disk_group_id = dg_id, error = %e, "sysdata sync: remove_disk_group_cascade failed");
        }
    }

    Ok(StatusCode::NO_CONTENT)
}

// ── Disk lifecycle ────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct AddDiskBody {
    disk_id: String,
    disk_type: String,
    capacity_bytes: u64,
    zone_size_bytes: u64,
    unit_size_bytes: u32,
}

/// `GET /api/nodes/:node_id/disk-groups/:dg_id/disks`.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns `404` if the disk-group does not exist.
pub async fn http_list_disks_in_group(
    State(state): State<AppState>,
    Path((node_id, dg_id)): Path<(NodeId, DiskGroupId)>,
) -> Result<Json<Vec<DiskEntry>>, (StatusCode, Json<ErrorBody>)> {
    let cfg = state.config.read().unwrap();
    if !cfg
        .disk_groups
        .iter()
        .any(|dg| dg.node_id == node_id && dg.id == dg_id)
    {
        return Err((
            StatusCode::NOT_FOUND,
            Json(ErrorBody {
                error: format!("disk-group {dg_id} not found on node {node_id}"),
            }),
        ));
    }
    Ok(Json(
        cfg.disks
            .iter()
            .filter(|d| d.disk_group_id == dg_id && d.node_id == node_id)
            .cloned()
            .collect(),
    ))
}

/// `GET /api/nodes/:node_id/disk-groups/:dg_id/disks/:disk_id`.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns `404` if the disk does not exist.
pub async fn http_get_disk(
    State(state): State<AppState>,
    Path((node_id, dg_id, disk_id)): Path<(NodeId, DiskGroupId, String)>,
) -> Result<Json<DiskEntry>, (StatusCode, Json<ErrorBody>)> {
    let cfg = state.config.read().unwrap();
    let disk = cfg
        .disks
        .iter()
        .find(|d| d.node_id == node_id && d.disk_group_id == dg_id && d.disk_id == disk_id)
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ErrorBody {
                    error: format!("disk {disk_id} not found"),
                }),
            )
        })?;
    Ok(Json(disk.clone()))
}

/// `POST /api/nodes/:node_id/disk-groups/:dg_id/disks`.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns an error if disk addition or config persistence fails.
pub async fn http_add_disk(
    State(state): State<AppState>,
    Path((node_id, dg_id)): Path<(NodeId, DiskGroupId)>,
    Json(body): Json<AddDiskBody>,
) -> Result<(StatusCode, Json<DiskEntry>), (StatusCode, Json<ErrorBody>)> {
    // Resolve rack_id from the disk-group.
    let rack_id = {
        let cfg = state.config.read().unwrap();
        cfg.disk_groups
            .iter()
            .find(|dg| dg.node_id == node_id && dg.id == dg_id)
            .map(|dg| dg.rack_id)
            .ok_or_else(|| {
                (
                    StatusCode::NOT_FOUND,
                    Json(ErrorBody {
                        error: format!("disk-group {dg_id} not found on node {node_id}"),
                    }),
                )
            })?
    };
    let entry = DiskEntry {
        disk_id: body.disk_id,
        disk_group_id: dg_id,
        rack_id,
        node_id,
        disk_type: body.disk_type,
        capacity_bytes: body.capacity_bytes,
        zone_size_bytes: body.zone_size_bytes,
        unit_size_bytes: body.unit_size_bytes,
    };
    {
        let mut cfg = state.config.write().unwrap();
        cfg.add_disk(entry.clone()).map_err(map_config_err)?;
    }
    state.persist().map_err(map_persist_err)?;

    // Sync group-0 sysdata. Best-effort.
    if let Some(hw) = crate::mgmt::build_hardware_client(&state).await {
        let disk_id = match <crow_protocol::common::DiskId as crow_protocol::diskdb_type_util::DiskIdExt>::from_display_string(&entry.disk_id) {
            Ok(id) => id,
            Err(e) => {
                tracing::warn!(disk_id = %entry.disk_id, error = %e, "sysdata sync: invalid disk_id format");
                return Ok((StatusCode::CREATED, Json(entry)));
            }
        };
        let unit_size = u64::from(entry.unit_size_bytes);
        let value = crow_protocol::diskdb::rpc::DiskValue {
            status: crow_protocol::common::HwStatus::Up as i32,
            disk_type: match entry.disk_type.as_str() {
                "Ssd" | "BLOCK_SSD" => crow_protocol::diskdb::rpc::DiskType::BlockSsd as i32,
                "ZONE_SSD" => crow_protocol::diskdb::rpc::DiskType::ZoneSsd as i32,
                "SMR_HDD" => crow_protocol::diskdb::rpc::DiskType::SmrHdd as i32,
                _ => crow_protocol::diskdb::rpc::DiskType::BlockHdd as i32,
            },
            capacity_units: entry.capacity_bytes.checked_div(unit_size).unwrap_or(0),
            zone_size_units: entry.zone_size_bytes.checked_div(unit_size).unwrap_or(0),
            unit_size_bytes: entry.unit_size_bytes,
            zone_count: 0,
        };
        if let Err(e) = hw.add_disk(rack_id, node_id, dg_id, &disk_id, &value).await {
            tracing::warn!(disk_id = %entry.disk_id, error = %e, "sysdata sync: add_disk failed");
        }
    }

    Ok((StatusCode::CREATED, Json(entry)))
}

/// `DELETE /api/nodes/:node_id/disk-groups/:dg_id/disks/:disk_id`.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns an error if disk removal or config persistence fails.
pub async fn http_remove_disk(
    State(state): State<AppState>,
    Path((node_id, dg_id, disk_id)): Path<(NodeId, DiskGroupId, String)>,
) -> Result<StatusCode, (StatusCode, Json<ErrorBody>)> {
    // Capture rack_id before removing.
    let rack_id = {
        let cfg = state.config.read().unwrap();
        cfg.disks
            .iter()
            .find(|d| d.node_id == node_id && d.disk_group_id == dg_id && d.disk_id == disk_id)
            .map(|d| d.rack_id)
    };
    {
        let mut cfg = state.config.write().unwrap();
        cfg.remove_disk(&disk_id).map_err(map_config_err)?;
    }
    state.persist().map_err(map_persist_err)?;

    // Cascade-remove group-0 sysdata.
    if let (Some(hw), Some(rack_id)) = (crate::mgmt::build_hardware_client(&state).await, rack_id) {
        match <crow_protocol::common::DiskId as crow_protocol::diskdb_type_util::DiskIdExt>::from_display_string(&disk_id) {
            Ok(disk_id_proto) => {
                if let Err(e) = hw.remove_disk_cascade(rack_id, node_id, dg_id, &disk_id_proto).await {
                    tracing::warn!(disk_id = %disk_id, error = %e, "sysdata sync: remove_disk_cascade failed");
                }
            }
            Err(e) => {
                tracing::warn!(disk_id = %disk_id, error = %e, "sysdata sync: invalid disk_id format, skipping cascade");
            }
        }
    }

    Ok(StatusCode::NO_CONTENT)
}

// ── Disk move ────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
#[allow(clippy::struct_field_names)]
pub struct MoveDiskBody {
    new_rack_id: RackId,
    new_node_id: NodeId,
    new_disk_group_id: DiskGroupId,
}

/// `POST /api/disks/:disk_id/move`.
///
/// Moves a disk from its current placement to a new disk-group. The
/// disk's records (zone/busy/free) are copied from the old bind to
/// the new bind, then the group-0 placement is updated.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns an error if the disk is not found, the new disk-group is
/// not found, or the record copy / group-0 update fails.
#[allow(clippy::too_many_lines)]
pub async fn http_move_disk(
    State(state): State<AppState>,
    Path(disk_id): Path<String>,
    Json(body): Json<MoveDiskBody>,
) -> Result<Json<DiskEntry>, (StatusCode, Json<ErrorBody>)> {
    // 1. Resolve the disk's current placement from config.
    let (old_rack_id, old_node_id, old_dg_id, disk_entry) = {
        let cfg = state.config.read().unwrap();
        let entry = cfg.disk(&disk_id).ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ErrorBody {
                    error: format!("disk {disk_id} not found"),
                }),
            )
        })?;
        (entry.rack_id, entry.node_id, entry.disk_group_id, entry.clone())
    };

    // 2. Verify the new disk-group exists in config.
    {
        let cfg = state.config.read().unwrap();
        if !cfg
            .disk_groups
            .iter()
            .any(|dg| dg.id == body.new_disk_group_id && dg.node_id == body.new_node_id)
        {
            return Err((
                StatusCode::NOT_FOUND,
                Json(ErrorBody {
                    error: format!(
                        "disk-group {} on node {} not found",
                        body.new_disk_group_id, body.new_node_id
                    ),
                }),
            ));
        }
    }

    // 3. Parse the disk_id into a proto DiskId.
    let disk_id_proto = match <crow_protocol::common::DiskId as crow_protocol::diskdb_type_util::DiskIdExt>::from_display_string(&disk_id)
    {
        Ok(id) => id,
        Err(e) => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(ErrorBody { error: format!("invalid disk_id format: {e}") }),
            ));
        }
    };

    // 4. Set disk to Maintenance in group 0 (old placement).
    if let Some(hw) = crate::mgmt::build_hardware_client(&state).await {
        if let Err(e) = hw
            .set_disk_status(
                old_rack_id,
                old_node_id,
                old_dg_id,
                &disk_id_proto,
                crow_protocol::common::HwStatus::Maintenance,
            )
            .await
        {
            tracing::warn!(disk_id = %disk_id, error = %e, "move: set Maintenance failed");
        }
    }

    // 5. Copy records from old bind to new bind.
    if let Some(hw) = crate::mgmt::build_hardware_client(&state).await {
        let old_bind = match hw.get_bind(old_rack_id, old_node_id, old_dg_id).await {
            Ok(Some(bind)) => (bind.store_id, bind.group_id),
            Ok(None) => {
                return Err((
                    StatusCode::CONFLICT,
                    Json(ErrorBody {
                        error: format!(
                            "no bind for old placement ({old_rack_id}, {old_node_id}, {old_dg_id})"
                        ),
                    }),
                ));
            }
            Err(e) => {
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorBody {
                        error: format!("get old bind: {e}"),
                    }),
                ));
            }
        };
        let new_bind = match hw
            .get_bind(body.new_rack_id, body.new_node_id, body.new_disk_group_id)
            .await
        {
            Ok(Some(bind)) => (bind.store_id, bind.group_id),
            Ok(None) => {
                return Err((
                    StatusCode::CONFLICT,
                    Json(ErrorBody {
                        error: format!(
                            "no bind for new placement ({}, {}, {}); create bind before move",
                            body.new_rack_id, body.new_node_id, body.new_disk_group_id
                        ),
                    }),
                ));
            }
            Err(e) => {
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorBody {
                        error: format!("get new bind: {e}"),
                    }),
                ));
            }
        };

        // Build a CrowkvClient seeded with both old and new bind leaders.
        let kv = crow_kv_client::CrowkvClient::new(crow_kv_client::ClientConfig::new(Vec::new()));
        if let Some(ep) = crate::mgmt::grpc_endpoint_for_node(&state, old_node_id, old_bind.0).await {
            kv.seed_leader(old_bind.0, old_bind.1, ep);
        }
        if let Some(ep) = crate::mgmt::grpc_endpoint_for_node(&state, body.new_node_id, new_bind.0).await {
            kv.seed_leader(new_bind.0, new_bind.1, ep);
        }
        let copy_count = copy_disk_records(&kv, old_bind, new_bind, &disk_id_proto).await;
        tracing::info!(disk_id = %disk_id, records_copied = copy_count, "move: records copied");
    }

    // 6. Update group 0 placement: remove from old, add to new.
    if let Some(hw) = crate::mgmt::build_hardware_client(&state).await {
        // Get the current DiskValue from group 0.
        if let Ok(Some(disk_value)) = hw
            .get_disk(old_rack_id, old_node_id, old_dg_id, &disk_id_proto)
            .await
        {
            // Remove from old placement.
            if let Err(e) = hw
                .remove_disk(old_rack_id, old_node_id, old_dg_id, &disk_id_proto)
                .await
            {
                tracing::warn!(disk_id = %disk_id, error = %e, "move: remove from old placement failed");
            }
            // Add to new placement with Maintenance status.
            let mut new_value = disk_value;
            new_value.status = crow_protocol::common::HwStatus::Maintenance as i32;
            if let Err(e) = hw
                .add_disk(
                    body.new_rack_id,
                    body.new_node_id,
                    body.new_disk_group_id,
                    &disk_id_proto,
                    &new_value,
                )
                .await
            {
                tracing::warn!(disk_id = %disk_id, error = %e, "move: add to new placement failed");
            }
        }
    }

    // 7. Update ConsoleConfig: move the DiskEntry to the new disk-group.
    let updated_entry = {
        let mut cfg = state.config.write().unwrap();
        cfg.remove_disk(&disk_id).map_err(map_config_err)?;
        let new_entry = DiskEntry {
            disk_id: disk_entry.disk_id,
            disk_group_id: body.new_disk_group_id,
            rack_id: body.new_rack_id,
            node_id: body.new_node_id,
            disk_type: disk_entry.disk_type,
            capacity_bytes: disk_entry.capacity_bytes,
            zone_size_bytes: disk_entry.zone_size_bytes,
            unit_size_bytes: disk_entry.unit_size_bytes,
        };
        cfg.add_disk(new_entry.clone()).map_err(map_config_err)?;
        new_entry
    };
    state.persist().map_err(map_persist_err)?;

    Ok(Json(updated_entry))
}

/// Copy all records for a disk from one bind to another. Scans zone,
/// busy, and free block records by `DiskId` prefix, batch-writes them
/// to the new bind.
async fn copy_disk_records(
    kv: &crow_kv_client::CrowkvClient,
    old_bind: (u64, u64),
    new_bind: (u64, u64),
    disk_id: &crow_protocol::common::DiskId,
) -> u64 {
    use crow_protocol::key::{BusyBlockKey, FreeBlockKey, ZoneKey};

    let prefixes = [
        ZoneKey::prefix_for_disk(disk_id),
        BusyBlockKey::prefix_for_disk(disk_id),
        FreeBlockKey::prefix_for_disk(disk_id),
    ];

    let mut total_copied: u64 = 0;
    let batch_size = 100u32;

    for prefix in &prefixes {
        let mut start_after: Vec<u8> = Vec::new();
        loop {
            let outcome = match kv
                .scan(
                    old_bind.0,
                    old_bind.1,
                    prefix,
                    &start_after,
                    &[], // empty end_key = no upper bound
                    batch_size,
                    crow_kv_client::ReadMode::Linearizable,
                    None,
                    false,
                    None,
                )
                .await
            {
                Ok(o) => o,
                Err(e) => {
                    tracing::warn!(error = %e, "copy_disk_records: scan failed");
                    break;
                }
            };

            if outcome.items.is_empty() {
                break;
            }

            // Batch-write to new bind.
            let ops: Vec<crow_kv_client::BatchOp> = outcome
                .items
                .iter()
                .map(|(k, v)| crow_kv_client::BatchOp::Put {
                    key: k.clone(),
                    value: v.clone(),
                })
                .collect();

            if let Err(e) = kv.batch_write(new_bind.0, new_bind.1, &ops).await {
                tracing::warn!(error = %e, "copy_disk_records: batch_write failed");
                break;
            }

            total_copied += u64::try_from(outcome.items.len()).unwrap_or(0);

            if !outcome.truncated {
                break;
            }
            // Set start_after to the last key for pagination.
            if let Some((last_key, _)) = outcome.items.last() {
                start_after = last_key.to_vec();
            } else {
                break;
            }
        }
    }

    total_copied
}
