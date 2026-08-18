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
use crow_console_shared::config::{
    DiskEntry, DiskGroupEntry, NodeEntry, RackEntry, ServerEntry, ServiceType,
};
use crow_console_shared::expand::RecursiveDepth;
use crow_console_shared::monitor::NodeRecord;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
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
        // Override health to Down if no PID is tracked — the process
        // was stopped or the console restarted. The monitor cache may
        // be stale (no background polling to update it).
        if proc.pid.is_none() {
            proc.health = NodeHealth::Down;
            proc.state = ProcState::Failed;
        }
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
    // Cascade-stop the server process and drop its deployment record +
    // topology before removing the node, so a direct DELETE /api/nodes/:id
    // does not orphan a running crow-kv-server. No-op when no server is
    // deployed (e.g. the UI already called DELETE .../server first).
    stop_and_remove_server_for_node(&state, id).await;
    {
        let mut cfg = state.config.write().unwrap();
        cfg.remove_node(id).map_err(map_config_err)?;
    }
    state.monitor_cache.drop_node(&id).await;
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
    /// Service type: "kv" (crow-kv-server) or "diskdb".
    pub service_type: String,
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
            let pid = if s.service_type == ServiceType::Diskdb {
                s.node_id.and_then(|n| state.diskdb_runtime_pid(n.to_string()))
            } else {
                s.node_id.and_then(|n| state.runtime_pid(n.to_string()))
            };
            // KV health comes from the monitor cache (probed via the KV
            // server's /topology), overridden to Down when no PID is
            // tracked. DDB has no topology probe, so its health is derived
            // from PID presence alone — the shared node record reflects KV
            // health and must not flip the DDB badge when KV is stopped or
            // restarted while DDB keeps running.
            let health = if s.service_type == ServiceType::Diskdb {
                if pid.is_some() {
                    NodeHealth::Up
                } else {
                    NodeHealth::Down
                }
            } else if pid.is_some() {
                s.node_id
                    .and_then(|n| snap.get(&n))
                    .map_or(NodeHealth::Unknown, |rec| rec.health)
            } else {
                NodeHealth::Down
            };
            ServerSummary {
                node_id: s.node_id,
                mgmt_url: s.url.clone(),
                grpc_url: s.grpc_url.clone(),
                pid,
                health,
                service_type: match s.service_type {
                    crow_console_shared::config::ServiceType::Kv => "kv",
                    crow_console_shared::config::ServiceType::Diskdb => "diskdb",
                }
                .to_string(),
            }
        })
        .collect();
    Json(rows)
}

#[derive(Debug, Deserialize)]
pub struct DeployNodeServerBody {
    rest_port: u16,
    rpc_port: u16,
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
        rest_port: body.rest_port,
        rpc_port: body.rpc_port,
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
        rest_port: Some(body.rest_port),
        rpc_port: Some(body.rpc_port),
        auto_start: true,
        binary: body.binary.clone(),
        election_profile: body
            .election_profile
            .clone()
            .or_else(|| std::env::var("CROW_KV_SERVER_ELECTION_PROFILE").ok()),
        pid: None,
        service_type: ServiceType::Kv,
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
#[allow(clippy::too_many_lines)]
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
    let rest_port = crate::mgmt::port_of(&entry.url)
        .ok_or_else(|| err_500(format!("server entry has malformed mgmt_url: {}", entry.url)))?;
    let rpc_port = entry
        .grpc_url
        .as_deref()
        .and_then(crate::mgmt::port_of)
        .ok_or_else(|| {
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
        rest_port,
        rpc_port,
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
        rest_port: entry.rest_port,
        rpc_port: entry.rpc_port,
        auto_start: entry.auto_start,
        binary: entry.binary.clone(),
        election_profile: entry.election_profile.clone(),
        pid: None,
        service_type: entry.service_type,
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
    // Refresh the monitor cache so health badges reflect the restarted
    // server. The process may not be listening yet, so retry a few
    // times with short delays until the probe succeeds.
    crate::mgmt::refresh_node_cache(&state, node_id).await;
    let state_clone = state.clone();
    tokio::spawn(async move {
        for _ in 0..10 {
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            crate::mgmt::refresh_node_cache(&state_clone, node_id).await;
            let snap = state_clone.monitor_cache.snapshot().await;
            if let Some(rec) = snap.get(&node_id) {
                if rec.health == NodeHealth::Up {
                    break;
                }
            }
        }
    });

    Ok(Json(DeployResult {
        node_id,
        mgmt_url: deployed.mgmt_url,
        grpc_url: deployed.grpc_url,
        pid: deployed.pid,
    }))
}

#[derive(Debug, Serialize)]
pub struct StopResult {
    pub sent: bool,
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
    // Only mark the shared node record Down when no DDB instance is still
    // running on this node. The record is shared between KV and DDB; an
    // unconditional mark_down would flip the node-level badge (and any
    // DDB health derived from the record) even though DDB is unaffected.
    // The KV badge already drops via the no-pid override in
    // build_server_process / http_list_servers, and DDB health in
    // http_list_servers is derived from the DDB pid alone.
    if state.diskdb_runtime_pid(node_id).is_none() {
        state.monitor_cache.mark_down(node_id, "server stopped").await;
    }
    Ok(Json(StopResult { sent }))
}

/// Stop the server process deployed on `node_id` (best-effort) and remove
/// its deployment record + topology from config. Returns `true` if a
/// server was deployed (and is now removed), `false` if no server was
/// deployed on the node. Does NOT persist — the caller persists. Does NOT
/// remove the node itself. Used by both `DELETE /api/nodes/:id/server`
/// and the node-delete cascade so a direct `DELETE /api/nodes/:id` does
/// not orphan a running crow-kv-server.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
async fn stop_and_remove_server_for_node(state: &AppState, node_id: u64) -> bool {
    use crow_console_shared::lifecycle;

    let node = {
        let cfg = state.config.read().unwrap();
        if cfg.server_for_node(node_id).is_none() {
            return false;
        }
        cfg.node(node_id).cloned()
    };
    if let Some(pid) = state.runtime_pid(node_id) {
        let _ = match node {
            Some(n) if n.ssh_enabled() => crow_console_shared::ssh::stop_via_ssh(&n, pid)
                .await
                .unwrap_or(false),
            _ => matches!(
                tokio::task::spawn_blocking(move || lifecycle::stop_pid(pid)).await,
                Ok(Ok(true))
            ),
        };
    }
    {
        let mut cfg = state.config.write().unwrap();
        let _ = cfg.remove_server_for_node(node_id);
        cfg.purge_node_topology(node_id);
    }
    state.clear_runtime_pid(node_id);
    state.monitor_cache.drop_node(&node_id).await;
    true
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
    if !stop_and_remove_server_for_node(&state, node_id).await {
        return Err((
            StatusCode::NOT_FOUND,
            Json(ErrorBody {
                error: format!("no server deployed on node {node_id}"),
            }),
        ));
    }
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
#[allow(clippy::too_many_lines)]
pub async fn http_internal_reset(
    State(state): State<AppState>,
) -> Result<Json<ResetResult>, (StatusCode, Json<ErrorBody>)> {
    use crow_console_shared::lifecycle;

    // 0. Capture rack IDs and store IDs, then clean group-0 sysdata
    //    before stopping servers (R81: cluster reset must remove
    //    hardware hierarchy and KV-cluster topology from group 0).
    let rack_ids: Vec<RackId> = {
        let cfg = state.config.read().unwrap();
        cfg.racks.iter().map(|r| r.id).collect()
    };
    let stores: Vec<u64> = {
        let snap = state.monitor_cache.snapshot().await;
        let mut ids: Vec<u64> = snap.values().flat_map(|rec| rec.stores.keys().copied()).collect();
        ids.sort_unstable();
        ids.dedup();
        ids
    };
    if let Some(hw) = crate::mgmt::build_hardware_client(&state).await {
        for rid in &rack_ids {
            if let Err(e) = hw.remove_rack_cascade(*rid).await {
                tracing::warn!(rack_id = rid, error = %e, "reset: remove_rack_cascade failed");
            }
        }
        // Also clean KV-cluster topology records (stores, groups)
        // from group 0 — the existing reset removes them from config
        // but not from sysdata.
        let meta = crow_kv_client::KVClusterMetaClient::from_shared(hw.shared_kv());
        for sid in &stores {
            if let Err(e) = meta.remove_store(*sid).await {
                tracing::warn!(store_id = sid, error = %e, "reset: remove_store from sysdata failed");
            }
        }
        // Unregister all diskdb instances from the service registry
        // so stale endpoints don't cause continuous gRPC errors after
        // reset. The TTL-based expiry would eventually drop them, but
        // explicit unregister is immediate and avoids the 15s window.
        let svc = crow_kv_client::ServiceRegistryClient::from_shared(hw.shared_kv());
        match svc.read_all_diskdb_instances().await {
            Ok(instances) => {
                for (instance_id, _) in &instances {
                    if let Err(e) = svc.unregister("diskdb", *instance_id).await {
                        tracing::warn!(instance_id, error = %e, "reset: unregister diskdb failed");
                    }
                }
                tracing::info!("reset: unregistered {} diskdb instances", instances.len());
            }
            Err(e) => {
                tracing::warn!(error = %e, "reset: read_all_diskdb_instances failed; skipping unregister");
            }
        }
        tracing::info!(
            "reset: group-0 sysdata cleanup complete for {} racks",
            rack_ids.len()
        );
    } else {
        tracing::warn!("reset: no group-0 endpoint, skipping sysdata cleanup");
    }

    let mut stopped: Vec<String> = Vec::new();

    // 1. For each store: list & remove all groups, then remove the store.
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

    // 2. List all nodes, stop their servers, then remove them.
    let node_ids: Vec<NodeId> = {
        let cfg = state.config.read().unwrap();
        cfg.nodes.iter().map(|n| n.id).collect()
    };

    for nid in &node_ids {
        // Stop the KV server process if a PID is tracked.
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

        // Stop the DDB process if a PID is tracked.
        if let Some(pid) = state.diskdb_runtime_pid(nid) {
            let _ = tokio::task::spawn_blocking(move || lifecycle::stop_pid(pid)).await;
            state.clear_diskdb_runtime_pid(nid);
        }

        // Remove all server entries + purge topology from config.
        {
            let mut cfg = state.config.write().unwrap();
            let _ = cfg.remove_server_for_node(*nid);
            // Also remove any DDB entry for this node.
            let pos = cfg
                .servers
                .iter()
                .position(|s| s.node_id == Some(*nid) && s.service_type == ServiceType::Diskdb);
            if let Some(p) = pos {
                cfg.servers.remove(p);
            }
            cfg.purge_node_topology(*nid);
            let _ = cfg.remove_node(*nid);
        }

        state.monitor_cache.drop_node(nid).await;
    }

    // 3. Remove all racks.
    let rack_ids: Vec<String> = {
        let cfg = state.config.read().unwrap();
        cfg.racks.iter().map(|r| r.id.to_string()).collect()
    };
    {
        let mut cfg = state.config.write().unwrap();
        for rid in &rack_ids {
            let _ = cfg.remove_rack(rid.parse().unwrap());
        }
        // Clear disk-groups and disks from config — the rack/node
        // cascade above removed them from group-0 sysdata, but the
        // config file still carries the stale entries from before
        // the reset. Without this, a restart reloads stale DGs/disks.
        cfg.disk_groups.clear();
        cfg.disks.clear();
    }

    // 4. Clear caches and workspace directories.
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
/// When group-0 is available, reads the authoritative disk-group list
/// from group-0 sysdata (the source of truth). Falls back to the
/// console config file only when group-0 is not reachable. The config
/// `name` field is merged in as metadata where available.
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
    let (rack_id, node_exists) = {
        let cfg = state.config.read().unwrap();
        let rack = cfg.nodes.iter().find(|n| n.id == node_id);
        (rack.map(|n| n.rack_id), cfg.nodes.iter().any(|n| n.id == node_id))
    };
    if !node_exists {
        return Err((
            StatusCode::NOT_FOUND,
            Json(ErrorBody {
                error: format!("node {node_id} not found"),
            }),
        ));
    }
    // Try group-0 first (authoritative source of truth), but only if
    // the cluster is initialized — avoids logging warnings on every
    // poll when group-0 doesn't exist yet.
    if crate::mgmt::group0_available(&state).await {
        if let Some(hw) = crate::mgmt::build_hardware_client(&state).await {
            if let Some(rack_id) = rack_id {
                match hw.list_disk_groups_on_node(rack_id, node_id).await {
                    Ok(g0_dgs) => {
                        // Merge config names as metadata where available.
                        let name_map: std::collections::HashMap<DiskGroupId, String> = {
                            let cfg = state.config.read().unwrap();
                            cfg.disk_groups
                                .iter()
                                .filter(|dg| dg.node_id == node_id)
                                .map(|dg| (dg.id, dg.name.clone()))
                                .collect()
                        };
                        let entries: Vec<DiskGroupEntry> = g0_dgs
                            .into_iter()
                            .map(|dg| DiskGroupEntry {
                                id: dg.dg_id,
                                rack_id: dg.rack_id,
                                node_id: dg.node_id,
                                name: name_map.get(&dg.dg_id).cloned().unwrap_or_default(),
                            })
                            .collect();
                        return Ok(Json(entries));
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, node_id, "list_node_disk_groups: group-0 query failed; falling back to config");
                    }
                }
            }
        }
    }
    // Fallback: read from console config.
    let cfg = state.config.read().unwrap();
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

        // Auto-assign ownership: pick the diskdb instance with the
        // fewest owned DGs and write the ownership entry to group-0.
        // This ensures new DGs are immediately visible to a diskdb
        // keepalive sync without manual assignment via the UI.
        if let Err(e) = auto_assign_owner(&hw, rack_id, node_id, entry.id).await {
            tracing::warn!(disk_group_id = entry.id, error = %e, "sysdata sync: auto-assign owner failed");
        }
    }

    Ok((StatusCode::CREATED, Json(entry)))
}

/// Pick the diskdb instance with the fewest owned DGs from a list of
/// live instance IDs and the current ownership map. Ties are broken by
/// lowest `instance_id`. Returns `None` if `instance_ids` is empty.
///
/// Pure function — no I/O — so it can be unit-tested without a live
/// group-0 connection.
fn pick_least_loaded_instance(
    instance_ids: &[u64],
    owners: &[crow_protocol::sysdata::DiskdbOwnerEntry],
) -> Option<u64> {
    if instance_ids.is_empty() {
        return None;
    }
    let mut counts: std::collections::HashMap<u64, usize> = std::collections::HashMap::new();
    for o in owners {
        *counts.entry(o.instance_id).or_default() += 1;
    }
    instance_ids
        .iter()
        .map(|id| (*id, counts.get(id).copied().unwrap_or(0)))
        .min_by(|a, b| a.1.cmp(&b.1).then(a.0.cmp(&b.0)))
        .map(|(id, _)| id)
}

/// Auto-assign a newly created disk-group to the diskdb instance with
/// the fewest owned DGs. Reads the service registry for live diskdb
/// instances and the current ownership map, counts DGs per instance,
/// picks the one with the lowest count, and writes the ownership entry
/// to group-0. Does nothing (returns Ok) if no diskdb instances are
/// registered.
async fn auto_assign_owner(
    hw: &crow_kv_client::HardwareClient,
    rack_id: RackId,
    node_id: NodeId,
    dg_id: DiskGroupId,
) -> Result<(), String> {
    let svc = crow_kv_client::ServiceRegistryClient::from_shared(hw.shared_kv());
    let instances = svc
        .read_all_diskdb_instances()
        .await
        .map_err(|e| format!("read_all_diskdb_instances: {e}"))?;
    if instances.is_empty() {
        tracing::info!(dg_id, "auto-assign: no diskdb instances registered; skipping");
        return Ok(());
    }
    let owners = hw.list_owners().await.map_err(|e| format!("list_owners: {e}"))?;
    let instance_ids: Vec<u64> = instances.iter().map(|(id, _)| *id).collect();
    let Some(instance_id) = pick_least_loaded_instance(&instance_ids, &owners) else {
        return Ok(());
    };
    // Lease = 1 hour from now (the diskdb keepalive will refresh it).
    #[allow(clippy::cast_possible_truncation)]
    let lease_expiry_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
        + 3_600_000;
    hw.set_owner(rack_id, node_id, dg_id, instance_id, lease_expiry_ms)
        .await
        .map_err(|e| format!("set_owner: {e}"))?;
    tracing::info!(dg_id, instance_id, "auto-assign: assigned DG to diskdb instance");
    Ok(())
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
    // Capture rack_id + child disk ids before removing.
    let (rack_id, disk_ids): (Option<RackId>, Vec<String>) = {
        let cfg = state.config.read().unwrap();
        let rack_id = cfg
            .disk_groups
            .iter()
            .find(|dg| dg.node_id == node_id && dg.id == dg_id)
            .map(|dg| dg.rack_id);
        let disk_ids = cfg
            .disks_in_group(dg_id)
            .iter()
            .map(|d| d.disk_id.clone())
            .collect();
        (rack_id, disk_ids)
    };
    {
        let mut cfg = state.config.write().unwrap();
        // Cascade-remove child disks first, then the disk-group.
        for disk_id in &disk_ids {
            cfg.remove_disk(disk_id).map_err(map_config_err)?;
        }
        cfg.remove_disk_group(dg_id).map_err(map_config_err)?;
    }
    state.persist().map_err(map_persist_err)?;

    // Cascade-remove group-0 sysdata (handles child disks + DG).
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

/// Validate a single disk-add input, producing the `DiskEntry`, proto
/// `DiskId`, and `DiskValue` for group-0 sysdata sync. Shared by
/// `http_add_disk` and `http_add_disks_batch` so both paths reject
/// bad inputs before any config mutation or persist.
fn validate_disk_input(
    body: &AddDiskBody,
    dg_id: DiskGroupId,
    rack_id: RackId,
    node_id: NodeId,
) -> Result<
    (
        DiskEntry,
        crow_protocol::common::DiskId,
        crow_protocol::diskdb::rpc::DiskValue,
    ),
    (StatusCode, Json<ErrorBody>),
> {
    let disk_id_proto =
        match <crow_protocol::common::DiskId as crow_protocol::diskdb_type_util::DiskIdExt>::from_display_string(&body.disk_id) {
            Ok(id) => id,
            Err(e) => {
                return Err((
                    StatusCode::BAD_REQUEST,
                    Json(ErrorBody {
                        error: format!("invalid disk_id format for {}: {e}", body.disk_id),
                    }),
                ));
            }
        };
    if body.unit_size_bytes == 0 || body.zone_size_bytes == 0 {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorBody {
                error: format!(
                    "disk {}: unit_size_bytes and zone_size_bytes must be non-zero",
                    body.disk_id
                ),
            }),
        ));
    }
    if body.zone_size_bytes % u64::from(body.unit_size_bytes) != 0 {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorBody {
                error: format!(
                    "disk {}: zone_size_bytes must be a multiple of unit_size_bytes",
                    body.disk_id
                ),
            }),
        ));
    }
    if body.capacity_bytes < body.zone_size_bytes {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorBody {
                error: format!("disk {}: capacity_bytes must be >= zone_size_bytes", body.disk_id),
            }),
        ));
    }
    let unit_size = u64::from(body.unit_size_bytes);
    let capacity_units = body.capacity_bytes / unit_size;
    let zone_size_units = body.zone_size_bytes / unit_size;
    let zone_count = u32::try_from(capacity_units / zone_size_units).unwrap_or(u32::MAX);
    let disk_type_proto = match body.disk_type.as_str() {
        "Hdd" | "BLOCK_HDD" => crow_protocol::diskdb::rpc::DiskType::BlockHdd as i32,
        "Ssd" | "BLOCK_SSD" => crow_protocol::diskdb::rpc::DiskType::BlockSsd as i32,
        "ZONE_SSD" => crow_protocol::diskdb::rpc::DiskType::ZoneSsd as i32,
        "SMR_HDD" => crow_protocol::diskdb::rpc::DiskType::SmrHdd as i32,
        other => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(ErrorBody {
                    error: format!("disk {}: unknown disk_type: {other}", body.disk_id),
                }),
            ));
        }
    };
    let entry = DiskEntry {
        disk_id: body.disk_id.clone(),
        disk_group_id: dg_id,
        rack_id,
        node_id,
        disk_type: body.disk_type.clone(),
        capacity_bytes: body.capacity_bytes,
        zone_size_bytes: body.zone_size_bytes,
        unit_size_bytes: body.unit_size_bytes,
    };
    let value = crow_protocol::diskdb::rpc::DiskValue {
        status: crow_protocol::common::HwStatus::Up as i32,
        disk_type: disk_type_proto,
        capacity_units,
        zone_size_units,
        unit_size_bytes: body.unit_size_bytes,
        zone_count,
    };
    Ok((entry, disk_id_proto, value))
}

/// `GET /api/nodes/:node_id/disk-groups/:dg_id/disks`.
///
/// When group-0 is available, reads the authoritative disk list from
/// group-0 sysdata. Falls back to the console config file only when
/// group-0 is not reachable.
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
    // Resolve rack_id from config (needed for the group-0 key path).
    let rack_id = {
        let cfg = state.config.read().unwrap();
        cfg.nodes.iter().find(|n| n.id == node_id).map(|n| n.rack_id)
    };
    // Try group-0 first (authoritative source of truth), but only if
    // the cluster is initialized.
    if crate::mgmt::group0_available(&state).await {
        if let Some(hw) = crate::mgmt::build_hardware_client(&state).await {
            if let Some(rack_id) = rack_id {
                match hw.list_disks_in_group(rack_id, node_id, dg_id).await {
                    Ok(g0_disks) => {
                        let entries: Vec<DiskEntry> = g0_disks
                            .into_iter()
                            .map(|(disk_id, val)| {
                                let unit_size = u64::from(val.unit_size_bytes);
                                DiskEntry {
                                    disk_id: crow_protocol::diskdb_type_util::DiskIdExt::to_display_string(
                                        &disk_id,
                                    ),
                                    disk_group_id: dg_id,
                                    rack_id,
                                    node_id,
                                    disk_type: disk_type_proto_to_str(val.disk_type),
                                    capacity_bytes: val.capacity_units * unit_size,
                                    zone_size_bytes: val.zone_size_units * unit_size,
                                    unit_size_bytes: val.unit_size_bytes,
                                }
                            })
                            .collect();
                        return Ok(Json(entries));
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, node_id, dg_id, "list_disks_in_group: group-0 query failed; falling back to config");
                    }
                }
            }
        }
    }
    // Fallback: read from console config.
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

/// Map a proto `DiskType` i32 to the console string representation.
fn disk_type_proto_to_str(disk_type: i32) -> String {
    match disk_type {
        0 => "Hdd",
        1 => "Ssd",
        2 => "ZONE_SSD",
        3 => "SMR_HDD",
        _ => "Unknown",
    }
    .to_string()
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
    // Validate all inputs before mutating config or syncing group 0.
    let (entry, disk_id_proto, value) = validate_disk_input(&body, dg_id, rack_id, node_id)?;
    {
        let mut cfg = state.config.write().unwrap();
        cfg.add_disk(entry.clone()).map_err(map_config_err)?;
    }
    state.persist().map_err(map_persist_err)?;

    // Sync group-0 sysdata. Best-effort.
    if let Some(hw) = crate::mgmt::build_hardware_client(&state).await {
        if let Err(e) = hw.add_disk(rack_id, node_id, dg_id, &disk_id_proto, &value).await {
            tracing::warn!(disk_id = %entry.disk_id, error = %e, "sysdata sync: add_disk failed");
        }
    }

    Ok((StatusCode::CREATED, Json(entry)))
}

/// `POST /api/nodes/:node_id/disk-groups/:dg_id/disks/batch` —
/// add multiple disks in one request (R77). Validates all inputs
/// and checks for duplicates (against config and within the batch)
/// before mutating config; best-effort sysdata sync per disk.
/// Atomic all-or-nothing on the config mutation: if any `add_disk`
/// fails, the ones already added are rolled back.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns `404` if the disk-group doesn't exist, `400` on the
/// first invalid disk input, `409` on a duplicate `disk_id` (in
/// config or within the batch).
pub async fn http_add_disks_batch(
    State(state): State<AppState>,
    Path((node_id, dg_id)): Path<(NodeId, DiskGroupId)>,
    Json(body): Json<AddDisksBatchBody>,
) -> Result<(StatusCode, Json<AddDisksBatchResult>), (StatusCode, Json<ErrorBody>)> {
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

    // Validate all inputs and check for duplicates before any mutation.
    let mut validated: Vec<(
        DiskEntry,
        crow_protocol::common::DiskId,
        crow_protocol::diskdb::rpc::DiskValue,
    )> = Vec::with_capacity(body.disks.len());
    let mut seen_ids: HashSet<String> = HashSet::with_capacity(body.disks.len());
    for d in &body.disks {
        if !seen_ids.insert(d.disk_id.clone()) {
            return Err((
                StatusCode::CONFLICT,
                Json(ErrorBody {
                    error: format!("duplicate disk_id within batch: {}", d.disk_id),
                }),
            ));
        }
        validated.push(validate_disk_input(d, dg_id, rack_id, node_id)?);
    }

    // Mutate config: add all disks. Roll back on any failure so the
    // in-memory config stays consistent with the (un-persisted) state.
    let mut added: Vec<DiskEntry> = Vec::with_capacity(validated.len());
    {
        let mut cfg = state.config.write().unwrap();
        for (entry, _, _) in &validated {
            if let Err(e) = cfg.add_disk(entry.clone()).map_err(map_config_err) {
                // Roll back already-added disks.
                for a in &added {
                    let _ = cfg.remove_disk(&a.disk_id);
                }
                return Err(e);
            }
            added.push(entry.clone());
        }
    }
    state.persist().map_err(map_persist_err)?;

    // Best-effort sysdata sync per disk.
    let mut sysdata_errors: Vec<String> = Vec::new();
    if let Some(hw) = crate::mgmt::build_hardware_client(&state).await {
        for (entry, disk_id_proto, value) in &validated {
            if let Err(e) = hw.add_disk(rack_id, node_id, dg_id, disk_id_proto, value).await {
                let msg = format!("disk {}: sysdata sync failed: {e}", entry.disk_id);
                tracing::warn!(disk_id = %entry.disk_id, error = %e, "batch sysdata sync: add_disk failed");
                sysdata_errors.push(msg);
            }
        }
    }

    Ok((
        StatusCode::CREATED,
        Json(AddDisksBatchResult {
            added,
            sysdata_errors,
        }),
    ))
}

#[derive(Debug, Deserialize)]
pub struct AddDisksBatchBody {
    pub disks: Vec<AddDiskBody>,
}

#[derive(Debug, Serialize)]
pub struct AddDisksBatchResult {
    pub added: Vec<DiskEntry>,
    pub sysdata_errors: Vec<String>,
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
///
/// B.1: add-before-remove + conditional config update + quiescence
/// window. The disk is never "nowhere" — it's added to the new
/// placement before being removed from the old. The console config is
/// updated only if both group-0 operations succeed.
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

    let hw = crate::mgmt::build_hardware_client(&state).await.ok_or_else(|| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorBody {
                error: "no hardware client available".into(),
            }),
        )
    })?;

    // 4. Set disk to Maintenance in group 0 (old placement) —
    //    quiescence window. This blocks new allocates. The next
    //    diskdb sync tick (10 s) applies the status change and drains
    //    in-flight allocates.
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
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorBody {
                error: format!("move: set Maintenance failed: {e}"),
            }),
        ));
    }

    // 5. Wait for one sync tick (10 s) for diskdb to apply the
    //    Maintenance status. This is the quiescence window — no source
    //    lock, but Maintenance prevents new allocates and the sync
    //    tick ensures the disk is removed from the allocating-disks
    //    RCU context.
    tokio::time::sleep(std::time::Duration::from_secs(10)).await;

    // 6. Copy records from old bind to new bind.
    let old_bind = match hw.get_bind(old_rack_id, old_node_id, old_dg_id).await {
        Ok(Some(bind)) => (bind.store_id, bind.group_id),
        Ok(None) => {
            return Err((
                StatusCode::CONFLICT,
                Json(ErrorBody {
                    error: format!("no bind for old placement ({old_rack_id}, {old_node_id}, {old_dg_id})"),
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

    // 7. Get the current DiskValue from group 0 (old placement).
    let disk_value = match hw
        .get_disk(old_rack_id, old_node_id, old_dg_id, &disk_id_proto)
        .await
    {
        Ok(Some(dv)) => dv,
        Ok(None) => {
            return Err((
                StatusCode::NOT_FOUND,
                Json(ErrorBody {
                    error: format!("disk {disk_id} not found in group 0 (old placement)"),
                }),
            ));
        }
        Err(e) => {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorBody {
                    error: format!("get disk from old placement: {e}"),
                }),
            ));
        }
    };

    // 8. Add to new placement (Maintenance status) — add-before-remove.
    //    If this fails, the disk is still in the old placement
    //    (Maintenance), no data loss.
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
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorBody {
                error: format!("move: add to new placement failed (disk still in old): {e}"),
            }),
        ));
    }

    // 9. Remove from old placement. If this fails, the disk exists in
    //    both placements (both Maintenance, not allocatable). The
    //    operator should retry the remove.
    let remove_ok = hw
        .remove_disk(old_rack_id, old_node_id, old_dg_id, &disk_id_proto)
        .await
        .is_ok();
    if !remove_ok {
        // Partial success — disk in both placements. Don't update
        // console config (it stays pointing to old). Return a
        // partial-success response.
        return Err((
            StatusCode::CONFLICT,
            Json(ErrorBody {
                error: format!(
                    "move: added to new placement but remove from old failed; disk is in both placements (both Maintenance). Retry the remove for ({old_rack_id}, {old_node_id}, {old_dg_id})"
                ),
            }),
        ));
    }

    // 10. Update ConsoleConfig: move the DiskEntry to the new
    //     disk-group. Only done if both group-0 operations succeeded.
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

    // 11. Set disk to Up in new placement — diskdb at the new
    //     placement sees the disk on the next sync tick, runs
    //     background_zone_load (Init → Up), and the disk becomes
    //     allocatable. We set Up here so the operator doesn't need to
    //     do it manually.
    if let Err(e) = hw
        .set_disk_status(
            body.new_rack_id,
            body.new_node_id,
            body.new_disk_group_id,
            &disk_id_proto,
            crow_protocol::common::HwStatus::Up,
        )
        .await
    {
        tracing::warn!(disk_id = %disk_id, error = %e, "move: set Up at new placement failed (operator can set manually)");
    }

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

#[cfg(test)]
mod tests {
    use super::pick_least_loaded_instance;
    use crow_protocol::common_type::{DiskGroupId, NodeId, RackId};
    use crow_protocol::sysdata::DiskdbOwnerEntry;

    fn owner(rack: RackId, node: NodeId, dg: DiskGroupId, inst: u64) -> DiskdbOwnerEntry {
        DiskdbOwnerEntry {
            rack_id: rack,
            node_id: node,
            dg_id: dg,
            instance_id: inst,
            lease_expiry_ms: 0,
        }
    }

    #[test]
    fn pick_returns_none_for_empty_instances() {
        assert_eq!(pick_least_loaded_instance(&[], &[]), None);
    }

    #[test]
    fn pick_assigns_to_only_instance() {
        assert_eq!(pick_least_loaded_instance(&[1], &[]), Some(1));
    }

    #[test]
    fn pick_assigns_to_least_loaded() {
        // Instance 1 owns 2 DGs, instance 2 owns 0 → pick 2.
        let owners = vec![owner(1, 1, 1, 1), owner(1, 1, 2, 1)];
        assert_eq!(pick_least_loaded_instance(&[1, 2], &owners), Some(2));
    }

    #[test]
    fn pick_breaks_tie_by_lowest_instance_id() {
        // Both own 1 DG → pick lower instance_id.
        let owners = vec![owner(1, 1, 1, 1), owner(1, 1, 2, 2)];
        assert_eq!(pick_least_loaded_instance(&[1, 2], &owners), Some(1));
    }

    #[test]
    fn pick_ignores_owners_for_unknown_instances() {
        // Instance 3 owns DGs but is not in the live instance list.
        let owners = vec![owner(1, 1, 1, 3), owner(1, 1, 2, 3)];
        assert_eq!(pick_least_loaded_instance(&[1, 2], &owners), Some(1));
    }

    #[test]
    fn pick_counts_correctly_with_mixed_owners() {
        // Instance 1: 1 DG, instance 2: 2 DGs, instance 3: 1 DG.
        // Tie between 1 and 3 (both 1) → pick 1 (lower id).
        let owners = vec![
            owner(1, 1, 1, 1),
            owner(1, 1, 2, 2),
            owner(1, 1, 3, 2),
            owner(1, 1, 4, 3),
        ];
        assert_eq!(pick_least_loaded_instance(&[1, 2, 3], &owners), Some(1));
    }
}
