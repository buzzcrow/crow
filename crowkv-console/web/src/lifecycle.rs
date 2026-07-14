// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

use crate::error::{err_400, err_500, err_502, map_config_err, map_persist_err, ErrorBody};
use crate::expand::Recursive;
use crate::physical_view::PhysicalBuilder;
use crate::state::AppState;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use crowkv_console_shared::cluster::{NodeHealth, ProcState, ServerProcess};
use crowkv_console_shared::config::{NodeEntry, RackEntry, ServerEntry};
use crowkv_console_shared::expand::RecursiveDepth;
use crowkv_console_shared::monitor::NodeRecord;
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
    if let Some(node_id) = entry.node_id.as_deref() {
        proc.pid = state.runtime_pid(node_id);
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
    id: String,
    #[serde(default)]
    name: String,
}

#[derive(Debug, Deserialize)]
pub struct NodeQuery {
    /// Optional `?rack_id=<id>` filter for `GET /api/nodes`.
    #[serde(default)]
    rack_id: Option<String>,
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
    Path(id): Path<String>,
) -> Result<StatusCode, (StatusCode, Json<ErrorBody>)> {
    {
        let mut cfg = state.config.write().unwrap();
        cfg.remove_rack(&id).map_err(map_config_err)?;
    }
    state.persist().map_err(map_persist_err)?;
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
        Some(r) => cfg.nodes.iter().filter(|n| n.rack_id == r).cloned().collect(),
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
        .prepare_node_workspace(&entry.id)
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
pub async fn http_remove_node(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<StatusCode, (StatusCode, Json<ErrorBody>)> {
    {
        let mut cfg = state.config.write().unwrap();
        cfg.remove_node(&id).map_err(map_config_err)?;
    }
    state.persist().map_err(map_persist_err)?;
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
/// `crowkv_console_shared::ssh::probe` handshake; for local-fork nodes (`ssh_user=""`)
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
    Path(id): Path<String>,
) -> Result<Json<PingResult>, (StatusCode, Json<ErrorBody>)> {
    let node = {
        let cfg = state.config.read().unwrap();
        cfg.node(&id).cloned().ok_or_else(|| {
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
    match crowkv_console_shared::ssh::probe(&node).await {
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
    Path(id): Path<String>,
    Recursive(depth): Recursive,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorBody>)> {
    if matches!(depth, RecursiveDepth::None) {
        let cfg = state.config.read().unwrap();
        let rack = cfg.racks.iter().find(|r| r.id == id).ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ErrorBody {
                    error: format!("rack {id} not found"),
                }),
            )
        })?;
        let node_ids: Vec<&str> = cfg
            .nodes
            .iter()
            .filter(|n| n.rack_id == id)
            .map(|n| n.id.as_str())
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
        .find(|r| r.id == id)
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
    Path(rack_id): Path<String>,
    Recursive(depth): Recursive,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorBody>)> {
    if matches!(depth, RecursiveDepth::None) {
        let cfg = state.config.read().unwrap();
        if !cfg.racks.iter().any(|r| r.id == rack_id) {
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
            .filter(|n| n.rack_id == rack_id)
            .cloned()
            .collect();
        return Ok(Json(serde_json::to_value(&nodes).expect("serialize nodes")));
    }
    let snap = state.monitor_cache.snapshot().await;
    let cfg = state.config.read().unwrap();
    if !cfg.racks.iter().any(|r| r.id == rack_id) {
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
        .filter(|n| n.rack_id == rack_id)
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
    Path(rack_id): Path<String>,
    Json(mut entry): Json<NodeEntry>,
) -> Result<(StatusCode, Json<NodeEntry>), (StatusCode, Json<ErrorBody>)> {
    entry.rack_id = rack_id;
    {
        let mut cfg = state.config.write().unwrap();
        cfg.add_node(entry.clone()).map_err(map_config_err)?;
    }
    state.persist().map_err(map_persist_err)?;
    state
        .prepare_node_workspace(&entry.id)
        .map_err(|e| err_500(e.to_string()))?;
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
    Path(id): Path<String>,
    Recursive(_depth): Recursive,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorBody>)> {
    let snap = state.monitor_cache.snapshot().await;
    let cfg = state.config.read().unwrap();
    let node = cfg.node(&id).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorBody {
                error: format!("node {id} not found"),
            }),
        )
    })?;
    let server = cfg
        .server_for_node(&id)
        .map(|entry| live_server_process_with_pid(&state, entry, snap.get(&id)));
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

/// One row of `GET /api/servers`: a deployed `crowkv-server` projected
/// from the persisted config plus the live monitor cache.
#[derive(Debug, Serialize)]
pub struct ServerSummary {
    /// Owning node id (`None` for plain externally-registered servers).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_id: Option<String>,
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
                .as_deref()
                .and_then(|n| snap.get(n))
                .map_or(NodeHealth::Unknown, |rec| rec.health);
            let pid = s.node_id.as_deref().and_then(|n| state.runtime_pid(n));
            ServerSummary {
                node_id: s.node_id.clone(),
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
}

#[derive(Debug, Serialize)]
pub struct DeployResult {
    node_id: String,
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
    Path(node_id): Path<String>,
    Recursive(_depth): Recursive,
) -> Result<Json<ServerEntry>, (StatusCode, Json<ErrorBody>)> {
    let mut entry = {
        let cfg = state.config.read().unwrap();
        cfg.server_for_node(&node_id).cloned().ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ErrorBody {
                    error: format!("no server deployed on node {node_id}"),
                }),
            )
        })?
    };
    entry.pid = state.runtime_pid(&node_id);
    Ok(Json(entry))
}

/// `POST /api/nodes/:node_id/server/deploy`. Spawn `crowkv-server` on
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
    Path(node_id): Path<String>,
    Json(body): Json<DeployNodeServerBody>,
) -> Result<(StatusCode, Json<DeployResult>), (StatusCode, Json<ErrorBody>)> {
    use crowkv_console_shared::lifecycle::{self, DeployRequest};

    let node = {
        let cfg = state.config.read().unwrap();
        if cfg.server_for_node(&node_id).is_some() {
            return Err((
                StatusCode::CONFLICT,
                Json(ErrorBody {
                    error: format!("node {node_id} already hosts a deployed server"),
                }),
            ));
        }
        cfg.node(&node_id).cloned().ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ErrorBody {
                    error: format!("node {node_id} not found"),
                }),
            )
        })?
    };

    let req = DeployRequest {
        server_id: node_id.clone(),
        mgmt_port: body.mgmt_port,
        grpc_port: body.grpc_port,
        election_profile: std::env::var("CROWKV_SERVER_ELECTION_PROFILE").ok(),
        binary: body.binary.clone().map(std::path::PathBuf::from),
    };

    let deployed = if node.ssh_enabled() {
        let server_bin = body.binary.clone().unwrap_or_else(|| {
            std::env::var("CROWKV_SERVER_BIN").unwrap_or_else(|_| "crowkv-server".to_string())
        });
        crowkv_console_shared::ssh::deploy_via_ssh(&req, &node, &server_bin)
            .await
            .map_err(|e| err_502(format!("ssh deploy: {e}")))?
    } else {
        let workspace_dir = state
            .prepare_node_workspace(&node_id)
            .map_err(|e| err_500(e.to_string()))?;
        lifecycle::deploy_local_in_dir(&req, &node, &workspace_dir)
            .await
            .map_err(|e| err_502(format!("local deploy: {e}")))?
    };

    let entry = ServerEntry {
        id: node_id.clone(),
        url: deployed.mgmt_url.clone(),
        node_id: Some(node_id.clone()),
        grpc_url: Some(deployed.grpc_url.clone()),
        mgmt_port: Some(body.mgmt_port),
        grpc_port: Some(body.grpc_port),
        auto_start: true,
        binary: body.binary.clone(),
        election_profile: std::env::var("CROWKV_SERVER_ELECTION_PROFILE").ok(),
        pid: None,
    };
    state.set_runtime_pid(node_id.clone(), deployed.pid);
    {
        let mut cfg = state.config.write().unwrap();
        cfg.add_server(entry).map_err(map_config_err)?;
    }
    state.persist().map_err(map_persist_err)?;
    crate::mgmt::refresh_node_cache(&state, &node_id).await;
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
/// `crowkv-server` process on this node (if any) and immediately
/// re-deploy on the same ports recorded in the `ServerEntry`. The
/// binary path falls back to `CROWKV_SERVER_BIN` / `"crowkv-server"`
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
    Path(node_id): Path<String>,
) -> Result<Json<DeployResult>, (StatusCode, Json<ErrorBody>)> {
    use crowkv_console_shared::lifecycle::{self, DeployRequest};

    let (entry, node) = {
        let cfg = state.config.read().unwrap();
        let entry = cfg.server_for_node(&node_id).cloned().ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ErrorBody {
                    error: format!("no server registered on node {node_id}"),
                }),
            )
        })?;
        let node = cfg.node(&node_id).cloned().ok_or_else(|| {
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

    if let Some(pid) = state.runtime_pid(&node_id) {
        let _sent = match &node {
            n if n.ssh_enabled() => crowkv_console_shared::ssh::stop_via_ssh(n, pid)
                .await
                .map_err(|e| err_502(format!("ssh stop (restart): {e}")))?,
            _ => tokio::task::spawn_blocking(move || lifecycle::stop_pid(pid))
                .await
                .map_err(|e| err_500(format!("spawn_blocking (restart): {e}")))?
                .unwrap_or(false),
        };
    }

    let req = DeployRequest {
        server_id: node_id.clone(),
        mgmt_port,
        grpc_port,
        election_profile: std::env::var("CROWKV_SERVER_ELECTION_PROFILE").ok(),
        binary: None,
    };
    let deployed = if node.ssh_enabled() {
        let server_bin = std::env::var("CROWKV_SERVER_BIN").unwrap_or_else(|_| "crowkv-server".to_string());
        crowkv_console_shared::ssh::deploy_via_ssh(&req, &node, &server_bin)
            .await
            .map_err(|e| err_502(format!("ssh redeploy (restart): {e}")))?
    } else {
        let workspace_dir = state
            .prepare_node_workspace(&node_id)
            .map_err(|e| err_500(e.to_string()))?;
        lifecycle::deploy_local_in_dir(&req, &node, &workspace_dir)
            .await
            .map_err(|e| err_502(format!("local redeploy (restart): {e}")))?
    };

    let new_entry = ServerEntry {
        id: node_id.clone(),
        url: deployed.mgmt_url.clone(),
        node_id: Some(node_id.clone()),
        grpc_url: Some(deployed.grpc_url.clone()),
        mgmt_port: entry.mgmt_port,
        grpc_port: entry.grpc_port,
        auto_start: entry.auto_start,
        binary: entry.binary.clone(),
        election_profile: entry.election_profile.clone(),
        pid: None,
    };
    state.set_runtime_pid(node_id.clone(), deployed.pid);
    {
        let mut cfg = state.config.write().unwrap();
        // The old entry is still keyed by node_id; replace it.
        let _ = cfg.remove_server_for_node(&node_id);
        cfg.add_server(new_entry).map_err(map_config_err)?;
    }
    state.persist().map_err(map_persist_err)?;
    crate::mgmt::restore_persisted_topology_for_node(&state, &node_id)
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
    Path(node_id): Path<String>,
) -> Result<Json<StopResult>, (StatusCode, Json<ErrorBody>)> {
    use crowkv_console_shared::lifecycle;

    let node = {
        let cfg = state.config.read().unwrap();
        let _entry = cfg.server_for_node(&node_id).cloned().ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ErrorBody {
                    error: format!("no server deployed on node {node_id}"),
                }),
            )
        })?;
        cfg.node(&node_id).cloned()
    };
    let Some(pid) = state.runtime_pid(&node_id) else {
        return Err(err_400(format!("server on node {node_id} has no tracked pid")));
    };
    let sent = match node {
        Some(n) if n.ssh_enabled() => crowkv_console_shared::ssh::stop_via_ssh(&n, pid)
            .await
            .map_err(|e| err_502(format!("ssh stop: {e}")))?,
        _ => tokio::task::spawn_blocking(move || lifecycle::stop_pid(pid))
            .await
            .map_err(|e| err_500(format!("spawn_blocking: {e}")))?
            .map_err(|e| err_500(format!("stop_pid: {e}")))?,
    };
    state.clear_runtime_pid(&node_id);
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
    Path(node_id): Path<String>,
) -> Result<StatusCode, (StatusCode, Json<ErrorBody>)> {
    use crowkv_console_shared::lifecycle;

    let node = {
        let cfg = state.config.read().unwrap();
        let _entry = cfg.server_for_node(&node_id).cloned().ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ErrorBody {
                    error: format!("no server deployed on node {node_id}"),
                }),
            )
        })?;
        cfg.node(&node_id).cloned()
    };
    if let Some(pid) = state.runtime_pid(&node_id) {
        let _ = match node {
            Some(n) if n.ssh_enabled() => crowkv_console_shared::ssh::stop_via_ssh(&n, pid)
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
        let _ = cfg.remove_server_for_node(&node_id);
        cfg.purge_node_topology(&node_id);
    }
    state.clear_runtime_pid(&node_id);
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
    Path(node_id): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorBody>)> {
    let mgmt_url = {
        let cfg = state.config.read().unwrap();
        let entry = cfg.server_for_node(&node_id).ok_or_else(|| {
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
    let cid = crowkv_console_shared::corr_id::current_or_new();
    let started = std::time::Instant::now();
    let resp = reqwest::Client::new()
        .get(&url)
        .header(crowkv_console_shared::corr_id::HEADER, &cid)
        .send()
        .await
        .map_err(|e| {
            crowkv_console_shared::ops_log::append_http(
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
    crowkv_console_shared::ops_log::append_http(
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
    use crowkv_console_shared::lifecycle;

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
                    let node_ids: Vec<String> = gv.replicas.iter().map(|r| r.node_id.clone()).collect();
                    for nid in &node_ids {
                        if let Ok(url) = crate::mgmt::mgmt_url_for_node(&state, nid) {
                            if let Ok(client) = crate::mgmt::build_server_client(url) {
                                let _ = client.remove_group(*sid, gid).await;
                            }
                        }
                    }
                    for nid in &node_ids {
                        crate::mgmt::refresh_node_cache(&state, nid).await;
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
                if let Ok(url) = crate::mgmt::mgmt_url_for_node(&state, nid) {
                    if let Ok(client) = crate::mgmt::build_server_client(url) {
                        let _ = client.remove_store(*sid).await;
                    }
                }
            }
            for nid in &view.nodes {
                crate::mgmt::refresh_node_cache(&state, nid).await;
            }
        }
        {
            let mut cfg = state.config.write().unwrap();
            cfg.remove_store_record(*sid);
        }
    }

    // 3. List all nodes, stop their servers, then remove them.
    let node_ids: Vec<String> = {
        let cfg = state.config.read().unwrap();
        cfg.nodes.iter().map(|n| n.id.clone()).collect()
    };

    for nid in &node_ids {
        // Stop the server process if a PID is tracked.
        if let Some(pid) = state.runtime_pid(nid) {
            let ssh = state
                .config
                .read()
                .unwrap()
                .node(nid)
                .is_some_and(crowkv_console_shared::config::NodeEntry::ssh_enabled);
            let sent = if ssh {
                false
            } else {
                matches!(
                    tokio::task::spawn_blocking(move || lifecycle::stop_pid(pid)).await,
                    Ok(Ok(true))
                )
            };
            if sent {
                stopped.push(nid.clone());
            }
            state.clear_runtime_pid(nid);
        }

        // Remove the server entry + purge topology from config.
        {
            let mut cfg = state.config.write().unwrap();
            let _ = cfg.remove_server_for_node(nid);
            cfg.purge_node_topology(nid);
            let _ = cfg.remove_node(nid);
        }

        state.monitor_cache.drop_node(nid).await;
    }

    // 4. Remove all racks.
    let rack_ids: Vec<String> = {
        let cfg = state.config.read().unwrap();
        cfg.racks.iter().map(|r| r.id.clone()).collect()
    };
    {
        let mut cfg = state.config.write().unwrap();
        for rid in &rack_ids {
            let _ = cfg.remove_rack(rid);
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
