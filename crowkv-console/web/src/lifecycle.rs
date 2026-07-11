use crate::error::{err_400, err_500, err_502, map_config_err, map_persist_err, ErrorBody};
use crate::expand::Recursive;
use crate::state::AppState;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use crowkv_console_shared::config::{NodeEntry, RackEntry, ServerEntry};
use serde::{Deserialize, Serialize};
use std::time::Duration;

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
pub async fn http_list_racks(State(state): State<AppState>, Recursive(_depth): Recursive) -> Json<Vec<RackEntry>> {
    // Depth currently ignored: the payload is flat by design. A future
    // slice may inline each rack's nodes when depth >= 1.
    let cfg = state.config.read().unwrap();
    Json(cfg.racks.clone())
}

/// Add a new rack.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns an error if rack addition or config persistence fails.
pub async fn http_add_rack(State(state): State<AppState>, Json(body): Json<AddRackBody>) -> Result<(StatusCode, Json<RackEntry>), (StatusCode, Json<ErrorBody>)> {
    let entry = RackEntry { id: body.id, name: body.name };
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
pub async fn http_remove_rack(State(state): State<AppState>, Path(id): Path<String>) -> Result<StatusCode, (StatusCode, Json<ErrorBody>)> {
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
pub async fn http_list_nodes(State(state): State<AppState>, Query(q): Query<NodeQuery>, Recursive(_depth): Recursive) -> Json<Vec<NodeEntry>> {
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
pub async fn http_add_node(State(state): State<AppState>, Json(entry): Json<NodeEntry>) -> Result<(StatusCode, Json<NodeEntry>), (StatusCode, Json<ErrorBody>)> {
    {
        let mut cfg = state.config.write().unwrap();
        cfg.add_node(entry.clone()).map_err(map_config_err)?;
    }
    state.persist().map_err(map_persist_err)?;
    Ok((StatusCode::CREATED, Json(entry)))
}

/// Remove a node.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns an error if node removal or config persistence fails.
pub async fn http_remove_node(State(state): State<AppState>, Path(id): Path<String>) -> Result<StatusCode, (StatusCode, Json<ErrorBody>)> {
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
pub async fn http_ping_node(State(state): State<AppState>, Path(id): Path<String>) -> Result<Json<PingResult>, (StatusCode, Json<ErrorBody>)> {
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
        return Ok(Json(PingResult { ok: true, error: None }));
    }
    match crowkv_console_shared::ssh::probe(&node).await {
        Ok(()) => Ok(Json(PingResult { ok: true, error: None })),
        Err(e) => Ok(Json(PingResult {
            ok: false,
            error: Some(format!("{e}")),
        })),
    }
}

// ── Rack detail ──────────────────────────────────────────────────────

/// `GET /api/racks/:rack_id`. Rack detail with child node ids.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns `404` if the rack does not exist.
pub async fn http_get_rack(State(state): State<AppState>, Path(id): Path<String>, Recursive(_depth): Recursive) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorBody>)> {
    let cfg = state.config.read().unwrap();
    let rack = cfg.racks.iter().find(|r| r.id == id).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorBody {
                error: format!("rack {id} not found"),
            }),
        )
    })?;
    let node_ids: Vec<&str> = cfg.nodes.iter().filter(|n| n.rack_id == id).map(|n| n.id.as_str()).collect();
    Ok(Json(serde_json::json!({
        "id": rack.id,
        "name": rack.name,
        "nodes": node_ids,
    })))
}

/// `GET /api/racks/:rack_id/nodes`. List nodes under a specific rack.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns `404` if the rack does not exist.
pub async fn http_list_rack_nodes(
    State(state): State<AppState>,
    Path(rack_id): Path<String>,
    Recursive(_depth): Recursive,
) -> Result<Json<Vec<NodeEntry>>, (StatusCode, Json<ErrorBody>)> {
    let cfg = state.config.read().unwrap();
    if !cfg.racks.iter().any(|r| r.id == rack_id) {
        return Err((
            StatusCode::NOT_FOUND,
            Json(ErrorBody {
                error: format!("rack {rack_id} not found"),
            }),
        ));
    }
    Ok(Json(cfg.nodes.iter().filter(|n| n.rack_id == rack_id).cloned().collect()))
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
pub async fn http_get_node(State(state): State<AppState>, Path(id): Path<String>, Recursive(_depth): Recursive) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorBody>)> {
    let cfg = state.config.read().unwrap();
    let node = cfg.node(&id).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorBody {
                error: format!("node {id} not found"),
            }),
        )
    })?;
    let server = cfg.server_for_node(&id);
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
    let cfg = state.config.read().unwrap();
    let entry = cfg.server_for_node(&node_id).cloned().ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorBody {
                error: format!("no server deployed on node {node_id}"),
            }),
        )
    })?;
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
        binary: body.binary.clone().map(std::path::PathBuf::from),
    };

    let deployed = if node.ssh_enabled() {
        let server_bin = body
            .binary
            .clone()
            .unwrap_or_else(|| std::env::var("CROWKV_SERVER_BIN").unwrap_or_else(|_| "crowkv-server".to_string()));
        crowkv_console_shared::ssh::deploy_via_ssh(&req, &node, &server_bin)
            .await
            .map_err(|e| err_502(format!("ssh deploy: {e}")))?
    } else {
        lifecycle::deploy_local(&req, &node).await.map_err(|e| err_502(format!("local deploy: {e}")))?
    };

    let entry = ServerEntry {
        id: node_id.clone(),
        url: deployed.mgmt_url.clone(),
        node_id: Some(node_id.clone()),
        grpc_url: Some(deployed.grpc_url.clone()),
        pid: Some(deployed.pid),
    };
    {
        let mut cfg = state.config.write().unwrap();
        cfg.add_server(entry).map_err(map_config_err)?;
    }
    state.persist().map_err(map_persist_err)?;
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
pub async fn http_restart_node_server(State(state): State<AppState>, Path(node_id): Path<String>) -> Result<Json<DeployResult>, (StatusCode, Json<ErrorBody>)> {
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
    let mgmt_port = port_of(&entry.url).ok_or_else(|| err_500(format!("server entry has malformed mgmt_url: {}", entry.url)))?;
    let grpc_port = entry
        .grpc_url
        .as_deref()
        .and_then(port_of)
        .ok_or_else(|| err_500(format!("server entry has malformed grpc_url: {:?}", entry.grpc_url)))?;

    if let Some(pid) = entry.pid {
        let _sent = match &node {
            n if n.ssh_enabled() => crowkv_console_shared::ssh::stop_via_ssh(n, pid)
                .await
                .map_err(|e| err_502(format!("ssh stop (restart): {e}")))?,
            _ => lifecycle::stop_pid(pid).unwrap_or(false),
        };
    }

    let req = DeployRequest {
        server_id: node_id.clone(),
        mgmt_port,
        grpc_port,
        binary: None,
    };
    let deployed = if node.ssh_enabled() {
        let server_bin = std::env::var("CROWKV_SERVER_BIN").unwrap_or_else(|_| "crowkv-server".to_string());
        crowkv_console_shared::ssh::deploy_via_ssh(&req, &node, &server_bin)
            .await
            .map_err(|e| err_502(format!("ssh redeploy (restart): {e}")))?
    } else {
        lifecycle::deploy_local(&req, &node).await.map_err(|e| err_502(format!("local redeploy (restart): {e}")))?
    };

    let new_entry = ServerEntry {
        id: node_id.clone(),
        url: deployed.mgmt_url.clone(),
        node_id: Some(node_id.clone()),
        grpc_url: Some(deployed.grpc_url.clone()),
        pid: Some(deployed.pid),
    };
    {
        let mut cfg = state.config.write().unwrap();
        // The old entry is still keyed by node_id; replace it.
        let _ = cfg.remove_server_for_node(&node_id);
        cfg.add_server(new_entry).map_err(map_config_err)?;
    }
    state.persist().map_err(map_persist_err)?;

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
    let stripped = url.strip_prefix("http://").or_else(|| url.strip_prefix("https://")).unwrap_or(url);
    let host_port = stripped.split('/').next().unwrap_or(stripped);
    let port_str = host_port.rsplit(':').next()?;
    port_str.parse::<u16>().ok()
}

#[derive(Debug, Serialize)]
pub struct StopResult {
    sent: bool,
}

/// `POST /api/nodes/:node_id/server/stop`. Stop the server on this node
/// and remove the deployment record.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns an error if the server is not found or has no tracked pid.
pub async fn http_stop_node_server(State(state): State<AppState>, Path(node_id): Path<String>) -> Result<Json<StopResult>, (StatusCode, Json<ErrorBody>)> {
    use crowkv_console_shared::lifecycle;

    let (entry, node) = {
        let cfg = state.config.read().unwrap();
        let entry = cfg.server_for_node(&node_id).cloned().ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ErrorBody {
                    error: format!("no server deployed on node {node_id}"),
                }),
            )
        })?;
        let node = cfg.node(&node_id).cloned();
        (entry, node)
    };
    let Some(pid) = entry.pid else {
        return Err(err_400(format!("server on node {node_id} has no tracked pid")));
    };
    let sent = match node {
        Some(n) if n.ssh_enabled() => crowkv_console_shared::ssh::stop_via_ssh(&n, pid).await.map_err(|e| err_502(format!("ssh stop: {e}")))?,
        _ => lifecycle::stop_pid(pid).map_err(|e| err_500(format!("stop_pid: {e}")))?,
    };
    {
        let mut cfg = state.config.write().unwrap();
        let _ = cfg.remove_server_for_node(&node_id);
    }
    state.persist().map_err(map_persist_err)?;
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
pub async fn http_delete_node_server(State(state): State<AppState>, Path(node_id): Path<String>) -> Result<StatusCode, (StatusCode, Json<ErrorBody>)> {
    use crowkv_console_shared::lifecycle;

    let (entry, node) = {
        let cfg = state.config.read().unwrap();
        let entry = cfg.server_for_node(&node_id).cloned().ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ErrorBody {
                    error: format!("no server deployed on node {node_id}"),
                }),
            )
        })?;
        let node = cfg.node(&node_id).cloned();
        (entry, node)
    };
    if let Some(pid) = entry.pid {
        let _ = match node {
            Some(n) if n.ssh_enabled() => crowkv_console_shared::ssh::stop_via_ssh(&n, pid).await.unwrap_or(false),
            _ => lifecycle::stop_pid(pid).unwrap_or(false),
        };
    }
    {
        let mut cfg = state.config.write().unwrap();
        let _ = cfg.remove_server_for_node(&node_id);
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
pub async fn http_node_openapi_proxy(State(state): State<AppState>, Path(node_id): Path<String>) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorBody>)> {
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
            crowkv_console_shared::ops_log::append_http(&cid, "GET", &url, 0, started.elapsed().as_millis(), Some(&format!("transport error: {e}")));
            err_502(format!("openapi proxy: {e}"))
        })?;
    let upstream_status = resp.status();
    crowkv_console_shared::ops_log::append_http(&cid, "GET", &url, upstream_status.as_u16(), started.elapsed().as_millis(), None);
    if !upstream_status.is_success() {
        return Err(err_502(format!("openapi proxy: upstream {upstream_status}")));
    }
    let value = resp.json::<serde_json::Value>().await.map_err(|e| err_502(format!("openapi proxy: parse: {e}")))?;

    {
        let mut cache = state.openapi_cache.lock().unwrap();
        cache.insert(node_id, (value.clone(), std::time::Instant::now()));
    }

    Ok(Json(value))
}
