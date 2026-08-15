// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! `DiskDB` instance deploy / restart / stop / delete handlers (R77).
//! These manage the `crow-diskdb` process lifecycle via
//! `crow_console_shared::lifecycle::deploy_diskdb_local` (local-fork
//! only; SSH is a C4 follow-up). The runtime REST proxy
//! (`/api/diskdb/*`) lives in `crate::diskdb`.

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use serde::{Deserialize, Serialize};

use crate::error::{err_400, err_500, err_502, ErrorBody};
use crate::state::AppState;

#[derive(Debug, Deserialize)]
pub struct DeployDiskdbBody {
    pub rest_port: u16,
    pub rpc_port: u16,
    #[serde(default)]
    pub binary: Option<String>,
    #[serde(default)]
    pub listen_addr: Option<String>,
    #[serde(default)]
    pub http_addr: Option<String>,
    #[serde(default)]
    pub config: Option<String>,
}

/// `POST /api/nodes/:id/diskdb/deploy` — spawn `crow-diskdb` on the
/// node's workspace. Registers a `ServerEntry` with
/// `service_type: Diskdb` and tracks the PID.
///
/// Local-fork only (mirrors `deploy_diskdb_local`); `node.host` is
/// used for URL construction but the process spawns on the console
/// host. SSH deploy is a follow-up (C4 status — see
/// `lib/crow-console-shared/src/lifecycle.rs`).
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns `409` if a diskdb instance is already deployed on the
/// node, `404` if the node doesn't exist, `502` on spawn failure.
pub async fn http_deploy_diskdb(
    State(state): State<AppState>,
    Path(node_id): Path<u64>,
    Json(body): Json<DeployDiskdbBody>,
) -> Result<(StatusCode, Json<DiskdbDeployResult>), (StatusCode, Json<ErrorBody>)> {
    use crow_console_shared::config::ServiceType;
    use crow_console_shared::lifecycle::{self, DiskdbDeployRequest};

    let node = {
        let cfg = state.config.read().unwrap();
        // Check for existing diskdb instance on this node.
        if cfg
            .servers
            .iter()
            .any(|s| s.node_id == Some(node_id) && s.service_type == ServiceType::Diskdb)
        {
            return Err((
                StatusCode::CONFLICT,
                Json(ErrorBody {
                    error: format!("node {node_id} already hosts a deployed diskdb instance"),
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

    let req = DiskdbDeployRequest {
        server_id: format!("diskdb-{node_id}"),
        rest_port: body.rest_port,
        rpc_port: body.rpc_port,
        binary: body.binary.clone().map(std::path::PathBuf::from),
        listen_addr: body.listen_addr.clone(),
        http_addr: body.http_addr.clone(),
        config: body.config.clone().map(std::path::PathBuf::from),
    };

    let workspace_dir = state
        .prepare_node_workspace(node_id)
        .map_err(|e| err_500(e.to_string()))?;
    let deployed = lifecycle::deploy_diskdb_local(&req, &node, &workspace_dir)
        .await
        .map_err(|e| err_502(format!("diskdb deploy: {e}")))?;

    let entry = crow_console_shared::config::ServerEntry {
        id: format!("diskdb-{node_id}"),
        url: deployed.mgmt_url.clone(),
        node_id: Some(node_id),
        grpc_url: Some(deployed.grpc_url.clone()),
        rest_port: Some(body.rest_port),
        rpc_port: Some(body.rpc_port),
        auto_start: true,
        binary: body.binary.clone(),
        election_profile: None,
        pid: None,
        service_type: ServiceType::Diskdb,
    };
    state.set_diskdb_runtime_pid(node_id, deployed.pid);
    {
        let mut cfg = state.config.write().unwrap();
        cfg.add_server(entry).map_err(|e| err_500(format!("{e}")))?;
    }
    state.persist().map_err(|e| err_500(format!("{e}")))?;

    Ok((
        StatusCode::CREATED,
        Json(DiskdbDeployResult {
            node_id,
            mgmt_url: deployed.mgmt_url,
            grpc_url: deployed.grpc_url,
            pid: deployed.pid,
        }),
    ))
}

/// `POST /api/nodes/:id/diskdb/restart` — stop and redeploy the
/// diskdb instance on a node, preserving ports and binary path from
/// the persisted `ServerEntry`.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns `404` if no diskdb instance is registered on the node,
/// `502` on spawn/stop failure.
pub async fn http_restart_diskdb(
    State(state): State<AppState>,
    Path(node_id): Path<u64>,
) -> Result<Json<DiskdbDeployResult>, (StatusCode, Json<ErrorBody>)> {
    use crow_console_shared::config::ServiceType;
    use crow_console_shared::lifecycle::{self, DiskdbDeployRequest};

    let (entry, node) = {
        let cfg = state.config.read().unwrap();
        let entry = cfg
            .servers
            .iter()
            .find(|s| s.node_id == Some(node_id) && s.service_type == ServiceType::Diskdb)
            .cloned()
            .ok_or_else(|| {
                (
                    StatusCode::NOT_FOUND,
                    Json(ErrorBody {
                        error: format!("no diskdb instance registered on node {node_id}"),
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

    let rest_port = crate::mgmt::port_of(&entry.url)
        .ok_or_else(|| err_500(format!("diskdb entry has malformed mgmt_url: {}", entry.url)))?;
    let rpc_port = entry
        .grpc_url
        .as_deref()
        .and_then(crate::mgmt::port_of)
        .ok_or_else(|| {
            err_500(format!(
                "diskdb entry has malformed grpc_url: {:?}",
                entry.grpc_url
            ))
        })?;

    // Stop existing process.
    if let Some(pid) = state.diskdb_runtime_pid(node_id) {
        let _ = tokio::task::spawn_blocking(move || lifecycle::stop_pid(pid))
            .await
            .map_err(|e| err_500(format!("spawn_blocking (diskdb restart): {e}")))?
            .map_err(|e| err_500(format!("stop_pid (diskdb restart): {e}")))?;
    }

    let req = DiskdbDeployRequest {
        server_id: format!("diskdb-{node_id}"),
        rest_port,
        rpc_port,
        binary: entry.binary.clone().map(std::path::PathBuf::from),
        ..Default::default()
    };
    let workspace_dir = state
        .prepare_node_workspace(node_id)
        .map_err(|e| err_500(e.to_string()))?;
    let deployed = lifecycle::deploy_diskdb_local(&req, &node, &workspace_dir)
        .await
        .map_err(|e| err_502(format!("diskdb redeploy (restart): {e}")))?;

    let new_entry = crow_console_shared::config::ServerEntry {
        id: entry.id.clone(),
        url: deployed.mgmt_url.clone(),
        node_id: Some(node_id),
        grpc_url: Some(deployed.grpc_url.clone()),
        rest_port: entry.rest_port,
        rpc_port: entry.rpc_port,
        auto_start: entry.auto_start,
        binary: entry.binary.clone(),
        election_profile: None,
        pid: None,
        service_type: ServiceType::Diskdb,
    };
    state.set_diskdb_runtime_pid(node_id, deployed.pid);
    {
        let mut cfg = state.config.write().unwrap();
        let _ = cfg.remove_server_for_node(node_id);
        cfg.add_server(new_entry).map_err(|e| err_500(format!("{e}")))?;
    }
    state.persist().map_err(|e| err_500(format!("{e}")))?;

    Ok(Json(DiskdbDeployResult {
        node_id,
        mgmt_url: deployed.mgmt_url,
        grpc_url: deployed.grpc_url,
        pid: deployed.pid,
    }))
}

/// `POST /api/nodes/:id/diskdb/stop` — stop the diskdb instance on a
/// node and remove its `ServerEntry`.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns `404` if no diskdb instance is registered, `400` if no
/// tracked PID, `502` on stop failure.
pub async fn http_stop_diskdb(
    State(state): State<AppState>,
    Path(node_id): Path<u64>,
) -> Result<Json<crate::lifecycle::StopResult>, (StatusCode, Json<ErrorBody>)> {
    use crow_console_shared::config::ServiceType;
    use crow_console_shared::lifecycle;

    {
        let cfg = state.config.read().unwrap();
        let exists = cfg
            .servers
            .iter()
            .any(|s| s.node_id == Some(node_id) && s.service_type == ServiceType::Diskdb);
        if !exists {
            return Err((
                StatusCode::NOT_FOUND,
                Json(ErrorBody {
                    error: format!("no diskdb instance deployed on node {node_id}"),
                }),
            ));
        }
    }
    let Some(pid) = state.diskdb_runtime_pid(node_id) else {
        return Err(err_400(format!("diskdb on node {node_id} has no tracked pid")));
    };
    let sent = tokio::task::spawn_blocking(move || lifecycle::stop_pid(pid))
        .await
        .map_err(|e| err_500(format!("spawn_blocking: {e}")))?
        .map_err(|e| err_500(format!("stop_pid: {e}")))?;
    state.clear_diskdb_runtime_pid(node_id);
    {
        let mut cfg = state.config.write().unwrap();
        // Remove the diskdb ServerEntry (not the kv-server entry).
        let pos = cfg
            .servers
            .iter()
            .position(|s| s.node_id == Some(node_id) && s.service_type == ServiceType::Diskdb);
        if let Some(p) = pos {
            cfg.servers.remove(p);
        }
    }
    state.persist().map_err(|e| err_500(format!("{e}")))?;
    Ok(Json(crate::lifecycle::StopResult { sent }))
}

/// `DELETE /api/nodes/:id/diskdb` — stop the diskdb instance (if a
/// PID is tracked) and remove its `ServerEntry`. Best-effort stop:
/// if no PID is tracked (e.g. console restarted after deploy), the
/// entry is still removed. Mirrors `http_delete_node_server` for KV.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns `404` if no diskdb instance is registered on the node.
pub async fn http_delete_diskdb(
    State(state): State<AppState>,
    Path(node_id): Path<u64>,
) -> Result<StatusCode, (StatusCode, Json<ErrorBody>)> {
    use crow_console_shared::config::ServiceType;
    use crow_console_shared::lifecycle;

    {
        let cfg = state.config.read().unwrap();
        if !cfg
            .servers
            .iter()
            .any(|s| s.node_id == Some(node_id) && s.service_type == ServiceType::Diskdb)
        {
            return Err((
                StatusCode::NOT_FOUND,
                Json(ErrorBody {
                    error: format!("no diskdb instance deployed on node {node_id}"),
                }),
            ));
        }
    }
    // Best-effort stop: the PID may be gone (console restart).
    if let Some(pid) = state.diskdb_runtime_pid(node_id) {
        let _ = tokio::task::spawn_blocking(move || lifecycle::stop_pid(pid))
            .await
            .map_err(|e| err_500(format!("spawn_blocking: {e}")))?
            .map_err(|e| err_500(format!("stop_pid: {e}")))?;
        state.clear_diskdb_runtime_pid(node_id);
    }
    {
        let mut cfg = state.config.write().unwrap();
        let pos = cfg
            .servers
            .iter()
            .position(|s| s.node_id == Some(node_id) && s.service_type == ServiceType::Diskdb);
        if let Some(p) = pos {
            cfg.servers.remove(p);
        }
    }
    state.persist().map_err(|e| err_500(format!("{e}")))?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Debug, Serialize)]
pub struct DiskdbDeployResult {
    pub node_id: u64,
    pub mgmt_url: String,
    pub grpc_url: String,
    pub pid: u32,
}
