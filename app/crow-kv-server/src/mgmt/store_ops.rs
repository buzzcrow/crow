// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Store management endpoints: list, get, add, remove.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use tracing::{debug, info};

use crow_kv::cluster::kv_server::KvServer;
use crow_kv::cluster::px_kv_store::PxKvStore;
use crow_kv::common::config::ServerConfig;
use crow_protocol::mgmt::{AddStoreRequest, StoreDetail, StoreListResponse, StoreSummary};

use super::{err_json, ErrorResponse, RegistryArc};

#[utoipa::path(
        get,
        path = "/stores",
        tag = "management",
        responses((status = 200, description = "Stores in this server", body = StoreListResponse))
    )]
pub(super) async fn list_stores(State(state): State<RegistryArc>) -> Json<StoreListResponse> {
    let stores: Vec<StoreSummary> = state
        .stores
        .iter()
        .map(|entry| {
            let store_id = *entry.key();
            let store = entry.value();
            StoreSummary {
                store_id,
                listen_addr: store.listen_addr().map(|a| a.to_string()),
                group_count: store.group_count(),
            }
        })
        .collect();
    Json(StoreListResponse { stores })
}

#[utoipa::path(
        get,
        path = "/stores/{sid}",
        tag = "management",
        params(("sid" = u64, Path, description = "Store id")),
        responses(
            (status = 200, description = "Store detail", body = StoreDetail),
            (status = 404, description = "Store not found", body = ErrorResponse)
        )
    )]
pub(super) async fn get_store(
    State(state): State<RegistryArc>,
    Path(sid): Path<u64>,
) -> Result<Json<StoreDetail>, (StatusCode, Json<ErrorResponse>)> {
    let store = state
        .get_store(sid)
        .ok_or_else(|| err_json(StatusCode::NOT_FOUND, format!("store {sid} not found")))?;

    let groups = store.group_summaries();
    let group_list: Vec<crow_protocol::mgmt::GroupSummary> = groups
        .into_iter()
        .map(
            |(group_id, local_replica_id, leader_id, remote_count)| crow_protocol::mgmt::GroupSummary {
                group_id,
                local_replica_id,
                leader_id,
                remote_count,
            },
        )
        .collect();

    Ok(Json(StoreDetail {
        store_id: sid,
        listen_addr: store.listen_addr().map(|a| a.to_string()),
        groups: group_list,
    }))
}

#[utoipa::path(
        post,
        path = "/stores",
        tag = "management",
        request_body = AddStoreRequest,
        responses(
            (status = 201, description = "Store created", body = StoreSummary),
            (status = 400, description = "Invalid request", body = ErrorResponse),
            (status = 409, description = "Store already exists", body = ErrorResponse),
            (status = 500, description = "Store failed to start", body = ErrorResponse)
        )
    )]
pub(super) async fn add_store(
    State(state): State<RegistryArc>,
    Json(req): Json<AddStoreRequest>,
) -> Result<(StatusCode, Json<StoreSummary>), (StatusCode, Json<ErrorResponse>)> {
    if state.stores.contains_key(&req.store_id) {
        return Err(err_json(
            StatusCode::CONFLICT,
            format!("store {} already exists", req.store_id),
        ));
    }

    // Port priority: explicit request > port pool (--ports) > persisted
    // config file > OS-assigned (0). Delegated to `resolve_store_port` so
    // `/system/init` and `POST /stores` stay consistent.
    let port = super::resolve_store_port(&state, req.port, req.store_id).await;
    let addr: SocketAddr = format!("0.0.0.0:{port}")
        .parse()
        .map_err(|e| err_json(StatusCode::BAD_REQUEST, format!("invalid address: {e}")))?;

    debug!(
        store_id = req.store_id,
        bind_addr = %addr,
        "creating PxKvStore via management API"
    );
    let mut store = PxKvStore::new(req.store_id, addr);
    if let Some(ref mr) = state.metrics_registry {
        store.set_metrics_registry(Arc::clone(mr));
    }
    store.set_scan_byte_budget(state.config.server.scan_byte_budget);
    let store = Arc::new(store);

    if let Err(e) = store.start().await {
        return Err(err_json(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to start store server: {e}"),
        ));
    }
    store.wire_rpc_transport();

    info!(
        store_id = req.store_id,
        listen_addr = ?store.listen_addr(),
        "PxKvStore added and started via management API"
    );

    let summary = StoreSummary {
        store_id: req.store_id,
        listen_addr: store.listen_addr().map(|a| a.to_string()),
        group_count: 0,
    };

    state.add_store(req.store_id, store);
    Ok((StatusCode::CREATED, Json(summary)))
}

#[utoipa::path(
        delete,
        path = "/stores/{sid}",
        tag = "management",
        params(("sid" = u64, Path, description = "Store id")),
        responses(
            (status = 200, description = "Store removed"),
            (status = 404, description = "Store not found", body = ErrorResponse)
        )
    )]
pub(super) async fn remove_store(
    State(state): State<RegistryArc>,
    Path(sid): Path<u64>,
) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
    info!(store_id = sid, "removing PxKvStore via management API");
    let store = state
        .remove_store(sid)
        .ok_or_else(|| err_json(StatusCode::NOT_FOUND, format!("store {sid} not found")))?;
    let report = store
        .shutdown(std::time::Duration::from_millis(
            ServerConfig::DEFAULT.shutdown_timeout_ms,
        ))
        .await;
    if !report.is_clean() {
        for err in &report.errors {
            tracing::error!(store_id = sid, "{err}");
        }
    }

    // Delete the engine store dir (cascades all group subdirs).
    let engine_store_dir = state.config.data_root.join(format!("store{sid}"));
    if let Err(e) = tokio::fs::remove_dir_all(&engine_store_dir).await {
        if e.kind() != std::io::ErrorKind::NotFound {
            tracing::warn!(store_id = sid, error = %e, "failed to delete engine store dir; continuing");
        }
    }

    // Delete the WAL store dir (cascades all group subdirs).
    let wal_store_dir = crate::startup::store_wal_root(&state.config.wal_root, sid);
    if let Err(e) = tokio::fs::remove_dir_all(&wal_store_dir).await {
        if e.kind() != std::io::ErrorKind::NotFound {
            tracing::warn!(store_id = sid, error = %e, "failed to delete WAL store dir; continuing");
        }
    }

    // Update node-config.json so the store does not resurrect on restart.
    let node_config_store = crow_kv::cluster::node_config::NodeConfigStore::new(&state.config.config_root);
    if let Err(e) = node_config_store.remove_store(sid).await {
        tracing::warn!(store_id = sid, error = %e, "failed to update node_config; continuing");
    }

    info!(
        store_id = sid,
        error_count = report.errors.len(),
        "PxKvStore removed via management API (dirs deleted + node_config updated)"
    );
    Ok(StatusCode::OK)
}
