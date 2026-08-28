// Copyright 2026-present buzzcrow <buzzcrow::126.com>
// Licensed under the Apache License, Version 2.0.

//! System initialization and health-check endpoints.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;
use tracing::info;

use crow_kv::cluster::group_election::LeaderElection;
use crow_kv::cluster::kv_server::KvServer;
use crow_kv::cluster::local_replica::PxLocalReplicaRole;
use crow_kv::cluster::px_kv_store::PxKvStore;
use crow_kv::cluster::status::{StatusLevel, StoreStatus};
use crow_protocol::mgmt::{HealthResponse, SystemInitRequest, SystemInitResponse};

use super::{err_json, ErrorResponse, RegistryArc};

/// `GET /health` — hierarchical cluster health report.
///
/// Aggregates per-layer cached status (no active probing in V1). Returns `200`
/// when overall status is `ok` / `degraded`, `503` when `unhealthy`
/// (load-balancer signal).
#[utoipa::path(
        get,
        path = "/health",
        tag = "management",
        responses(
            (status = 200, description = "Cluster is live", body = HealthResponse),
            (status = 503, description = "Cluster is unhealthy", body = HealthResponse)
        )
    )]
pub(super) async fn health_check(State(state): State<RegistryArc>) -> (StatusCode, Json<HealthResponse>) {
    let mut overall = StatusLevel::Ok;
    let mut messages: Vec<String> = Vec::new();
    let stores: Vec<StoreStatus> = state
        .stores
        .iter()
        .map(|entry| {
            let store = entry.value();
            let s = store.status();
            overall = StatusLevel::worst(overall, s.status);
            s
        })
        .collect();

    if state.stores.is_empty() {
        messages.push("no stores configured".to_string());
    }

    let http_status = if overall == StatusLevel::Unhealthy {
        StatusCode::SERVICE_UNAVAILABLE
    } else {
        StatusCode::OK
    };

    (
        http_status,
        Json(HealthResponse {
            status: overall.as_str().to_string(),
            messages,
            stores,
        }),
    )
}

/// `POST /system/init` — bootstrap the system group (store 0, group 0).
///
/// Creates store 0 (if it does not already exist) and group 0 with a
/// local replica on this node. For single-node init, `start_election`
/// defaults to `true` (self-elect). For multi-node, the caller sets
/// `start_election: false` and wires remotes afterward via
/// `POST /stores/0/groups/0/remotes`.
#[utoipa::path(
        post,
        path = "/system/init",
        tag = "management",
        request_body = SystemInitRequest,
        responses(
            (status = 201, description = "System group created", body = SystemInitResponse),
            (status = 409, description = "Group 0 already exists", body = ErrorResponse),
            (status = 500, description = "Store or group creation failed", body = ErrorResponse)
        )
    )]
#[allow(clippy::too_many_lines)]
pub(super) async fn system_init(
    State(state): State<RegistryArc>,
    req: Option<Json<SystemInitRequest>>,
) -> Result<(StatusCode, Json<SystemInitResponse>), (StatusCode, Json<ErrorResponse>)> {
    const SYSTEM_STORE_ID: u64 = 0;
    const SYSTEM_GROUP_ID: u64 = 0;

    let req = req.map_or(
        SystemInitRequest {
            replica_id: 1,
            start_election: true,
        },
        |Json(r)| r,
    );

    // Create store 0 if it does not exist. Use the shared port resolver so
    // store 0 consumes the port pool port deterministically — using
    // `0.0.0.0:0` here lets the OS pick a random port that may collide with
    // a future pool allocation (e.g. `add_store` for store 1).
    if !state.stores.contains_key(&SYSTEM_STORE_ID) {
        let port = super::resolve_store_port(&state, None, SYSTEM_STORE_ID).await;
        let addr: SocketAddr = format!("0.0.0.0:{port}")
            .parse()
            .map_err(|e| err_json(StatusCode::BAD_REQUEST, format!("invalid address: {e}")))?;
        let mut store = PxKvStore::new(SYSTEM_STORE_ID, addr);
        store.rpc_workers = state.rpc_workers;
        if let Some(ref mr) = state.metrics_registry {
            store.set_metrics_registry(Arc::clone(mr));
        }
        store.set_scan_byte_budget(state.config.server.scan_byte_budget);
        store.set_peer_pool_size(state.config.server.peer_pool_size);
        store.set_enable_nagle(state.config.server.enable_nagle);
        store.set_quickack(state.config.server.quickack);
        store.set_event_write(state.config.server.event_write);
        store.set_send_queue_capacity(state.config.server.send_queue_capacity);
        let store = Arc::new(store);
        store.start().await.map_err(|e| {
            err_json(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to start store 0: {e}"),
            )
        })?;
        store.wire_rpc_transport();
        state.add_store(SYSTEM_STORE_ID, store);
        info!(
            store_id = SYSTEM_STORE_ID,
            "system store 0 created via /system/init"
        );
    }

    let store = state.get_store(SYSTEM_STORE_ID).ok_or_else(|| {
        err_json(
            StatusCode::INTERNAL_SERVER_ERROR,
            "store 0 not found after creation",
        )
    })?;

    // Check if group 0 already exists.
    if store.get_group(SYSTEM_GROUP_ID).is_some() {
        return Err(err_json(
            StatusCode::CONFLICT,
            "group 0 already exists in store 0",
        ));
    }

    let group = crate::startup::create_group_with_wal(
        SYSTEM_STORE_ID,
        SYSTEM_GROUP_ID,
        req.replica_id,
        PxLocalReplicaRole::Leader,
        &state.config,
        state.wal_backend.clone(),
        state.crowtree_backend,
    )
    .await
    .map_err(|e| {
        err_json(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to create group 0: {e}"),
        )
    })?;

    if req.start_election && group.quorum() == 1 {
        let current_term = group.local_replica().current_term_snapshot();
        if current_term == 0 {
            group.local_replica().become_candidate(1);
            group.local_replica().persist_current_vote().await;
            group.local_replica().become_leader();
        }
        group.stamp_proposing_term(group.local_replica().current_term_snapshot());
    }

    let listen_addr = store.listen_addr().map(|a| a.to_string());

    if req.start_election {
        store.add_group(group);
    } else {
        store.add_group_without_election(group);
    }

    // Persist the group config to node-config.json so the replica_id,
    // endpoint, and membership survive a restart. Without this,
    // single-node init (no remote-wiring step) leaves no node-config
    // entry for store 0, and restore mode cannot recover the store's
    // listen port — it falls back to the port pool, which may collide
    // with another store's persisted port.
    if let Some(g) = store.get_group(SYSTEM_GROUP_ID) {
        g.persist_config().await;
    }

    info!(
        store_id = SYSTEM_STORE_ID,
        group_id = SYSTEM_GROUP_ID,
        replica_id = req.replica_id,
        start_election = req.start_election,
        "system group 0 created via /system/init"
    );

    Ok((
        StatusCode::CREATED,
        Json(SystemInitResponse {
            store_id: SYSTEM_STORE_ID,
            group_id: SYSTEM_GROUP_ID,
            replica_id: req.replica_id,
            listen_addr,
        }),
    ))
}
