// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use tracing::{debug, info};
use utoipa::{OpenApi, ToSchema};

use crowkv::cluster::group::PxGroup;
use crowkv::cluster::group_config::GroupConfigStore;
use crowkv::cluster::group_election::LeaderElection;
use crowkv::cluster::kv_server::KvServer;
use crowkv::cluster::kv_store::KvStore;
use crowkv::cluster::local_replica::{PxLocalReplica, PxLocalReplicaRole};
use crowkv::cluster::px_kv_store::PxKvStore;
use crowkv::cluster::remote_replica::PxRemoteReplica;
use crowkv::cluster::topology_kv;
use crowkv::common::config::ServerConfig;

use crate::operation_registry::{AppState, Operation, OperationKind, OperationStatus, OperationTarget};
use crate::startup::create_group_with_wal;

type RegistryArc = AppState;

pub fn router(state: RegistryArc) -> Router {
    Router::new()
        .route("/health", get(health_check))
        .route("/system/init", post(system_init))
        .route("/topology/finalize", post(topology_finalize))
        .route("/topology/ready", get(topology_ready))
        .route("/stores", get(list_stores).post(add_store))
        .route("/stores/:sid", get(get_store).delete(remove_store))
        .route("/stores/:sid/groups", get(list_groups).post(add_group))
        .route("/stores/:sid/groups/:gid", delete(remove_group))
        .route(
            "/stores/:sid/groups/:gid/remotes",
            get(list_remote_replicas).post(add_remote_replicas),
        )
        .route(
            "/stores/:sid/groups/:gid/remotes/batch",
            post(batch_add_remote_replicas),
        )
        .route(
            "/stores/:sid/groups/:gid/remotes/:rid",
            delete(remove_remote_replica),
        )
        .route("/stores/:sid/groups/:gid/step-down", post(step_down))
        .route("/stores/:sid/groups/:gid/join", post(join_group_via_snapshot))
        .route("/stores/:sid/groups/:gid/ready", get(group_readiness))
        .route("/operations/:id", get(get_operation))
        .route("/topology", get(export_topology))
        .route("/top", get(export_topology))
        .route("/openapi.json", get(openapi_spec))
        .with_state(state)
}

use crowkv::cluster::status::{GroupStatus, RemoteStatus, ReplicaStatus, StatusLevel, StoreStatus};

// ── JSON types ──────────────────────────────────────────────

#[derive(ToSchema, Serialize)]
struct HealthResponse {
    status: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    messages: Vec<String>,
    stores: Vec<StoreStatus>,
}

#[derive(ToSchema, Serialize)]
struct StoreListResponse {
    stores: Vec<StoreSummary>,
}

#[derive(ToSchema, Serialize)]
struct StoreSummary {
    store_id: u64,
    listen_addr: Option<String>,
    group_count: usize,
}

#[derive(ToSchema, Serialize)]
struct StoreDetail {
    store_id: u64,
    listen_addr: Option<String>,
    groups: Vec<GroupSummary>,
}

#[derive(ToSchema, Serialize)]
struct GroupSummary {
    group_id: u64,
    local_replica_id: u64,
    leader_id: u64,
    remote_count: usize,
}

#[derive(ToSchema, Deserialize)]
struct SystemInitRequest {
    /// Replica ID for this node's group 0 replica. Defaults to 1.
    #[serde(default = "default_replica_id")]
    replica_id: u64,
    /// Whether to start the election driver immediately. For single-node
    /// init, set `true` (self-elect). For multi-node, set `false` and
    /// wire remotes first. Defaults to `true`.
    #[serde(default = "default_start_election")]
    start_election: bool,
}

fn default_replica_id() -> u64 {
    1
}

fn default_start_election() -> bool {
    true
}

#[derive(ToSchema, Serialize)]
struct SystemInitResponse {
    store_id: u64,
    group_id: u64,
    replica_id: u64,
    listen_addr: Option<String>,
}

#[derive(ToSchema, Serialize)]
struct TopologyFinalizeResponse {
    ready: bool,
    already_finalized: bool,
}

#[derive(ToSchema, Serialize)]
struct TopologyReadyResponse {
    ready: bool,
}

#[derive(ToSchema, Deserialize)]
struct AddStoreRequest {
    store_id: u64,
    #[serde(default)]
    port: Option<u16>,
}

/// Scan the config root for any `store{store_id}_group*.json` file, load it,
/// and extract the port from the first member's endpoint. Returns `None` if
/// no config file exists or no valid endpoint is found.
///
/// Priority: explicit port > persisted config port > OS-assigned (port 0).
pub async fn persisted_port_for_store(config_root: &std::path::Path, store_id: u64) -> Option<u16> {
    let conf_dir = std::fs::read_dir(config_root).ok()?;
    for entry in conf_dir.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if let Some(rest) = name.strip_prefix(&format!("store{store_id}_group")) {
            if let Some(group_str) = rest.strip_suffix(".json") {
                if let Ok(group_id) = group_str.parse::<u64>() {
                    let store = GroupConfigStore::new(config_root, store_id, group_id);
                    if let Ok(Some(config)) = store.load().await {
                        for member in &config.members {
                            if let Some(port) = member
                                .endpoint
                                .rsplit(':')
                                .next()
                                .and_then(|p| p.parse::<u16>().ok())
                            {
                                if port > 0 {
                                    return Some(port);
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    None
}

#[derive(ToSchema, Deserialize)]
#[serde(rename_all = "snake_case")]
enum AddGroupInitialRole {
    Leader,
    Follower,
}

#[derive(ToSchema, Deserialize)]
struct AddGroupRequest {
    group_id: u64,
    replica_id: u64,
    #[serde(default)]
    initial_role: Option<AddGroupInitialRole>,
    /// When `Some(false)`, the group is added **without** starting its election
    /// driver, so it cannot self-elect at `quorum == 1` before its remotes are
    /// wired. The subsequent remote-wiring rebuild
    /// (`add_remote_replicas`) starts the driver with a correct quorum. Defaults
    /// to starting the driver (backward compatible).
    #[serde(default)]
    start_election: Option<bool>,
}

/// Request body for [`join_group_via_snapshot`]: bootstrap a new/far-lagging
/// group member by pulling a snapshot from an existing member instead of
/// replaying full Paxos history.
#[derive(ToSchema, Deserialize)]
struct JoinGroupRequest {
    replica_id: u64,
    /// gRPC endpoint (`host:port`) of an existing, already-caught-up member
    /// of this group to pull the snapshot from. Must run the **same**
    /// crow-tree backend as this store -- `KVEngine::snapshot_import`
    /// is only ever meaningful fed a stream from the same engine kind's
    /// `snapshot_export`.
    peer_endpoint: String,
}

#[derive(ToSchema, Serialize, Deserialize, Clone)]
struct RemoteReplicaInfo {
    replica_id: u64,
    endpoint: String,
    /// Whether this remote counts toward quorum (`PxGroup::recompute_quorum`
    /// only counts voting members). Defaults to `true` for backward
    /// compatibility with callers predating snapshot-join
    /// flow. A newly-joined member is typically wired as `false` on its
    /// peers until it has caught up via [`join_group_via_snapshot`], then
    /// promoted with a follow-up call that re-adds it as `true`.
    #[serde(default = "default_voting_true")]
    voting: bool,
}

fn default_voting_true() -> bool {
    true
}

#[derive(ToSchema, Serialize)]
struct RemoteListResponse {
    remotes: Vec<RemoteReplicaInfo>,
}

#[derive(ToSchema, Serialize, Deserialize)]
struct TopologyResponse {
    stores: Vec<StoreStatus>,
}

#[derive(ToSchema, Serialize)]
struct ErrorResponse {
    error: String,
}

fn err_json(status: StatusCode, msg: impl Into<String>) -> (StatusCode, Json<ErrorResponse>) {
    (status, Json(ErrorResponse { error: msg.into() }))
}

// ── Handlers ────────────────────────────────────────────────

#[derive(OpenApi)]
#[openapi(
    paths(
        health_check,
        list_stores,
        get_store,
        add_store,
        remove_store,
        list_groups,
        add_group,
        remove_group,
        list_remote_replicas,
        add_remote_replicas,
        remove_remote_replica,
        batch_add_remote_replicas,
        step_down,
        join_group_via_snapshot,
        group_readiness,
        system_init,
        topology_finalize,
        topology_ready,
        get_operation,
        export_topology
    ),
    components(
        schemas(
            HealthResponse,
            StoreStatus,
            GroupStatus,
            ReplicaStatus,
            RemoteStatus,
            StoreListResponse,
            StoreSummary,
            StoreDetail,
            GroupSummary,
            SystemInitRequest,
            SystemInitResponse,
            TopologyFinalizeResponse,
            TopologyReadyResponse,
            AddStoreRequest,
            AddGroupRequest,
            JoinGroupRequest,
            RemoteReplicaInfo,
            RemoteListResponse,
            TopologyResponse,
            StepDownBody,
            StepDownResult,
            ReadinessResponse,
            OperationResponse,
            OperationTarget,
            AsyncOperationResponse,
            ErrorResponse
        )
    ),
    tags((name = "management", description = "CrowKV management API"))
)]
pub struct ApiDoc;

/// Serialize the `OpenAPI` document to JSON.
///
/// # Panics
///
/// Panics if the `OpenAPI` document cannot be serialized to JSON (should never happen with valid utoipa annotations).
#[must_use]
pub fn openapi_json() -> serde_json::Value {
    serde_json::to_value(ApiDoc::openapi()).expect("OpenAPI document should serialize")
}

async fn openapi_spec() -> Json<serde_json::Value> {
    Json(openapi_json())
}

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
async fn health_check(State(state): State<RegistryArc>) -> (StatusCode, Json<HealthResponse>) {
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
async fn system_init(
    State(state): State<RegistryArc>,
    req: Option<Json<SystemInitRequest>>,
) -> Result<(StatusCode, Json<SystemInitResponse>), (StatusCode, Json<ErrorResponse>)> {
    const SYSTEM_STORE_ID: u64 = 0;
    const SYSTEM_GROUP_ID: u64 = 0;

    let req = req.map_or(
        SystemInitRequest {
            replica_id: default_replica_id(),
            start_election: default_start_election(),
        },
        |Json(r)| r,
    );

    // Create store 0 if it does not exist.
    if !state.stores.contains_key(&SYSTEM_STORE_ID) {
        let addr: SocketAddr = "0.0.0.0:0"
            .parse()
            .map_err(|e| err_json(StatusCode::BAD_REQUEST, format!("invalid address: {e}")))?;
        let mut store = PxKvStore::new(SYSTEM_STORE_ID, addr);
        if let Some(ref mr) = state.metrics_registry {
            store.set_metrics_registry(Arc::clone(mr));
        }
        let store = Arc::new(store);
        store.start().await.map_err(|e| {
            err_json(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to start store 0: {e}"),
            )
        })?;
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

    let group = create_group_with_wal(
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

/// `POST /topology/finalize` — idempotent cutover from console TOML to
/// group 0 authoritative. Proposes a KV put of `/topology/ready` = `true`
/// into group 0 on store 0. Returns `200` if already finalized (key
/// exists) or if the proposal succeeds.
#[utoipa::path(
        post,
        path = "/topology/finalize",
        tag = "management",
        responses(
            (status = 200, description = "Topology finalized", body = TopologyFinalizeResponse),
            (status = 404, description = "Store 0 or group 0 not found", body = ErrorResponse),
            (status = 409, description = "Not leader; retry at hinted leader", body = ErrorResponse),
            (status = 500, description = "Proposal failed", body = ErrorResponse)
        )
    )]
async fn topology_finalize(
    State(state): State<RegistryArc>,
) -> Result<(StatusCode, Json<TopologyFinalizeResponse>), (StatusCode, Json<ErrorResponse>)> {
    let store = state
        .get_store(0)
        .ok_or_else(|| err_json(StatusCode::NOT_FOUND, "store 0 not found"))?;
    // Ensure group 0 exists.
    if store.get_group(0).is_none() {
        return Err(err_json(StatusCode::NOT_FOUND, "group 0 not found in store 0"));
    }

    // Check if already finalized (idempotent).
    let get_resp = store.kv_get(0, topology_kv::READY_KEY, 0, 0, 0, 0).await;
    if get_resp.ok && !get_resp.value.is_empty() {
        return Ok((
            StatusCode::OK,
            Json(TopologyFinalizeResponse {
                ready: true,
                already_finalized: true,
            }),
        ));
    }

    // Propose the ready key.
    let put_resp = store.kv_put(0, topology_kv::READY_KEY, b"true", 0, 0, 0, 0).await;
    if !put_resp.ok {
        if put_resp.error == "not leader" {
            return Err(err_json(
                StatusCode::CONFLICT,
                format!("not leader; hint: {}", put_resp.not_leader_hint),
            ));
        }
        return Err(err_json(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("topology finalize proposal failed: {}", put_resp.error),
        ));
    }

    info!("topology finalized: /topology/ready key written to group 0");

    Ok((
        StatusCode::OK,
        Json(TopologyFinalizeResponse {
            ready: true,
            already_finalized: false,
        }),
    ))
}

/// `GET /topology/ready` — check whether group 0 has the `/topology/ready`
/// flag key set, indicating the cutover to group 0 authoritative is complete.
#[utoipa::path(
        get,
        path = "/topology/ready",
        tag = "management",
        responses(
            (status = 200, description = "Readiness checked", body = TopologyReadyResponse),
            (status = 404, description = "Store 0 or group 0 not found", body = ErrorResponse)
        )
    )]
async fn topology_ready(
    State(state): State<RegistryArc>,
) -> Result<(StatusCode, Json<TopologyReadyResponse>), (StatusCode, Json<ErrorResponse>)> {
    let store = state
        .get_store(0)
        .ok_or_else(|| err_json(StatusCode::NOT_FOUND, "store 0 not found"))?;
    // Ensure group 0 exists.
    if store.get_group(0).is_none() {
        return Err(err_json(StatusCode::NOT_FOUND, "group 0 not found in store 0"));
    }

    let resp = store.kv_get(0, topology_kv::READY_KEY, 0, 0, 0, 0).await;
    let ready = resp.ok && !resp.value.is_empty();

    Ok((StatusCode::OK, Json(TopologyReadyResponse { ready })))
}

#[utoipa::path(
        get,
        path = "/stores",
        tag = "management",
        responses((status = 200, description = "Stores in this server", body = StoreListResponse))
    )]
async fn list_stores(State(state): State<RegistryArc>) -> Json<StoreListResponse> {
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
async fn get_store(
    State(state): State<RegistryArc>,
    Path(sid): Path<u64>,
) -> Result<Json<StoreDetail>, (StatusCode, Json<ErrorResponse>)> {
    let store = state
        .get_store(sid)
        .ok_or_else(|| err_json(StatusCode::NOT_FOUND, format!("store {sid} not found")))?;

    let groups = store.group_summaries();
    let group_list: Vec<GroupSummary> = groups
        .into_iter()
        .map(
            |(group_id, local_replica_id, leader_id, remote_count)| GroupSummary {
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
async fn add_store(
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
    // config file > OS-assigned (0).
    let port = req.port.filter(|&p| p > 0);
    let port = if port.is_none() {
        match state.next_port() {
            Some(p) => Some(p),
            None => persisted_port_for_store(&state.config.config_root, req.store_id).await,
        }
    } else {
        port
    };
    let addr: SocketAddr = format!("0.0.0.0:{}", port.unwrap_or(0))
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
    let store = Arc::new(store);

    if let Err(e) = store.start().await {
        return Err(err_json(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to start store gRPC server: {e}"),
        ));
    }

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
async fn remove_store(
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
    info!(
        store_id = sid,
        error_count = report.errors.len(),
        "PxKvStore removed via management API"
    );
    Ok(StatusCode::OK)
}

#[utoipa::path(
        get,
        path = "/stores/{sid}/groups",
        tag = "management",
        params(("sid" = u64, Path, description = "Store id")),
        responses(
            (status = 200, description = "Groups in the store", body = Vec<GroupSummary>),
            (status = 404, description = "Store not found", body = ErrorResponse)
        )
    )]
async fn list_groups(
    State(state): State<RegistryArc>,
    Path(sid): Path<u64>,
) -> Result<Json<Vec<GroupSummary>>, (StatusCode, Json<ErrorResponse>)> {
    let store = state
        .get_store(sid)
        .ok_or_else(|| err_json(StatusCode::NOT_FOUND, format!("store {sid} not found")))?;

    let groups: Vec<GroupSummary> = store
        .group_summaries()
        .into_iter()
        .map(
            |(group_id, local_replica_id, leader_id, remote_count)| GroupSummary {
                group_id,
                local_replica_id,
                leader_id,
                remote_count,
            },
        )
        .collect();

    Ok(Json(groups))
}

#[utoipa::path(
        post,
        path = "/stores/{sid}/groups",
        tag = "management",
        params(("sid" = u64, Path, description = "Store id")),
        request_body = AddGroupRequest,
        responses(
            (status = 201, description = "Group created"),
            (status = 404, description = "Store not found", body = ErrorResponse),
            (status = 409, description = "Group already exists", body = ErrorResponse)
        )
    )]
async fn add_group(
    State(state): State<RegistryArc>,
    Path(sid): Path<u64>,
    Json(req): Json<AddGroupRequest>,
) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
    let store = state
        .get_store(sid)
        .ok_or_else(|| err_json(StatusCode::NOT_FOUND, format!("store {sid} not found")))?;

    if store.get_group(req.group_id).is_some() {
        return Err(err_json(
            StatusCode::CONFLICT,
            format!("group {} already exists in store {sid}", req.group_id),
        ));
    }

    debug!(
        store_id = sid,
        group_id = req.group_id,
        replica_id = req.replica_id,
        "creating PxGroup with local replica via management API"
    );
    let initial_role = match req.initial_role.unwrap_or(AddGroupInitialRole::Leader) {
        AddGroupInitialRole::Leader => PxLocalReplicaRole::Leader,
        AddGroupInitialRole::Follower => PxLocalReplicaRole::Follower,
    };
    let group = create_group_with_wal(
        sid,
        req.group_id,
        req.replica_id,
        initial_role,
        &state.config,
        state.wal_backend.clone(),
        state.crowtree_backend,
    )
    .await
    .map_err(|e| {
        err_json(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!(
                "failed to create WAL-backed group {} in store {sid}: {e}",
                req.group_id
            ),
        )
    })?;
    let group = group;
    // Defer the election driver when the caller is about to wire remotes
    // (multi-replica restore / creation). Avoids a `quorum == 1` self-election
    // running `bulk_phase1` / `repair_once` against only itself, which can
    // erase committed data. The driver is started by
    // the subsequent `add_remote_replicas` rebuild.
    let start_election = req.start_election.unwrap_or(true);
    if start_election && group.quorum() == 1 {
        let current_term = group.local_replica().current_term_snapshot();
        if current_term == 0 {
            group.local_replica().become_candidate(1);
            group.local_replica().persist_current_vote().await;
            group.local_replica().become_leader();
        }
        group.stamp_proposing_term(group.local_replica().current_term_snapshot());
    }
    if start_election {
        store.add_group(group);
    } else if group.quorum() > 1 {
        // Remotes were restored from the persisted config file, so the
        // group already has the correct quorum. Start the election driver
        // now — the quorum=1 deferral does not apply.
        info!(
            store_id = sid,
            group_id = req.group_id,
            quorum = group.quorum(),
            "starting election driver for group with persisted config remotes"
        );
        store.add_group(group);
    } else {
        store.add_group_without_election(group);
    }

    info!(
        store_id = sid,
        group_id = req.group_id,
        replica_id = req.replica_id,
        start_election,
        "PxGroup added via management API"
    );
    Ok(StatusCode::CREATED)
}

/// `POST /stores/{sid}/groups/{gid}/join` — new-member snapshot join
///: create this
/// store's local replica for `gid` and bootstrap its state by pulling a
/// snapshot from `peer_endpoint` instead of replaying full Paxos history.
///
/// The group is added **without** wiring any remotes and **without**
/// starting its election driver (mirrors `add_group`'s `quorum == 1`
/// self-election guard) — this replica is not yet part of the group's
/// topology on either side. The caller's follow-up steps:
/// 1. `POST.../remotes` on **this** store to wire the group's existing
///    members as this replica's remotes (`voting: true`, since they're
///    already established).
/// 2. `POST.../remotes` on **each existing member** to add this replica
///    as their remote with `voting: false`, so it starts receiving
///    heartbeat/repair catch-up for the WAL tail without affecting quorum.
/// 3. Once caught up, re-add this replica everywhere with `voting: true`
///    to promote it to a full voting member.
#[utoipa::path(
        post,
        path = "/stores/{sid}/groups/{gid}/join",
        tag = "management",
        params(
            ("sid" = u64, Path, description = "Store id"),
            ("gid" = u64, Path, description = "Group id")
        ),
        request_body = JoinGroupRequest,
        responses(
            (status = 201, description = "Group created and snapshot import succeeded"),
            (status = 404, description = "Store not found", body = ErrorResponse),
            (status = 409, description = "Group already exists", body = ErrorResponse),
            (status = 502, description = "Snapshot pull from peer_endpoint failed", body = ErrorResponse)
        )
    )]
async fn join_group_via_snapshot(
    State(state): State<RegistryArc>,
    Path((sid, gid)): Path<(u64, u64)>,
    Json(req): Json<JoinGroupRequest>,
) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
    let store = state
        .get_store(sid)
        .ok_or_else(|| err_json(StatusCode::NOT_FOUND, format!("store {sid} not found")))?;

    if store.get_group(gid).is_some() {
        return Err(err_json(
            StatusCode::CONFLICT,
            format!("group {gid} already exists in store {sid}"),
        ));
    }

    info!(
        store_id = sid,
        group_id = gid,
        replica_id = req.replica_id,
        peer_endpoint = %req.peer_endpoint,
        "joining PxGroup via snapshot pull"
    );
    let group = create_group_with_wal(
        sid,
        gid,
        req.replica_id,
        PxLocalReplicaRole::Follower,
        &state.config,
        state.wal_backend.clone(),
        state.crowtree_backend,
    )
    .await
    .map_err(|e| {
        err_json(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to create WAL-backed group {gid} in store {sid}: {e}"),
        )
    })?;

    let at_slot = group.join_via_snapshot(&req.peer_endpoint).await.map_err(|e| {
        err_json(
            StatusCode::BAD_GATEWAY,
            format!("snapshot join against {} failed: {e}", req.peer_endpoint),
        )
    })?;
    // The frontier moved from 0 to `at_slot` (or further, if a concurrent
    // catch-up already advanced it) after `create_group_with_wal` computed
    // `next_slot` from a still-empty replica; recompute so a future
    // proposal (once this replica becomes leader-eligible) doesn't clash
    // with an already-applied slot.
    let next_slot = group
        .local_replica()
        .highest_seen_slot()
        .max(group.local_replica().last_chosen_slot())
        .max(group.local_replica().contiguous_applied())
        .saturating_add(1)
        .max(1);
    group.set_next_slot(next_slot);

    // No remotes wired yet on either side -- same self-election hazard
    // `add_group` guards against, so defer the driver (see that handler's
    // comment for the full reasoning).
    store.add_group_without_election(group);

    info!(
        store_id = sid,
        group_id = gid,
        replica_id = req.replica_id,
        at_slot,
        "PxGroup joined via snapshot; remotes must be wired separately"
    );
    Ok(StatusCode::CREATED)
}

#[utoipa::path(
        delete,
        path = "/stores/{sid}/groups/{gid}",
        tag = "management",
        params(
            ("sid" = u64, Path, description = "Store id"),
            ("gid" = u64, Path, description = "Group id")
        ),
        responses(
            (status = 200, description = "Group removed"),
            (status = 404, description = "Store or group not found", body = ErrorResponse)
        )
    )]
async fn remove_group(
    State(state): State<RegistryArc>,
    Path((sid, gid)): Path<(u64, u64)>,
) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
    let store = state
        .get_store(sid)
        .ok_or_else(|| err_json(StatusCode::NOT_FOUND, format!("store {sid} not found")))?;

    info!(
        store_id = sid,
        group_id = gid,
        "removing PxGroup via management API"
    );
    if !store.remove_group(gid) {
        return Err(err_json(
            StatusCode::NOT_FOUND,
            format!("group {gid} not found in store {sid}"),
        ));
    }

    info!(
        store_id = sid,
        group_id = gid,
        "PxGroup removed via management API"
    );
    Ok(StatusCode::OK)
}

#[utoipa::path(
        get,
        path = "/stores/{sid}/groups/{gid}/remotes",
        tag = "management",
        params(
            ("sid" = u64, Path, description = "Store id"),
            ("gid" = u64, Path, description = "Group id")
        ),
        responses(
            (status = 200, description = "Remote replicas", body = RemoteListResponse),
            (status = 404, description = "Store or group not found", body = ErrorResponse)
        )
    )]
async fn list_remote_replicas(
    State(state): State<RegistryArc>,
    Path((sid, gid)): Path<(u64, u64)>,
) -> Result<Json<RemoteListResponse>, (StatusCode, Json<ErrorResponse>)> {
    let store = state
        .get_store(sid)
        .ok_or_else(|| err_json(StatusCode::NOT_FOUND, format!("store {sid} not found")))?;
    let group = store.get_group(gid).ok_or_else(|| {
        err_json(
            StatusCode::NOT_FOUND,
            format!("group {gid} not found in store {sid}"),
        )
    })?;

    let remotes: Vec<RemoteReplicaInfo> = group
        .remote_replica_info()
        .into_iter()
        .map(|(id, endpoint, voting)| RemoteReplicaInfo {
            replica_id: id,
            endpoint: endpoint.to_string(),
            voting,
        })
        .collect();

    Ok(Json(RemoteListResponse { remotes }))
}

#[utoipa::path(
        post,
        path = "/stores/{sid}/groups/{gid}/remotes",
        tag = "management",
        params(
            ("sid" = u64, Path, description = "Store id"),
            ("gid" = u64, Path, description = "Group id")
        ),
        request_body = Vec<RemoteReplicaInfo>,
        responses(
            (status = 200, description = "Remote replicas added"),
            (status = 400, description = "Invalid remote replica", body = ErrorResponse),
            (status = 404, description = "Store or group not found", body = ErrorResponse)
        )
    )]
async fn add_remote_replicas(
    State(state): State<RegistryArc>,
    Path((sid, gid)): Path<(u64, u64)>,
    Json(remotes): Json<Vec<RemoteReplicaInfo>>,
) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
    let store = state
        .get_store(sid)
        .ok_or_else(|| err_json(StatusCode::NOT_FOUND, format!("store {sid} not found")))?;
    let group = store.get_group(gid).ok_or_else(|| {
        err_json(
            StatusCode::NOT_FOUND,
            format!("group {gid} not found in store {sid}"),
        )
    })?;

    let local_id = group.local_replica().id;
    for r in &remotes {
        if r.replica_id == local_id {
            return Err(err_json(
                StatusCode::BAD_REQUEST,
                format!(
                    "cannot add local replica {} as remote; local replicas are managed with the group",
                    r.replica_id
                ),
            ));
        }
    }

    debug!(
        store_id = sid,
        group_id = gid,
        count = remotes.len(),
        "adding remote replicas via management API"
    );
    for r in &remotes {
        debug!(
            store_id = sid,
            group_id = gid,
            remote_id = r.replica_id,
            endpoint = %r.endpoint,
            voting = r.voting,
            "adding remote replica"
        );
    }
    let new_remotes: Vec<(u64, String, bool)> = remotes
        .iter()
        .map(|r| (r.replica_id, r.endpoint.clone(), r.voting))
        .collect();
    let new_group = rebuild_group_with_new_remotes(&group, &new_remotes);
    store.add_group(new_group);
    // Re-persist after add_group so the local replica's endpoint is set.
    if let Some(g) = store.get_group(gid) {
        g.persist_config().await;
    }

    info!(
        store_id = sid,
        group_id = gid,
        count = remotes.len(),
        "remote replicas added via management API"
    );
    Ok(StatusCode::OK)
}

#[utoipa::path(
        delete,
        path = "/stores/{sid}/groups/{gid}/remotes/{rid}",
        tag = "management",
        params(
            ("sid" = u64, Path, description = "Store id"),
            ("gid" = u64, Path, description = "Group id"),
            ("rid" = u64, Path, description = "Remote replica id")
        ),
        responses(
            (status = 200, description = "Remote replica removed"),
            (status = 400, description = "Local replica cannot be removed as remote", body = ErrorResponse),
            (status = 404, description = "Store, group, or remote replica not found", body = ErrorResponse)
        )
    )]
async fn remove_remote_replica(
    State(state): State<RegistryArc>,
    Path((sid, gid, rid)): Path<(u64, u64, u64)>,
) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
    let store = state
        .get_store(sid)
        .ok_or_else(|| err_json(StatusCode::NOT_FOUND, format!("store {sid} not found")))?;
    let group = store.get_group(gid).ok_or_else(|| {
        err_json(
            StatusCode::NOT_FOUND,
            format!("group {gid} not found in store {sid}"),
        )
    })?;

    let local_id = group.local_replica().id;
    if rid == local_id {
        return Err(err_json(
            StatusCode::BAD_REQUEST,
            "cannot remove local replica; local replicas are managed with the group",
        ));
    }

    // Check if remote exists
    let exists = group.remote_replica_info().iter().any(|(id, _, _)| *id == rid);
    if !exists {
        return Err(err_json(
            StatusCode::NOT_FOUND,
            format!("remote replica {rid} not found in group {gid}"),
        ));
    }

    info!(
        store_id = sid,
        group_id = gid,
        remote_id = rid,
        "removing remote replica via management API"
    );
    // Reconstruct group without this remote, preserving voting flags.
    // Carry every existing remote over verbatim (bulk, non-bumping
    // `set_remote_replicas`, including `rid` itself for now), then remove
    // `rid` through the bump-aware `remove_remote_replica` -- see the
    // matching comment in `add_remote_replicas` for why a loop of
    // `add_remote_replica` calls over the *surviving* members would bump
    // the epoch once per survivor instead of once for the actual removal.
    let mut new_group = rebuild_group_with_same_config(&group);
    new_group.set_remote_replicas(
        group
            .remote_replica_info()
            .into_iter()
            .map(|(id, endpoint, voting)| PxRemoteReplica::new(id, endpoint.to_string()).with_voting(voting))
            .collect(),
    );
    new_group.remove_remote_replica(rid);
    let current_term = group.local_replica().current_term_snapshot();
    if new_group.quorum() == 1 {
        new_group.local_replica().become_leader();
        new_group.local_replica().persist_current_vote().await;
        new_group.stamp_proposing_term(current_term);
    } else if group.leader_id() == rid {
        new_group.local_replica().become_follower(current_term);
        new_group.local_replica().clear_vote_lockout();
    }
    store.add_group(new_group);
    // Re-persist after add_group so the local replica's endpoint is set.
    if let Some(g) = store.get_group(gid) {
        g.persist_config().await;
    }

    info!(
        store_id = sid,
        group_id = gid,
        remote_id = rid,
        "remote replica removed via management API"
    );
    Ok(StatusCode::OK)
}

#[derive(Debug, Clone, Deserialize, ToSchema)]
struct StepDownBody {
    /// Free-text reason surfaced in the replica's own logs; purely
    /// diagnostic, no effect on the strict-fence decision.
    #[serde(default)]
    reason: String,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
struct StepDownResult {
    accepted: bool,
    current_term: u64,
    current_leader_id: u64,
}

#[utoipa::path(
        post,
        path = "/stores/{sid}/groups/{gid}/step-down",
        tag = "management",
        params(
            ("sid" = u64, Path, description = "Store id"),
            ("gid" = u64, Path, description = "Group id")
        ),
        request_body = StepDownBody,
        responses(
            (status = 200, description = "Step-down attempted; `accepted` is false if this node was not leader", body = StepDownResult),
            (status = 404, description = "Store or group not found", body = ErrorResponse)
        )
    )]
async fn step_down(
    State(state): State<RegistryArc>,
    Path((sid, gid)): Path<(u64, u64)>,
    Query(sync): Query<SyncQuery>,
    Json(body): Json<StepDownBody>,
) -> Result<axum::response::Response, (StatusCode, Json<ErrorResponse>)> {
    let store = state
        .get_store(sid)
        .ok_or_else(|| err_json(StatusCode::NOT_FOUND, format!("store {sid} not found")))?;
    let group = store.get_group(gid).ok_or_else(|| {
        err_json(
            StatusCode::NOT_FOUND,
            format!("group {gid} not found in store {sid}"),
        )
    })?;

    let reply = group.step_down_if_leader(&body.reason);
    info!(
        store_id = sid,
        group_id = gid,
        accepted = reply.accepted,
        sync = sync.is_sync(),
        "step-down requested via management API"
    );

    if sync.is_sync() || !reply.accepted {
        // Synchronous mode, or step-down was not accepted (no async needed)
        return Ok(Json(StepDownResult {
            accepted: reply.accepted,
            current_term: reply.current_term,
            current_leader_id: reply.current_leader_id,
        })
        .into_response());
    }

    // Async mode: create operation, spawn leader-wait task, return 202
    let op_id = state.operations.create(
        OperationKind::StepDown,
        OperationTarget {
            store_id: sid,
            group_id: gid,
            replica_id: None,
        },
    );
    spawn_leader_wait(state, op_id, sid, gid, std::time::Duration::from_secs(10));

    Ok((
        StatusCode::ACCEPTED,
        Json(AsyncOperationResponse {
            operation_id: op_id,
            status: "pending".to_string(),
        }),
    )
        .into_response())
}

#[utoipa::path(
        post,
        path = "/stores/{sid}/groups/{gid}/remotes/batch",
        tag = "management",
        params(
            ("sid" = u64, Path, description = "Store id"),
            ("gid" = u64, Path, description = "Group id")
        ),
        request_body = TopologyResponse,
        responses(
            (status = 200, description = "Remote replicas added from topology"),
            (status = 404, description = "Store or group not found", body = ErrorResponse)
        )
    )]
async fn batch_add_remote_replicas(
    State(state): State<RegistryArc>,
    Path((sid, gid)): Path<(u64, u64)>,
    Json(topology): Json<TopologyResponse>,
) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
    let store = state
        .get_store(sid)
        .ok_or_else(|| err_json(StatusCode::NOT_FOUND, format!("store {sid} not found")))?;
    let group = store.get_group(gid).ok_or_else(|| {
        err_json(
            StatusCode::NOT_FOUND,
            format!("group {gid} not found in store {sid}"),
        )
    })?;

    let local_id = group.local_replica().id;
    let mut new_remotes = Vec::new();

    for topo_store in &topology.stores {
        let Some(addr) = topo_store.listen_addr.as_ref() else {
            continue;
        };
        let addr = addr.clone();
        for topo_group in &topo_store.groups {
            if topo_group.group_id == gid && topo_group.local_replica_id != local_id {
                new_remotes.push(RemoteReplicaInfo {
                    replica_id: topo_group.local_replica_id,
                    endpoint: addr.clone(),
                    voting: true,
                });
            }
        }
    }

    if new_remotes.is_empty() {
        info!(
            store_id = sid,
            group_id = gid,
            "batch add remotes: no new remotes to add"
        );
        return Ok(StatusCode::OK);
    }

    debug!(
        store_id = sid,
        group_id = gid,
        count = new_remotes.len(),
        "batch adding remote replicas via management API"
    );
    for r in &new_remotes {
        debug!(
            store_id = sid,
            group_id = gid,
            remote_id = r.replica_id,
            endpoint = %r.endpoint,
            voting = r.voting,
            "batch adding remote replica"
        );
    }
    let remotes_tuple: Vec<(u64, String, bool)> = new_remotes
        .iter()
        .map(|r| (r.replica_id, r.endpoint.clone(), r.voting))
        .collect();
    let new_group = rebuild_group_with_new_remotes(&group, &remotes_tuple);
    store.add_group(new_group);
    // Re-persist after add_group so the local replica's endpoint is set.
    if let Some(g) = store.get_group(gid) {
        g.persist_config().await;
    }

    info!(
        store_id = sid,
        group_id = gid,
        count = new_remotes.len(),
        "batch remote replicas added via management API"
    );
    Ok(StatusCode::OK)
}

/// `GET /topology` (alias `/top`) — full hierarchy with per-remote RPC
/// metrics and cheap kv-store stats.
#[utoipa::path(
        get,
        path = "/topology",
        tag = "management",
        responses((status = 200, description = "Cluster topology status", body = TopologyResponse))
    )]
async fn export_topology(State(state): State<RegistryArc>) -> impl IntoResponse {
    let stores: Vec<StoreStatus> = state.stores.iter().map(|entry| entry.value().status()).collect();
    let body = serde_json::to_string_pretty(&TopologyResponse { stores }).unwrap();
    ([("content-type", "application/json")], body)
}

/// Rebuild `group` with `new_remotes` merged into its remote list,
/// applying the membership-epoch bump correctly.
/// Shared by `add_remote_replicas` and
/// `batch_add_remote_replicas` -- both need the exact same bootstrap
/// handling, and having it live in one place (rather than duplicated
/// per-handler) is what actually keeps them consistent; a previous
/// version of this fix only special-cased one of the two call sites and
/// broke a real multi-node test that fans out through the other.
///
/// Existing remotes are carried over via the bulk, non-bumping
/// `set_remote_replicas` -- never through the bump-aware
/// `add_remote_replica`, which would treat every replay of an
/// already-known member as a fresh voting-set change and bump once per
/// *existing* member on every single mutation call.
///
/// If `group` currently has **no** remotes at all -- a freshly-joined
/// replica's first-ever wiring (`join_group_via_snapshot`'s step 1:
/// "wire the group's existing members as this replica's remotes") --
/// every entry in `new_remotes` is folded into that same bulk seed
/// instead of going through `add_remote_replica`: bootstrapping a brand
/// new replica to match an already-agreed cluster state is not a
/// membership *change*, no matter what `voting` flags it carries, and
/// bumping here would desync its epoch from peers who never bump for a
/// non-voting add of that same replica. This only checks "is the
/// **target's own** remote list currently empty", so the caller is
/// responsible for landing a freshly-joined replica's entire bootstrap
/// wiring in one call (as `crowkv-console/web/src/mgmt.rs::http_add_replica`
/// already does) -- splitting it into several single-entry calls would
/// only protect the first one.
///
/// Otherwise, each entry in `new_remotes` goes through the bump-aware
/// `add_remote_replica`, so only genuine voting-set changes (new
/// member, promotion, demotion) bump the epoch.
fn rebuild_group_with_new_remotes(group: &PxGroup, new_remotes: &[(u64, String, bool)]) -> PxGroup {
    let mut new_group = rebuild_group_with_same_config(group);
    let existing = group.remote_replica_info();
    if existing.is_empty() {
        new_group.set_remote_replicas(
            new_remotes
                .iter()
                .map(|(id, endpoint, voting)| {
                    PxRemoteReplica::new(*id, endpoint.clone()).with_voting(*voting)
                })
                .collect(),
        );
    } else {
        new_group.set_remote_replicas(
            existing
                .into_iter()
                .map(|(id, endpoint, voting)| {
                    PxRemoteReplica::new(id, endpoint.to_string()).with_voting(voting)
                })
                .collect(),
        );
        for (id, endpoint, voting) in new_remotes {
            new_group.add_remote_replica(PxRemoteReplica::new(*id, endpoint.clone()).with_voting(*voting));
        }
    }
    new_group
}

/// Rebuild a `PxGroup` with the same config (`group_id`, `local_replica`,
/// `leader_id`, `force_classic`, `election_cfg`) but no remote replicas. Caller is
/// responsible for re-adding remote replicas.
///
/// **Important:** the new `PxLocalReplica` inherits the prior replica's
/// election persistent state (`current_term`, `voted_for`, `role`,
/// `leader_id`, `vote_lockout_until`) via
/// [`PxLocalReplica::new_inheriting_election_state`]. Without this, every
/// `add_remote_replicas` / `remove_remote_replica` rebuild would reset the cluster's
/// election term back to 0 and trigger a fresh election round, which
/// prevents leadership from converging when a multi-replica group is
/// being built up incrementally (each remote-add kills the elected
/// leader and starts a new race).
fn rebuild_group_with_same_config(group: &PxGroup) -> PxGroup {
    let lr = group.local_replica();
    let local_replica = PxLocalReplica::new_inheriting_election_state(lr);
    let mut new_group = PxGroup::new(group.group_id, local_replica);
    // `new_inheriting_election_state` already copies a consistent
    // (role, leader_id) snapshot from the prior replica under the mutex,
    // so `set_leader_id` is redundant here. The `role_atomic` and
    // `believed_leader_id` on the new replica already match.
    // Carry the unified config wholesale — replaces the former per-flag carry blocks.
    new_group.set_from_config(group.config());
    // Preserve `proposing_term` so the new group's leadership gate
    // (`role == Leader && current_term == proposing_term`) passes for
    // an already-elected leader that didn't have to re-stamp the term
    // on the rebuild path. Otherwise the gate fails with `NotLeader`
    // even though the replica is still Leader, because the fresh
    // `PxGroup` starts with `proposing_term = 0`.
    new_group.stamp_proposing_term(group.proposing_term());
    // Carry the membership epoch forward. Without this, every rebuild
    // (every add/remove/promote call) would silently reset it to 0 and
    // re-bump from there, making the epoch reflect only "did the last
    // mutation change the voting set" instead of a true count across
    // the group's whole history -- defeating the exact-match fence the
    // very next time two mutations land close together.
    new_group.set_membership_epoch(group.membership_epoch());
    if let Some(store) = group.config_store() {
        new_group.set_config_store(store.clone());
    }
    if let Some(node_store) = group.node_config_store() {
        let sid = group.node_config_store_sid().unwrap_or(0);
        new_group.set_node_config_store(node_store.clone(), sid, new_group.group_id());
    }
    new_group
}

// ── Async operation + readiness API ───────────────────────────

/// Query parameter for backward-compatible synchronous mode.
#[derive(Debug, Deserialize)]
pub struct SyncQuery {
    #[serde(default)]
    pub sync: Option<bool>,
}

impl SyncQuery {
    fn is_sync(&self) -> bool {
        self.sync.unwrap_or(false)
    }
}

/// Response for `GET /stores/:sid/groups/:gid/ready`.
#[derive(Serialize, ToSchema)]
pub struct ReadinessResponse {
    pub ready: bool,
    pub leader_id: u64,
    pub term: u64,
    pub voting_replicas: u32,
    pub reachable_replicas: u32,
    pub max_applied_slot: u64,
    pub min_applied_slot: u64,
    pub lag: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[utoipa::path(
    get,
    path = "/stores/{sid}/groups/{gid}/ready",
    tag = "management",
    params(
        ("sid" = u64, Path, description = "Store id"),
        ("gid" = u64, Path, description = "Group id")
    ),
    responses(
        (status = 200, description = "Group is ready", body = ReadinessResponse),
        (status = 503, description = "Group is not ready", body = ReadinessResponse),
        (status = 404, description = "Store or group not found", body = ErrorResponse)
    )
)]
async fn group_readiness(
    State(state): State<RegistryArc>,
    Path((sid, gid)): Path<(u64, u64)>,
) -> Result<(StatusCode, Json<ReadinessResponse>), (StatusCode, Json<ErrorResponse>)> {
    let store = state
        .get_store(sid)
        .ok_or_else(|| err_json(StatusCode::NOT_FOUND, format!("store {sid} not found")))?;
    let group = store.get_group(gid).ok_or_else(|| {
        err_json(
            StatusCode::NOT_FOUND,
            format!("group {gid} not found in store {sid}"),
        )
    })?;

    let status = group.status();
    let leader_id = status.leader_id;
    let local_applied = group.local_replica().contiguous_applied();

    let mut voting_count = 0u32;
    let mut reachable_count = 0u32;
    let max_applied = local_applied;
    let min_applied = local_applied;

    if group.local_replica().voting {
        voting_count += 1;
        if status.local_replica.status != StatusLevel::Unhealthy {
            reachable_count += 1;
        }
    }

    for remote in &status.remotes {
        if remote.voting {
            voting_count += 1;
            if remote.status != StatusLevel::Unhealthy {
                reachable_count += 1;
            }
        }
    }

    let lag = 0u64;

    let ready = leader_id != 0 && reachable_count > voting_count / 2;
    let reason = if leader_id == 0 {
        Some("no leader elected".to_string())
    } else if reachable_count <= voting_count / 2 {
        Some(format!("quorum not reachable: {reachable_count}/{voting_count}"))
    } else {
        None
    };

    let resp = ReadinessResponse {
        ready,
        leader_id,
        term: group.local_replica().current_term_snapshot(),
        voting_replicas: voting_count,
        reachable_replicas: reachable_count,
        max_applied_slot: max_applied,
        min_applied_slot: min_applied,
        lag,
        reason,
    };

    let code = if ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    Ok((code, Json(resp)))
}

/// Response for `GET /operations/:id`.
#[derive(Serialize, ToSchema)]
pub struct OperationResponse {
    pub id: u64,
    pub kind: String,
    pub status: String,
    pub target: OperationTarget,
    pub started_at_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl From<Operation> for OperationResponse {
    fn from(op: Operation) -> Self {
        Self {
            id: op.id,
            kind: op.kind.as_str().to_string(),
            status: op.status.as_str().to_string(),
            target: op.target,
            started_at_ms: op.started_at_ms,
            completed_at_ms: op.completed_at_ms,
            error: op.error,
        }
    }
}

#[utoipa::path(
    get,
    path = "/operations/{id}",
    tag = "management",
    params(
        ("id" = u64, Path, description = "Operation id")
    ),
    responses(
        (status = 200, description = "Operation status", body = OperationResponse),
        (status = 404, description = "Operation not found", body = ErrorResponse)
    )
)]
async fn get_operation(
    State(state): State<RegistryArc>,
    Path(id): Path<u64>,
) -> Result<Json<OperationResponse>, (StatusCode, Json<ErrorResponse>)> {
    match state.operations.get(id) {
        Some(op) => Ok(Json(op.into())),
        None => Err(err_json(
            StatusCode::NOT_FOUND,
            format!("operation {id} not found"),
        )),
    }
}

/// Response for async operations that return an operation ID.
#[derive(Serialize, ToSchema)]
pub struct AsyncOperationResponse {
    pub operation_id: u64,
    pub status: String,
}

/// Spawn a background task that polls group readiness until a new leader
/// appears, then marks the operation as completed or failed.
fn spawn_leader_wait(
    state: RegistryArc,
    operation_id: u64,
    store_id: u64,
    group_id: u64,
    timeout: std::time::Duration,
) {
    state
        .operations
        .update_status(operation_id, OperationStatus::Running, None);

    tokio::spawn(async move {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if tokio::time::Instant::now() >= deadline {
                state.operations.update_status(
                    operation_id,
                    OperationStatus::Failed,
                    Some("timed out waiting for new leader".to_string()),
                );
                return;
            }

            let Some(store) = state.get_store(store_id) else {
                state.operations.update_status(
                    operation_id,
                    OperationStatus::Failed,
                    Some("store disappeared during operation".to_string()),
                );
                return;
            };
            let Some(group) = store.get_group(group_id) else {
                state.operations.update_status(
                    operation_id,
                    OperationStatus::Failed,
                    Some("group disappeared during operation".to_string()),
                );
                return;
            };

            if group.leader_id() != 0 {
                state
                    .operations
                    .update_status(operation_id, OperationStatus::Completed, None);
                info!(
                    store_id,
                    group_id,
                    operation_id,
                    new_leader = group.leader_id(),
                    "async leader-wait operation completed"
                );
                return;
            }

            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
    });
}
