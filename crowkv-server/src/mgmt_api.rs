use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use tracing::info;
use utoipa::{OpenApi, ToSchema};

use crowkv::cluster::group::PxGroup;
use crowkv::cluster::group_election::LeaderElection;
use crowkv::cluster::kv_server::KvServer;
use crowkv::cluster::local_replica::{PxLocalReplica, PxLocalReplicaRole};
use crowkv::cluster::px_kv_store::PxKvStore;
use crowkv::cluster::remote_replica::PxRemoteReplica;
use crowkv::common::config::ServerConfig;

use crate::startup::create_group_with_wal;
use crate::store_registry::KvStoreRegistry;

type RegistryArc = Arc<KvStoreRegistry>;

pub fn router(state: RegistryArc) -> Router {
    Router::new()
        .route("/health", get(health_check))
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
        .route("/topology", get(export_topology))
        .route("/top", get(export_topology))
        .route("/openapi.json", get(openapi_spec))
        .with_state(state)
}

#[allow(unused_imports)]
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
struct AddStoreRequest {
    store_id: u64,
    #[serde(default)]
    port: Option<u16>,
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
    /// wired (see `doc/bug-wal.md` §8.4). The subsequent remote-wiring rebuild
    /// (`add_remote_replicas`) starts the driver with a correct quorum. Defaults
    /// to starting the driver (backward compatible).
    #[serde(default)]
    start_election: Option<bool>,
}

#[derive(ToSchema, Serialize, Deserialize, Clone)]
struct RemoteReplicaInfo {
    replica_id: u64,
    endpoint: String,
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
            AddStoreRequest,
            AddGroupRequest,
            RemoteReplicaInfo,
            RemoteListResponse,
            TopologyResponse,
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

    let addr: SocketAddr = format!("0.0.0.0:{}", req.port.unwrap_or(0))
        .parse()
        .map_err(|e| err_json(StatusCode::BAD_REQUEST, format!("invalid address: {e}")))?;

    info!(
        store_id = req.store_id,
        bind_addr = %addr,
        "creating PxKvStore via management API"
    );
    let store = Arc::new(PxKvStore::new(req.store_id, addr));

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

    info!(
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
        state.election_cfg,
        &state.wal_root,
        state.wal_backend.clone(),
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
    // Defer the election driver when the caller is about to wire remotes
    // (multi-replica restore / creation). Avoids a `quorum == 1` self-election
    // running `bulk_phase1` / `repair_once` against only itself, which can
    // erase committed data (`doc/bug-wal.md` §8.4). The driver is started by
    // the subsequent `add_remote_replicas` rebuild.
    let start_election = req.start_election.unwrap_or(true);
    if start_election {
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
        .map(|(id, endpoint)| RemoteReplicaInfo {
            replica_id: id,
            endpoint: endpoint.to_string(),
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

    info!(
        store_id = sid,
        group_id = gid,
        count = remotes.len(),
        "adding remote replicas via management API"
    );
    // PxGroup::add_remote_replica requires &mut self, but the group is behind Arc.
    // We need to reconstruct and replace the group.
    // Build new remotes list = existing + new
    let mut new_group = rebuild_group_with_same_config(&group);
    // Re-add existing remotes
    for (id, endpoint) in group.remote_replica_info() {
        new_group.add_remote_replica(PxRemoteReplica::new(id, endpoint.to_string()));
    }
    // Add new remotes
    for r in &remotes {
        info!(
            store_id = sid,
            group_id = gid,
            remote_id = r.replica_id,
            endpoint = %r.endpoint,
            "adding remote replica"
        );
        new_group.add_remote_replica(PxRemoteReplica::new(r.replica_id, r.endpoint.clone()));
    }
    store.add_group(new_group);

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
    let exists = group.remote_replica_info().iter().any(|(id, _)| *id == rid);
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
    // Reconstruct group without this remote
    let mut new_group = rebuild_group_with_same_config(&group);
    for (id, endpoint) in group.remote_replica_info() {
        if id != rid {
            new_group.add_remote_replica(PxRemoteReplica::new(id, endpoint.to_string()));
        }
    }
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

    info!(
        store_id = sid,
        group_id = gid,
        remote_id = rid,
        "remote replica removed via management API"
    );
    Ok(StatusCode::OK)
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

    info!(
        store_id = sid,
        group_id = gid,
        count = new_remotes.len(),
        "batch adding remote replicas via management API"
    );
    let mut new_group = rebuild_group_with_same_config(&group);
    for (id, endpoint) in group.remote_replica_info() {
        new_group.add_remote_replica(PxRemoteReplica::new(id, endpoint.to_string()));
    }
    for r in &new_remotes {
        info!(
            store_id = sid,
            group_id = gid,
            remote_id = r.replica_id,
            endpoint = %r.endpoint,
            "batch adding remote replica"
        );
        new_group.add_remote_replica(PxRemoteReplica::new(r.replica_id, r.endpoint.clone()));
    }
    store.add_group(new_group);

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
    new_group.set_election_config(group.election_config());
    // Preserve `proposing_term` so the new group's leadership gate
    // (`role == Leader && current_term == proposing_term`) passes for
    // an already-elected leader that didn't have to re-stamp the term
    // on the rebuild path. Otherwise the gate fails with `NotLeader`
    // even though the replica is still Leader, because the fresh
    // `PxGroup` starts with `proposing_term = 0`.
    new_group.stamp_proposing_term(group.proposing_term());
    if group.force_classic() {
        new_group.set_force_classic(true);
    }
    new_group
}
