use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use tracing::info;
#[cfg(feature = "swagger-ui")]
use utoipa::{OpenApi, ToSchema};
#[cfg(feature = "swagger-ui")]
use utoipa_swagger_ui::SwaggerUi;

use crowkv::cluster::group::PxGroup;
use crowkv::cluster::kv_server::KvServer;
use crowkv::cluster::local_replica::{PxLocalReplica, PxLocalReplicaRole};
use crowkv::cluster::px_kv_store::PxKvStore;
use crowkv::cluster::remote_replica::PxRemoteReplica;

use crate::state::KvStoreRegistry;

type RegistryArc = Arc<KvStoreRegistry>;

pub fn router(state: RegistryArc) -> Router {
    let router = Router::new()
        .route("/health", get(health_check))
        .route("/stores", get(list_stores).post(add_store))
        .route("/stores/:sid", get(get_store).delete(remove_store))
        .route("/stores/:sid/groups", get(list_groups).post(add_group))
        .route("/stores/:sid/groups/:gid", delete(remove_group))
        .route("/stores/:sid/groups/:gid/remotes", get(list_remotes).post(add_remotes))
        .route("/stores/:sid/groups/:gid/remotes/batch", post(batch_add_remotes))
        .route("/stores/:sid/groups/:gid/remotes/:rid", delete(remove_remote))
        .route("/topology", get(export_topology))
        .route("/top", get(export_topology))
        .with_state(state);

    #[cfg(feature = "swagger-ui")]
    let router = router.merge(SwaggerUi::new("/api")).route("/openapi.json", get(openapi_spec));

    router
}

// ── JSON types ──────────────────────────────────────────────

#[cfg_attr(feature = "swagger-ui", derive(ToSchema))]
#[derive(Serialize)]
struct HealthResponse {
    status: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    messages: Vec<String>,
    stores: Vec<HealthStore>,
}

#[cfg_attr(feature = "swagger-ui", derive(ToSchema))]
#[derive(Serialize)]
struct HealthStore {
    store_id: u64,
    status: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    messages: Vec<String>,
    groups: Vec<HealthGroup>,
}

#[cfg_attr(feature = "swagger-ui", derive(ToSchema))]
#[derive(Serialize)]
struct HealthGroup {
    group_id: u64,
    status: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    messages: Vec<String>,
    local_replica: HealthReplica,
    remotes: Vec<HealthRemote>,
}

#[cfg_attr(feature = "swagger-ui", derive(ToSchema))]
#[derive(Serialize)]
struct HealthReplica {
    id: u64,
    role: String,
    status: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    messages: Vec<String>,
}

#[cfg_attr(feature = "swagger-ui", derive(ToSchema))]
#[derive(Serialize)]
struct HealthRemote {
    id: u64,
    endpoint: String,
    status: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    messages: Vec<String>,
}

#[cfg_attr(feature = "swagger-ui", derive(ToSchema))]
#[derive(Serialize)]
struct StoreListResponse {
    stores: Vec<StoreSummary>,
}

#[cfg_attr(feature = "swagger-ui", derive(ToSchema))]
#[derive(Serialize)]
struct StoreSummary {
    store_id: u64,
    #[cfg_attr(feature = "swagger-ui", schema(value_type = Option<String>))]
    listen_addr: Option<SocketAddr>,
    group_count: usize,
}

#[cfg_attr(feature = "swagger-ui", derive(ToSchema))]
#[derive(Serialize)]
struct StoreDetail {
    store_id: u64,
    #[cfg_attr(feature = "swagger-ui", schema(value_type = Option<String>))]
    listen_addr: Option<SocketAddr>,
    groups: Vec<GroupSummary>,
}

#[cfg_attr(feature = "swagger-ui", derive(ToSchema))]
#[derive(Serialize)]
struct GroupSummary {
    group_id: u64,
    local_replica_id: u64,
    leader_id: u64,
    remote_count: usize,
}

#[cfg_attr(feature = "swagger-ui", derive(ToSchema))]
#[derive(Deserialize)]
struct AddStoreRequest {
    store_id: u64,
    group_id: u64,
    replica_id: u64,
    #[serde(default)]
    port: Option<u16>,
}

#[cfg_attr(feature = "swagger-ui", derive(ToSchema))]
#[derive(Deserialize)]
struct AddGroupRequest {
    group_id: u64,
    replica_id: u64,
}

#[cfg_attr(feature = "swagger-ui", derive(ToSchema))]
#[derive(Serialize, Deserialize, Clone)]
struct RemoteReplicaInfo {
    replica_id: u64,
    endpoint: String,
}

#[cfg_attr(feature = "swagger-ui", derive(ToSchema))]
#[derive(Serialize)]
struct RemoteListResponse {
    remotes: Vec<RemoteReplicaInfo>,
}

#[cfg_attr(feature = "swagger-ui", derive(ToSchema))]
#[derive(Serialize, Deserialize)]
struct TopologyResponse {
    stores: Vec<TopologyStore>,
}

#[cfg_attr(feature = "swagger-ui", derive(ToSchema))]
#[derive(Serialize, Deserialize)]
struct TopologyStore {
    store_id: u64,
    #[cfg_attr(feature = "swagger-ui", schema(value_type = Option<String>))]
    listen_addr: Option<SocketAddr>,
    groups: Vec<TopologyGroup>,
}

#[cfg_attr(feature = "swagger-ui", derive(ToSchema))]
#[derive(Serialize, Deserialize)]
struct TopologyGroup {
    group_id: u64,
    /// Backwards-compat alias for `local_replica.id`. Existing
    /// `batch_add_remotes` clients read this.
    local_replica_id: u64,
    leader_id: u64,
    force_classic: bool,
    local_replica: TopologyLocalReplica,
    remotes: Vec<TopologyRemote>,
}

#[cfg_attr(feature = "swagger-ui", derive(ToSchema))]
#[derive(Serialize, Deserialize)]
struct TopologyLocalReplica {
    id: u64,
    role: String,
    voting: bool,
    kv_store: TopologyKvStore,
}

#[cfg_attr(feature = "swagger-ui", derive(ToSchema))]
#[derive(Serialize, Deserialize)]
struct TopologyKvStore {
    key_count: u64,
}

#[cfg_attr(feature = "swagger-ui", derive(ToSchema))]
#[derive(Serialize, Deserialize)]
struct TopologyRemote {
    id: u64,
    endpoint: String,
    voting: bool,
    metrics: TopologyMetrics,
}

#[cfg_attr(feature = "swagger-ui", derive(ToSchema))]
#[derive(Serialize, Deserialize)]
struct TopologyMetrics {
    rpc_count: u64,
    err_count: u64,
    last_rtt_ms: u64,
}

#[cfg_attr(feature = "swagger-ui", derive(ToSchema))]
#[derive(Serialize)]
struct ErrorResponse {
    error: String,
}

fn err_json(status: StatusCode, msg: impl Into<String>) -> (StatusCode, Json<ErrorResponse>) {
    (status, Json(ErrorResponse { error: msg.into() }))
}

// ── Handlers ────────────────────────────────────────────────

#[cfg(feature = "swagger-ui")]
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
        list_remotes,
        add_remotes,
        remove_remote,
        batch_add_remotes,
        export_topology
    ),
    components(
        schemas(
            HealthResponse,
            HealthStore,
            HealthGroup,
            HealthReplica,
            HealthRemote,
            StoreListResponse,
            StoreSummary,
            StoreDetail,
            GroupSummary,
            AddStoreRequest,
            AddGroupRequest,
            RemoteReplicaInfo,
            RemoteListResponse,
            TopologyResponse,
            TopologyStore,
            TopologyGroup,
            TopologyLocalReplica,
            TopologyKvStore,
            TopologyRemote,
            TopologyMetrics,
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
#[cfg(feature = "swagger-ui")]
#[must_use]
pub fn openapi_json() -> serde_json::Value {
    serde_json::to_value(ApiDoc::openapi()).expect("OpenAPI document should serialize")
}

#[cfg(feature = "swagger-ui")]
async fn openapi_spec() -> Json<serde_json::Value> {
    Json(openapi_json())
}

#[cfg(not(feature = "swagger-ui"))]
#[allow(dead_code)]
fn openapi_spec() -> StatusCode {
    StatusCode::NOT_FOUND
}

/// `GET /health` — hierarchical cluster health report.
///
/// Aggregates per-layer cached status (no active probing in V1). Returns `200`
/// when overall status is `ok` / `degraded`, `503` when `unhealthy`
/// (load-balancer signal).
#[cfg_attr(
    feature = "swagger-ui",
    utoipa::path(
        get,
        path = "/health",
        tag = "management",
        responses(
            (status = 200, description = "Cluster is live", body = HealthResponse),
            (status = 503, description = "Cluster is unhealthy", body = HealthResponse)
        )
    )
)]
async fn health_check(State(state): State<RegistryArc>) -> (StatusCode, Json<HealthResponse>) {
    use crowkv::cluster::HealthStatus;

    let mut overall = HealthStatus::Ok;
    let mut messages: Vec<String> = Vec::new();
    let mut stores: Vec<HealthStore> = Vec::new();

    for entry in &state.stores {
        let store_id = *entry.key();
        let store = entry.value();
        let store_health = store.health();
        overall = HealthStatus::worst(overall, store_health.status);

        let mut groups: Vec<HealthGroup> = Vec::new();
        for (group_id, local_replica_id, leader_id, _) in store.group_summaries() {
            let Some(group) = store.get_group(group_id) else {
                continue;
            };
            let g_health = group.health();
            let role = if leader_id == local_replica_id { "leader" } else { "follower" };
            let local_health = group.local_replica().health();
            let local_replica = HealthReplica {
                id: local_replica_id,
                role: role.to_string(),
                status: local_health.status.as_str().to_string(),
                messages: local_health.messages,
            };
            let remotes: Vec<HealthRemote> = group
                .remote_replica_info()
                .into_iter()
                .filter_map(|(rid, endpoint)| {
                    let r = group.get_remote_replica(rid)?;
                    let h = r.health();
                    Some(HealthRemote {
                        id: rid,
                        endpoint: endpoint.to_string(),
                        status: h.status.as_str().to_string(),
                        messages: h.messages,
                    })
                })
                .collect();
            groups.push(HealthGroup {
                group_id,
                status: g_health.status.as_str().to_string(),
                messages: g_health.messages,
                local_replica,
                remotes,
            });
        }

        stores.push(HealthStore {
            store_id,
            status: store_health.status.as_str().to_string(),
            messages: store_health.messages,
            groups,
        });
    }

    if state.stores.is_empty() {
        // No stores configured yet — server is up but has nothing to serve.
        // Treat as Ok for liveness probes; operators can read messages.
        messages.push("no stores configured".to_string());
    }

    let http_status = if overall.is_unhealthy() { StatusCode::SERVICE_UNAVAILABLE } else { StatusCode::OK };

    (
        http_status,
        Json(HealthResponse {
            status: overall.as_str().to_string(),
            messages,
            stores,
        }),
    )
}

#[cfg_attr(
    feature = "swagger-ui",
    utoipa::path(
        get,
        path = "/stores",
        tag = "management",
        responses((status = 200, description = "Stores in this server", body = StoreListResponse))
    )
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
                listen_addr: store.listen_addr(),
                group_count: store.group_count(),
            }
        })
        .collect();
    Json(StoreListResponse { stores })
}

#[cfg_attr(
    feature = "swagger-ui",
    utoipa::path(
        get,
        path = "/stores/{sid}",
        tag = "management",
        params(("sid" = u64, Path, description = "Store id")),
        responses(
            (status = 200, description = "Store detail", body = StoreDetail),
            (status = 404, description = "Store not found", body = ErrorResponse)
        )
    )
)]
async fn get_store(State(state): State<RegistryArc>, Path(sid): Path<u64>) -> Result<Json<StoreDetail>, (StatusCode, Json<ErrorResponse>)> {
    let store = state.get_store(sid).ok_or_else(|| err_json(StatusCode::NOT_FOUND, format!("store {sid} not found")))?;

    let groups = store.group_summaries();
    let group_list: Vec<GroupSummary> = groups
        .into_iter()
        .map(|(group_id, local_replica_id, leader_id, remote_count)| GroupSummary {
            group_id,
            local_replica_id,
            leader_id,
            remote_count,
        })
        .collect();

    Ok(Json(StoreDetail {
        store_id: sid,
        listen_addr: store.listen_addr(),
        groups: group_list,
    }))
}

#[cfg_attr(
    feature = "swagger-ui",
    utoipa::path(
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
    )
)]
async fn add_store(State(state): State<RegistryArc>, Json(req): Json<AddStoreRequest>) -> Result<(StatusCode, Json<StoreSummary>), (StatusCode, Json<ErrorResponse>)> {
    if state.stores.contains_key(&req.store_id) {
        return Err(err_json(StatusCode::CONFLICT, format!("store {} already exists", req.store_id)));
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

    info!(
        store_id = req.store_id,
        group_id = req.group_id,
        replica_id = req.replica_id,
        "creating PxGroup with local replica via management API"
    );
    let local_replica = PxLocalReplica::new(req.replica_id, PxLocalReplicaRole::Follower);
    let group = PxGroup::new(req.group_id, local_replica);
    store.add_group(group);

    let started = store.start().await;
    if !started {
        return Err(err_json(StatusCode::INTERNAL_SERVER_ERROR, "failed to start store gRPC server"));
    }

    info!(
        store_id = req.store_id,
        listen_addr = ?store.listen_addr(),
        "PxKvStore added and started via management API"
    );

    let summary = StoreSummary {
        store_id: req.store_id,
        listen_addr: store.listen_addr(),
        group_count: 1,
    };

    state.add_store(req.store_id, store);
    Ok((StatusCode::CREATED, Json(summary)))
}

#[cfg_attr(
    feature = "swagger-ui",
    utoipa::path(
        delete,
        path = "/stores/{sid}",
        tag = "management",
        params(("sid" = u64, Path, description = "Store id")),
        responses(
            (status = 200, description = "Store removed"),
            (status = 404, description = "Store not found", body = ErrorResponse)
        )
    )
)]
async fn remove_store(State(state): State<RegistryArc>, Path(sid): Path<u64>) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
    info!(store_id = sid, "removing PxKvStore via management API");
    let store = state.remove_store(sid).ok_or_else(|| err_json(StatusCode::NOT_FOUND, format!("store {sid} not found")))?;
    let report = store.shutdown(crowkv::cluster::DEFAULT_SHUTDOWN_TIMEOUT).await;
    if !report.is_clean() {
        for err in &report.errors {
            tracing::error!(store_id = sid, "{err}");
        }
    }
    info!(store_id = sid, error_count = report.errors.len(), "PxKvStore removed via management API");
    Ok(StatusCode::OK)
}

#[cfg_attr(
    feature = "swagger-ui",
    utoipa::path(
        get,
        path = "/stores/{sid}/groups",
        tag = "management",
        params(("sid" = u64, Path, description = "Store id")),
        responses(
            (status = 200, description = "Groups in the store", body = Vec<GroupSummary>),
            (status = 404, description = "Store not found", body = ErrorResponse)
        )
    )
)]
async fn list_groups(State(state): State<RegistryArc>, Path(sid): Path<u64>) -> Result<Json<Vec<GroupSummary>>, (StatusCode, Json<ErrorResponse>)> {
    let store = state.get_store(sid).ok_or_else(|| err_json(StatusCode::NOT_FOUND, format!("store {sid} not found")))?;

    let groups: Vec<GroupSummary> = store
        .group_summaries()
        .into_iter()
        .map(|(group_id, local_replica_id, leader_id, remote_count)| GroupSummary {
            group_id,
            local_replica_id,
            leader_id,
            remote_count,
        })
        .collect();

    Ok(Json(groups))
}

#[cfg_attr(
    feature = "swagger-ui",
    utoipa::path(
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
    )
)]
async fn add_group(State(state): State<RegistryArc>, Path(sid): Path<u64>, Json(req): Json<AddGroupRequest>) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
    let store = state.get_store(sid).ok_or_else(|| err_json(StatusCode::NOT_FOUND, format!("store {sid} not found")))?;

    if store.get_group(req.group_id).is_some() {
        return Err(err_json(StatusCode::CONFLICT, format!("group {} already exists in store {sid}", req.group_id)));
    }

    info!(
        store_id = sid,
        group_id = req.group_id,
        replica_id = req.replica_id,
        "creating PxGroup with local replica via management API"
    );
    let local_replica = PxLocalReplica::new(req.replica_id, PxLocalReplicaRole::Follower);
    let group = PxGroup::new(req.group_id, local_replica);
    store.add_group(group);

    info!(store_id = sid, group_id = req.group_id, replica_id = req.replica_id, "PxGroup added via management API");
    Ok(StatusCode::CREATED)
}

#[cfg_attr(
    feature = "swagger-ui",
    utoipa::path(
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
    )
)]
async fn remove_group(State(state): State<RegistryArc>, Path((sid, gid)): Path<(u64, u64)>) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
    let store = state.get_store(sid).ok_or_else(|| err_json(StatusCode::NOT_FOUND, format!("store {sid} not found")))?;

    info!(store_id = sid, group_id = gid, "removing PxGroup via management API");
    if !store.remove_group(gid) {
        return Err(err_json(StatusCode::NOT_FOUND, format!("group {gid} not found in store {sid}")));
    }

    info!(store_id = sid, group_id = gid, "PxGroup removed via management API");
    Ok(StatusCode::OK)
}

#[cfg_attr(
    feature = "swagger-ui",
    utoipa::path(
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
    )
)]
async fn list_remotes(State(state): State<RegistryArc>, Path((sid, gid)): Path<(u64, u64)>) -> Result<Json<RemoteListResponse>, (StatusCode, Json<ErrorResponse>)> {
    let store = state.get_store(sid).ok_or_else(|| err_json(StatusCode::NOT_FOUND, format!("store {sid} not found")))?;
    let group = store
        .get_group(gid)
        .ok_or_else(|| err_json(StatusCode::NOT_FOUND, format!("group {gid} not found in store {sid}")))?;

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

#[cfg_attr(
    feature = "swagger-ui",
    utoipa::path(
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
    )
)]
async fn add_remotes(
    State(state): State<RegistryArc>,
    Path((sid, gid)): Path<(u64, u64)>,
    Json(remotes): Json<Vec<RemoteReplicaInfo>>,
) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
    let store = state.get_store(sid).ok_or_else(|| err_json(StatusCode::NOT_FOUND, format!("store {sid} not found")))?;
    let group = store
        .get_group(gid)
        .ok_or_else(|| err_json(StatusCode::NOT_FOUND, format!("group {gid} not found in store {sid}")))?;

    let local_id = group.local_replica().id;
    for r in &remotes {
        if r.replica_id == local_id {
            return Err(err_json(
                StatusCode::BAD_REQUEST,
                format!("cannot add local replica {} as remote; local replicas are managed with the group", r.replica_id),
            ));
        }
    }

    info!(store_id = sid, group_id = gid, count = remotes.len(), "adding remote replicas via management API");
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

    info!(store_id = sid, group_id = gid, count = remotes.len(), "remote replicas added via management API");
    Ok(StatusCode::OK)
}

#[cfg_attr(
    feature = "swagger-ui",
    utoipa::path(
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
    )
)]
async fn remove_remote(State(state): State<RegistryArc>, Path((sid, gid, rid)): Path<(u64, u64, u64)>) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
    let store = state.get_store(sid).ok_or_else(|| err_json(StatusCode::NOT_FOUND, format!("store {sid} not found")))?;
    let group = store
        .get_group(gid)
        .ok_or_else(|| err_json(StatusCode::NOT_FOUND, format!("group {gid} not found in store {sid}")))?;

    let local_id = group.local_replica().id;
    if rid == local_id {
        return Err(err_json(StatusCode::BAD_REQUEST, "cannot remove local replica; local replicas are managed with the group"));
    }

    // Check if remote exists
    let exists = group.remote_replica_info().iter().any(|(id, _)| *id == rid);
    if !exists {
        return Err(err_json(StatusCode::NOT_FOUND, format!("remote replica {rid} not found in group {gid}")));
    }

    info!(store_id = sid, group_id = gid, remote_id = rid, "removing remote replica via management API");
    // Reconstruct group without this remote
    let mut new_group = rebuild_group_with_same_config(&group);
    for (id, endpoint) in group.remote_replica_info() {
        if id != rid {
            new_group.add_remote_replica(PxRemoteReplica::new(id, endpoint.to_string()));
        }
    }
    store.add_group(new_group);

    info!(store_id = sid, group_id = gid, remote_id = rid, "remote replica removed via management API");
    Ok(StatusCode::OK)
}

#[cfg_attr(
    feature = "swagger-ui",
    utoipa::path(
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
    )
)]
async fn batch_add_remotes(
    State(state): State<RegistryArc>,
    Path((sid, gid)): Path<(u64, u64)>,
    Json(topology): Json<TopologyResponse>,
) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
    let store = state.get_store(sid).ok_or_else(|| err_json(StatusCode::NOT_FOUND, format!("store {sid} not found")))?;
    let group = store
        .get_group(gid)
        .ok_or_else(|| err_json(StatusCode::NOT_FOUND, format!("group {gid} not found in store {sid}")))?;

    let local_id = group.local_replica().id;
    let mut new_remotes = Vec::new();

    for topo_store in &topology.stores {
        let Some(addr) = topo_store.listen_addr else {
            continue;
        };
        for topo_group in &topo_store.groups {
            if topo_group.group_id == gid && topo_group.local_replica_id != local_id {
                new_remotes.push(RemoteReplicaInfo {
                    replica_id: topo_group.local_replica_id,
                    endpoint: addr.to_string(),
                });
            }
        }
    }

    if new_remotes.is_empty() {
        info!(store_id = sid, group_id = gid, "batch add remotes: no new remotes to add");
        return Ok(StatusCode::OK);
    }

    info!(store_id = sid, group_id = gid, count = new_remotes.len(), "batch adding remote replicas via management API");
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

    info!(store_id = sid, group_id = gid, count = new_remotes.len(), "batch remote replicas added via management API");
    Ok(StatusCode::OK)
}

/// `GET /topology` (alias `/top`) — full hierarchy with per-remote RPC
/// metrics and cheap kv-store stats.
#[cfg_attr(
    feature = "swagger-ui",
    utoipa::path(
        get,
        path = "/topology",
        tag = "management",
        responses((status = 200, description = "Cluster topology snapshot", body = TopologyResponse))
    )
)]
async fn export_topology(State(state): State<RegistryArc>) -> Json<TopologyResponse> {
    let stores: Vec<TopologyStore> = state
        .stores
        .iter()
        .map(|entry| {
            let snap = entry.value().snapshot();
            TopologyStore {
                store_id: snap.store_id,
                listen_addr: snap.listen_addr,
                groups: snap.groups.into_iter().map(group_to_topology).collect(),
            }
        })
        .collect();
    Json(TopologyResponse { stores })
}

fn group_to_topology(g: crowkv::cluster::GroupSnapshot) -> TopologyGroup {
    TopologyGroup {
        group_id: g.group_id,
        local_replica_id: g.local_replica.id,
        leader_id: g.leader_id,
        force_classic: g.force_classic,
        local_replica: TopologyLocalReplica {
            id: g.local_replica.id,
            role: g.local_replica.role.to_string(),
            voting: g.local_replica.voting,
            kv_store: TopologyKvStore {
                key_count: g.local_replica.kv_store.key_count,
            },
        },
        remotes: g
            .remotes
            .into_iter()
            .map(|r| TopologyRemote {
                id: r.id,
                endpoint: r.endpoint,
                voting: r.voting,
                metrics: TopologyMetrics {
                    rpc_count: r.metrics.rpc_count,
                    err_count: r.metrics.err_count,
                    last_rtt_ms: r.metrics.last_rtt_ms,
                },
            })
            .collect(),
    }
}

/// Rebuild a `PxGroup` with the same config (`group_id`, `local_replica`, `leader_id`, `force_classic`)
/// but no remote replicas. Caller is responsible for adding remotes.
fn rebuild_group_with_same_config(group: &PxGroup) -> PxGroup {
    let lr = group.local_replica();
    let local_replica = PxLocalReplica::new(lr.id, lr.role);
    let mut new_group = PxGroup::new(group.group_id, local_replica);
    new_group.set_leader_id(group.leader_id);
    if group.force_classic() {
        new_group.set_force_classic(true);
    }
    new_group
}
