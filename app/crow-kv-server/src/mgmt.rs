// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Management API module root — router, `OpenAPI` spec, shared helpers.
//!
//! Handler functions are split by subject into submodules:
//! - [`store_ops`] — store management endpoints
//! - [`group_ops`] — group management endpoints
//! - [`replica_ops`] — replica management endpoints
//! - [`system_init`] — system initialization + health-check endpoints
//! - [`topology`] — topology export + metrics endpoints

mod group_ops;
mod replica_ops;
mod store_ops;
mod system_init;
mod topology;

use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Serialize;
use utoipa::{OpenApi, ToSchema};

use crow_kv::cluster::group_config::GroupConfigStore;
use crow_kv::cluster::node_config::NodeConfigStore;
use crow_kv::cluster::status::{GroupStatus, RemoteStatus, ReplicaStatus, StoreStatus};
use crow_protocol::mgmt::{
    AddGroupRequest, AddStoreRequest, GroupSummary, HealthResponse, MetricField, MetricPoint,
    MetricsResponse, RemoteListResponse, RemoteReplicaInfo, StepDownRequest, StepDownResult, StoreDetail,
    StoreListResponse, StoreSummary, SystemInitRequest, SystemInitResponse, TopologyResponse,
};

use crate::operation_registry::OperationTarget;

pub(crate) type RegistryArc = crate::operation_registry::AppState;

// ── Server-local JSON types (no external caller) ────────────────

#[derive(ToSchema, Serialize)]
pub(super) struct ErrorResponse {
    error: String,
}

pub(super) fn err_json(
    status: axum::http::StatusCode,
    msg: impl Into<String>,
) -> (axum::http::StatusCode, Json<ErrorResponse>) {
    (status, Json(ErrorResponse { error: msg.into() }))
}

/// Resolve the persisted listen port for `store_id` from the on-disk
/// node config. Checks `node-config.json` first (the primary format),
/// then falls back to legacy `store{S}_group{G}.json` files for
/// migration. Returns `None` if no config exists or no valid endpoint
/// is found.
///
/// Priority: explicit port > persisted config port > OS-assigned (port 0).
pub async fn persisted_port_for_store(config_root: &std::path::Path, store_id: u64) -> Option<u16> {
    // Primary: node-config.json.
    let node_store = NodeConfigStore::new(config_root);
    if let Ok(config) = node_store.load().await {
        if let Some(store_entry) = config.store(store_id) {
            for group in &store_entry.groups {
                if let Some(port) = extract_port_from_members(&group.members) {
                    return Some(port);
                }
            }
        }
    }

    // Fallback: legacy per-group config files.
    let conf_dir = std::fs::read_dir(config_root).ok()?;
    for entry in conf_dir.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if let Some(rest) = name.strip_prefix(&format!("store{store_id}_group")) {
            if let Some(group_str) = rest.strip_suffix(".json") {
                if let Ok(group_id) = group_str.parse::<u64>() {
                    let store = GroupConfigStore::new(config_root, store_id, group_id);
                    if let Ok(Some(config)) = store.load().await {
                        if let Some(port) = extract_port_from_members(&config.members) {
                            return Some(port);
                        }
                    }
                }
            }
        }
    }
    None
}

/// Extract the first non-zero port from a list of group members.
fn extract_port_from_members(members: &[crow_kv::cluster::group_config::PxGroupMember]) -> Option<u16> {
    members
        .iter()
        .find_map(|m| m.endpoint.rsplit(':').next().and_then(|p| p.parse::<u16>().ok()))
        .filter(|&p| p > 0)
}

// ── Router ────────────────────────────────────────────────

pub fn router(state: RegistryArc) -> Router {
    Router::new()
        .route("/health", get(system_init::health_check))
        .route("/system/init", post(system_init::system_init))
        .route("/stores", get(store_ops::list_stores).post(store_ops::add_store))
        .route(
            "/stores/:sid",
            get(store_ops::get_store).delete(store_ops::remove_store),
        )
        .route(
            "/stores/:sid/groups",
            get(group_ops::list_groups).post(group_ops::add_group),
        )
        .route(
            "/stores/:sid/groups/:gid",
            axum::routing::delete(group_ops::remove_group),
        )
        .route(
            "/stores/:sid/groups/:gid/remotes",
            get(replica_ops::list_remote_replicas).post(replica_ops::add_remote_replicas),
        )
        .route(
            "/stores/:sid/groups/:gid/remotes/batch",
            post(replica_ops::batch_add_remote_replicas),
        )
        .route(
            "/stores/:sid/groups/:gid/remotes/:rid",
            axum::routing::delete(replica_ops::remove_remote_replica),
        )
        .route("/stores/:sid/groups/:gid/step-down", post(group_ops::step_down))
        .route(
            "/stores/:sid/groups/:gid/join",
            post(group_ops::join_group_via_snapshot),
        )
        .route("/stores/:sid/groups/:gid/flush", post(group_ops::flush_group))
        .route("/stores/:sid/groups/:gid/ready", get(group_ops::group_readiness))
        .route("/operations/:id", get(group_ops::get_operation))
        .route("/topology", get(topology::export_topology))
        .route("/top", get(topology::export_topology))
        .route("/metrics", get(topology::metrics))
        .route("/openapi.json", get(openapi_spec))
        .with_state(state)
}

// ── OpenAPI ────────────────────────────────────────────────

#[derive(OpenApi)]
#[openapi(
    paths(
        system_init::health_check,
        store_ops::list_stores,
        store_ops::get_store,
        store_ops::add_store,
        store_ops::remove_store,
        group_ops::list_groups,
        group_ops::add_group,
        group_ops::remove_group,
        group_ops::join_group_via_snapshot,
        group_ops::flush_group,
        group_ops::step_down,
        group_ops::group_readiness,
        group_ops::get_operation,
        replica_ops::list_remote_replicas,
        replica_ops::add_remote_replicas,
        replica_ops::remove_remote_replica,
        replica_ops::batch_add_remote_replicas,
        system_init::system_init,
        topology::export_topology,
        topology::metrics
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
            AddStoreRequest,
            AddGroupRequest,
            group_ops::JoinGroupRequest,
            RemoteReplicaInfo,
            RemoteListResponse,
            TopologyResponse,
            StepDownRequest,
            StepDownResult,
            group_ops::FlushResult,
            group_ops::ReadinessResponse,
            group_ops::OperationResponse,
            OperationTarget,
            group_ops::AsyncOperationResponse,
            ErrorResponse,
            MetricsResponse,
            MetricPoint,
            MetricField
        )
    ),
    tags((name = "management", description = "CrowKV management API"))
)]
pub(crate) struct ApiDoc;

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
