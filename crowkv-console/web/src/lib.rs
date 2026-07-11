//! `CrowKV` Console web backend.
//!
//! Key work: two-tree API contract, physical tree lifecycle (A3),
//! per-node primitives (A4), logical store/group planes (A5/A6),
//! logical replica plane with bidirectional wiring + rollback (A7),
//! KV data plane with leader resolution via the monitor cache and
//! `NotLeader` retry (A8), Swagger UI (A9), React SPA shell.

pub mod corr_id;
pub mod error;
pub mod expand;
pub mod health;
pub mod kv;
pub mod lifecycle;
pub mod mgmt;
pub mod physical;
pub mod spa;
pub mod state;

pub use state::AppState;

/// Build the Axum router used by both the binary and integration tests.
pub fn router(state: AppState) -> axum::Router {
    use axum::routing::{delete, get, post};
    use state::SWAGGER_UI_DIR;

    axum::Router::new()
        .route("/healthz", get(health::healthz))
        // Swagger UI static bundle.
        //
        // Note: the legacy `/api/cluster/snapshot` aggregator was retired per
        // design-console.md §6.1. The SPA reads per-resource live endpoints
        // (`/api/racks`, `/api/nodes`, `/api/stores`, `/api/nodes/:id/stores`)
        // which are all served from the monitor cache (§4.2) and compose into
        // an equivalent topology view at lower latency.
        .nest_service("/api/swagger", tower_http::services::ServeDir::new(SWAGGER_UI_DIR))
        // ── Physical tree (A3): rack + node lifecycle ────────────────
        .route("/api/racks", get(lifecycle::http_list_racks).post(lifecycle::http_add_rack))
        .route("/api/racks/:rack_id", get(lifecycle::http_get_rack).delete(lifecycle::http_remove_rack))
        .route("/api/racks/:rack_id/nodes", get(lifecycle::http_list_rack_nodes).post(lifecycle::http_add_rack_node))
        .route("/api/nodes", get(lifecycle::http_list_nodes).post(lifecycle::http_add_node))
        .route("/api/nodes/:id", get(lifecycle::http_get_node).delete(lifecycle::http_remove_node))
        .route("/api/nodes/:id/ping", post(lifecycle::http_ping_node))
        .route("/api/nodes/:id/server", get(lifecycle::http_get_node_server).delete(lifecycle::http_delete_node_server))
        .route("/api/nodes/:id/server/deploy", post(lifecycle::http_deploy_node_server))
        .route("/api/nodes/:id/server/restart", post(lifecycle::http_restart_node_server))
        .route("/api/nodes/:id/server/stop", post(lifecycle::http_stop_node_server))
        .route("/api/nodes/:id/openapi.json", get(lifecycle::http_node_openapi_proxy))
        // ── Physical tree (A4): per-node store/group/remote primitives ─
        .route("/api/nodes/:id/stores", get(physical::http_list_node_stores).post(physical::http_add_node_store))
        .route("/api/nodes/:id/stores/:sid", get(physical::http_get_node_store).delete(physical::http_remove_node_store))
        .route(
            "/api/nodes/:id/stores/:sid/groups",
            get(physical::http_list_node_groups).post(physical::http_add_node_group),
        )
        .route(
            "/api/nodes/:id/stores/:sid/groups/:gid",
            get(physical::http_get_node_group).delete(physical::http_remove_node_group),
        )
        .route("/api/nodes/:id/stores/:sid/groups/:gid/remotes", post(physical::http_add_node_remote))
        .route("/api/nodes/:id/stores/:sid/groups/:gid/remotes/:rid", delete(physical::http_remove_node_remote))
        // ── Logical tree (A5/A6): store + group planes ──────────────
        .route("/api/stores", get(mgmt::http_list_stores).post(mgmt::http_add_store))
        .route("/api/stores/:sid", get(mgmt::http_get_store).delete(mgmt::http_remove_store))
        .route("/api/stores/:sid/groups", get(mgmt::http_list_groups).post(mgmt::http_add_group))
        .route("/api/stores/:sid/groups/:gid", get(mgmt::http_get_group).delete(mgmt::http_remove_group))
        // ── Logical tree (A7): replica plane ────────────────────────
        .route("/api/stores/:sid/groups/:gid/replicas", get(mgmt::http_list_replicas).post(mgmt::http_add_replica))
        .route("/api/stores/:sid/groups/:gid/replicas/:rid", get(mgmt::http_get_replica).delete(mgmt::http_remove_replica))
        // KV data plane: leader resolved via the monitor cache; NotLeader triggers one retry (A8).
        .route("/api/stores/:sid/groups/:gid/kv/get", get(kv::http_kv_get))
        .route("/api/stores/:sid/groups/:gid/kv/scan", get(kv::http_kv_scan))
        .route("/api/stores/:sid/groups/:gid/kv/put", post(kv::http_kv_put))
        .route("/api/stores/:sid/groups/:gid/kv/delete", post(kv::http_kv_delete))
        // React SPA fallback.
        .fallback(spa::spa_fallback)
        .with_state(state)
        // Propagate `x-crowkv-corr-id` through every request: read it
        // (or mint one), open a task-local scope so outbound clients
        // attach it to their own headers, echo it back on the response.
        .layer(axum::middleware::from_fn(corr_id::corr_id_layer))
}

#[cfg(test)]
mod tests {
    use super::{router, AppState};

    #[test]
    fn router_builds() {
        let _ = router(AppState::default());
    }
}
