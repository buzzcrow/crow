// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Logical tree: store and group planes (A5/A6).
//!
//! Key work: orchestrated store create/delete and group create/delete
//! built on top of the A4 per-node primitives. Reads aggregate from
//! the monitor cache; writes fan out to every listed node.

mod cluster_init;
mod group_ops;
mod replica_ops;
mod store_ops;
mod topology;

pub(crate) use cluster_init::http_cluster_init;
pub(crate) use group_ops::{http_add_group, http_get_group, http_list_groups, http_remove_group};
pub(crate) use replica_ops::{http_add_replica, http_get_replica, http_list_replicas, http_remove_replica};
pub(crate) use store_ops::{http_add_store, http_get_store, http_list_stores, http_remove_store};
pub(crate) use topology::restore_persisted_topology_for_node;
pub use topology::startup_topology_check;

use crate::error::{err_500, err_502, ErrorBody};
use crate::state::AppState;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use crow_console_shared::clients::http::ServerClient;
use crow_console_shared::cluster::{NodeHealth, NodeId};
use crow_console_shared::error::Error as SharedError;
use crow_console_shared::MetricsResponse;
use serde::Deserialize;
use std::collections::HashSet;
use std::time::Duration;
use tracing::warn;

// ── Shared helpers ───────────────────────────────────────────────────

pub(crate) fn mgmt_url_for_node(
    state: &AppState,
    node_id: NodeId,
) -> Result<String, (StatusCode, Json<ErrorBody>)> {
    let cfg = state.config.read().unwrap();
    let entry = cfg
        .server_for_node(node_id)
        .ok_or_else(|| err_502(format!("node {node_id} has no deployed server")))?;
    Ok(entry.url.clone())
}

pub(crate) fn build_server_client(url: String) -> Result<ServerClient, (StatusCode, Json<ErrorBody>)> {
    ServerClient::new(url).map_err(|e| err_500(format!("client build: {e}")))
}

/// Poll `mgmt_url`'s `/stores/{sid}` until `(sid, gid)` reports a leader
/// other than `excluded_leader_id` (the node being stepped down/removed),
/// or the timeout elapses. Best-effort: a `false` return does not block
/// the caller, it only means the leader-less window will close via lease
/// expiry instead of immediately.
pub(crate) async fn wait_for_new_leader(
    mgmt_url: &str,
    sid: u64,
    gid: u64,
    excluded_leader_id: u64,
    timeout: Duration,
) -> bool {
    let Ok(client) = ServerClient::new(mgmt_url.to_string()) else {
        return false;
    };
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        if let Ok(detail) = client.get_store(sid).await {
            if detail
                .groups
                .iter()
                .find(|g| g.group_id == gid)
                .is_some_and(|g| g.leader_id != 0 && g.leader_id != excluded_leader_id)
            {
                return true;
            }
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    false
}

/// Check whether the cluster is initialized (group 0 exists and is ready
/// on at least one reachable node). Returns `true` if the topology
/// cutover has been finalized, or if no nodes are deployed yet (first-run
/// scenario where the console itself drives init).
pub(crate) async fn cluster_initialized(state: &AppState) -> bool {
    let node_ids: Vec<NodeId> = {
        let cfg = state.config.read().unwrap();
        cfg.servers.iter().filter_map(|s| s.node_id).collect()
    };
    if node_ids.is_empty() {
        return true; // No servers deployed yet; allow first-run flows.
    }
    for nid in &node_ids {
        let Ok(url) = mgmt_url_for_node(state, *nid) else {
            continue;
        };
        let Ok(client) = build_server_client(url) else {
            continue;
        };
        if let Ok(stores) = client.list_stores().await {
            if stores.iter().any(|s| s.store_id == 0) {
                return true;
            }
        }
    }
    // If no node has group 0, but group 0 exists in the console
    // config, treat it as initialized (covers restart-before-finalize).
    let cfg = state.config.read().unwrap();
    cfg.group(0, 0).is_some()
}

pub(crate) async fn refresh_node_cache(state: &AppState, node_id: NodeId) {
    let url = {
        let cfg = state.config.read().unwrap();
        cfg.server_for_node(node_id).map(|s| s.url.clone())
    };
    if let Some(url) = url {
        if let Ok(client) = ServerClient::new(&url) {
            match client.topology().await {
                Ok(stores) => {
                    let rec = crow_console_shared::monitor::NodeRecord {
                        health: NodeHealth::Up,
                        last_seen_ms: 1,
                        stores: crow_console_shared::monitor::legacy_topology_to_node_stores(
                            node_id, &stores,
                        ),
                        last_error: None,
                    };
                    state.monitor_cache.set_node_report(node_id, rec).await;
                }
                Err(e) => {
                    state
                        .monitor_cache
                        .mark_down(node_id, format!("topology fetch failed: {e}"))
                        .await;
                }
            }
        } else {
            state
                .monitor_cache
                .mark_down(node_id, "server client construction failed")
                .await;
        }
    }
}

pub(crate) fn rpc_is_not_found(err: &SharedError) -> bool {
    matches!(err, SharedError::UpstreamRpc { status, .. } if status.contains("HTTP 404"))
}

pub(crate) fn rpc_is_conflict(err: &SharedError) -> bool {
    matches!(err, SharedError::UpstreamRpc { status, .. } if status.contains("HTTP 409"))
}

/// Return the bare `host:port` of the gRPC listener that hosts `store_id`
/// on `node_id`. Each `PxKvStore` on a `crow-kv-server` binds its own
/// random port, so the bootstrap `ServerEntry::grpc_url` only points at
/// the store created at process start (id 1). Operator-created stores
/// must be looked up via the monitor cache, which carries the actual
/// `listen_addr` reported by the server's `/topology` endpoint.
///
/// `0.0.0.0` listen addresses are remapped to `127.0.0.1` so other
/// processes on the same host can dial the channel.
pub(crate) async fn grpc_endpoint_for_node(
    state: &AppState,
    node_id: NodeId,
    store_id: u64,
) -> Option<String> {
    let snap = state.monitor_cache.snapshot().await;
    if let Some(rec) = snap.get(&node_id) {
        if let Some(addr) = rec.stores.get(&store_id).and_then(|s| s.listen_addr.clone()) {
            return Some(strip_scheme(remap_zero_host(&addr)));
        }
    }
    warn!(
        node_id,
        store_id, "grpc_endpoint_for_node: cache miss, no known endpoint"
    );
    None
}

fn strip_scheme(s: String) -> String {
    if let Some(stripped) = s.strip_prefix("http://").or_else(|| s.strip_prefix("https://")) {
        stripped.to_string()
    } else {
        s
    }
}

/// Parse `port` out of a URL like `http://host:9910` or `host:9910`.
/// Returns `None` on any shape we don't recognise.
#[must_use]
pub(crate) fn port_of(url: &str) -> Option<u16> {
    let stripped = url
        .strip_prefix("http://")
        .or_else(|| url.strip_prefix("https://"))
        .unwrap_or(url);
    let host_port = stripped.split('/').next().unwrap_or(stripped);
    let port_str = host_port.rsplit(':').next()?;
    port_str.parse::<u16>().ok()
}

fn remap_zero_host(addr: &str) -> String {
    addr.strip_prefix("0.0.0.0:")
        .map_or_else(|| addr.to_string(), |port| format!("127.0.0.1:{port}"))
}

/// Build a [`HardwareClient`] pinned to group 0 by finding any node in the
/// monitor cache that hosts store 0's gRPC listener. Returns `None` when
/// no group-0 endpoint is known (e.g. cluster not yet initialized).
pub(crate) async fn build_hardware_client(state: &AppState) -> Option<crow_kv_client::HardwareClient> {
    let snap = state.monitor_cache.snapshot().await;
    if snap.is_empty() {
        // First-run scenario — no nodes deployed yet. This is normal,
        // not a warning-worthy condition. Callers fall back to config.
        return None;
    }
    for node_id in snap.keys() {
        if let Some(ep) = grpc_endpoint_for_node(state, *node_id, 0).await {
            let kv = crow_kv_client::CrowkvClient::new(crow_kv_client::ClientConfig::new(Vec::new()));
            kv.seed_leader(0, 0, ep);
            return Some(crow_kv_client::HardwareClient::new(kv));
        }
    }
    warn!("build_hardware_client: nodes exist but no group-0 endpoint found in monitor cache");
    None
}

/// Cheap check: is group-0 (store 0) known to the monitor cache?
/// Use this to gate group-0 sysdata reads without logging warnings
/// on every poll when the cluster isn't initialized yet.
pub(crate) async fn group0_available(state: &AppState) -> bool {
    state.monitor_cache.resolve_store(0).await.is_some()
}

// ── Metrics proxy (R11) ───────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub(crate) struct MetricsQuery {
    /// Metric name prefix filter (e.g. `s.1.g.2.`). Default empty = all.
    #[serde(default)]
    prefix: Option<String>,
}

impl MetricsQuery {
    fn prefix(&self) -> &str {
        self.prefix.as_deref().unwrap_or("")
    }
}

/// `GET /api/nodes/:id/metrics` — proxy to the node's `GET /metrics`.
///
/// # Errors
/// Returns `502` if the node has no deployed server or the upstream
/// `/metrics` fetch fails.
pub(crate) async fn http_node_metrics(
    State(state): State<AppState>,
    Path(node_id): Path<u64>,
    Query(q): Query<MetricsQuery>,
) -> Result<Json<MetricsResponse>, (StatusCode, Json<ErrorBody>)> {
    let url = mgmt_url_for_node(&state, node_id)?;
    let client = build_server_client(url)?;
    let resp = client
        .metrics(q.prefix())
        .await
        .map_err(|e| err_502(format!("metrics fetch from node {node_id}: {e}")))?;
    Ok(Json(resp))
}

/// `GET /api/stores/:sid/groups/:gid/metrics` — proxy to the leader
/// node's `GET /metrics` with prefix `s.{sid}.g.{gid}.`.
///
/// # Errors
/// Returns `404` if the group has no healthy leader; `502` if the
/// upstream `/metrics` fetch fails.
pub(crate) async fn http_group_metrics(
    State(state): State<AppState>,
    Path((sid, gid)): Path<(u64, u64)>,
    Query(q): Query<MetricsQuery>,
) -> Result<Json<MetricsResponse>, (StatusCode, Json<ErrorBody>)> {
    let (_rid, node_id) = state.monitor_cache.leader_for(sid, gid).await.ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorBody {
                error: format!("group {gid} in store {sid} has no healthy leader"),
            }),
        )
    })?;
    let url = mgmt_url_for_node(&state, node_id)?;
    let client = build_server_client(url)?;
    let prefix = if q.prefix().is_empty() {
        format!("s.{sid}.g.{gid}.")
    } else {
        format!("s.{sid}.g.{gid}.{}", q.prefix())
    };
    let resp = client
        .metrics(&prefix)
        .await
        .map_err(|e| err_502(format!("metrics fetch from leader {node_id}: {e}")))?;
    Ok(Json(resp))
}

/// `GET /api/stores/:sid/metrics` — aggregate metrics across all groups
/// in the store. Fetches from each group's leader node with prefix
/// `s.{sid}.` and merges the results.
///
/// # Errors
/// Returns `404` if the store has no groups. Individual node fetch
/// failures are silently skipped (partial results returned).
pub(crate) async fn http_store_metrics(
    State(state): State<AppState>,
    Path(sid): Path<u64>,
    Query(q): Query<MetricsQuery>,
) -> Result<Json<MetricsResponse>, (StatusCode, Json<ErrorBody>)> {
    let group_ids: Vec<u64> = {
        let snap = state.monitor_cache.snapshot().await;
        snap.values()
            .filter_map(|rec| {
                rec.stores
                    .get(&sid)
                    .map(|ns| ns.groups.iter().map(|g| g.group_id).collect::<Vec<_>>())
            })
            .flatten()
            .collect()
    };
    if group_ids.is_empty() {
        return Err((
            StatusCode::NOT_FOUND,
            Json(ErrorBody {
                error: format!("store {sid} not found or has no groups"),
            }),
        ));
    }

    let store_prefix = if q.prefix().is_empty() {
        format!("s.{sid}.")
    } else {
        format!("s.{sid}.{}", q.prefix())
    };

    let mut seen_nodes: HashSet<NodeId> = HashSet::new();
    let mut merged: Vec<crow_console_shared::MetricPointView> = Vec::new();
    let mut window_secs = 5.0_f64;
    let mut timestamp = String::new();

    for gid in &group_ids {
        let Some((_rid, node_id)) = state.monitor_cache.leader_for(sid, *gid).await else {
            continue;
        };
        if !seen_nodes.insert(node_id) {
            continue;
        }
        let Ok(url) = mgmt_url_for_node(&state, node_id) else {
            continue;
        };
        let Ok(client) = build_server_client(url) else {
            continue;
        };
        if let Ok(resp) = client.metrics(&store_prefix).await {
            window_secs = resp.window_secs;
            if timestamp.is_empty() {
                timestamp = resp.timestamp;
            }
            merged.extend(resp.metrics);
        }
    }

    Ok(Json(MetricsResponse {
        window_secs,
        timestamp,
        metrics: merged,
    }))
}
