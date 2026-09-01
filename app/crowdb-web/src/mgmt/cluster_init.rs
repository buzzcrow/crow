// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! R2: Cluster initialization — delegates to `ops::cluster::init`.

use crate::error::{err_502, map_config_err, map_persist_err, ErrorBody};
use crate::mgmt::refresh_node_cache;
use crate::state::AppState;
use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;
use crowdb_console_shared::ops;
use serde::Deserialize;

/// Request body for `POST /api/cluster/init`.
#[derive(Debug, Deserialize)]
pub(crate) struct ClusterInitBody {
    /// Node IDs to include in the system group (store 0, group 0).
    /// Must be non-empty. For a single node, group 0 self-elects.
    /// For multiple nodes, remotes are wired and election starts after.
    pub nodes: Vec<u64>,
}

/// `POST /api/cluster/init` — initialize the cluster by bootstrapping
/// the system group (store 0, group 0) on the selected nodes, wiring
/// remotes, and writing hardware + KV topology into group-0 sysdata.
///
/// Delegates to `ops::cluster::init` which handles the 5-phase
/// bootstrap (system/init, remote wiring, config update, topology
/// write). The handler retains web-specific concerns: monitor cache
/// refresh after init so health badges reflect the new state.
///
/// # Errors
/// Returns `502` if a node is unreachable or `system/init` fails,
/// `500` if config persistence fails.
pub(crate) async fn http_cluster_init(
    State(state): State<AppState>,
    Json(body): Json<ClusterInitBody>,
) -> Result<(StatusCode, Json<serde_json::Value>), (StatusCode, Json<ErrorBody>)> {
    let ctx = state.op_context().await.map_err(|e| err_502(format!("{e}")))?;
    let summary = ops::cluster::init(&ctx, &body.nodes)
        .await
        .map_err(map_config_err)?;
    state.commit_op_context(&ctx).map_err(map_persist_err)?;

    // The cluster is now live — re-seed the shared kv_client with the
    // current config's server URLs so topology refresh can find the
    // new group-0 leader. Without this, a client left over from before
    // reset has stale/empty seeds and every KV op retries for ~5s.
    state.reseed_kv_client().await;

    // Refresh the monitor cache for all init nodes so health badges
    // and RPC endpoint resolution reflect the new group-0 state.
    futures::future::join_all(
        summary
            .nodes
            .iter()
            .map(|&(node_id, _)| refresh_node_cache(&state, node_id)),
    )
    .await;

    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({
            "store_id": summary.store_id,
            "group_id": summary.group_id,
            "nodes": summary.nodes.iter().map(|(n, r)| serde_json::json!({
                "node_id": n,
                "replica_id": r,
            })).collect::<Vec<_>>(),
        })),
    ))
}
