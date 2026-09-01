// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! A6: Logical group plane — writes delegate to `ops::kv_logical`,
//! reads from the monitor cache (live role/leader info).

use crate::error::{err_502, map_config_err, map_persist_err, ErrorBody};
use crate::expand::Recursive;
use crate::mgmt::{cluster_initialized, refresh_node_cache};
use crate::state::AppState;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use crowdb_console_shared::cluster::{GroupSummary, GroupView, NodeId};
use crowdb_console_shared::ops;
use serde::Deserialize;

/// `GET /api/stores/:store_id/groups`. List groups from cache.
///
/// # Errors
/// Returns `404` if the store is not found.
pub(crate) async fn http_list_groups(
    State(state): State<AppState>,
    Path(sid): Path<u64>,
    Recursive(_depth): Recursive,
) -> Result<Json<Vec<GroupSummary>>, (StatusCode, Json<ErrorBody>)> {
    let view = state.monitor_cache.resolve_store(sid).await.ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorBody {
                error: format!("store {sid} not found"),
            }),
        )
    })?;
    Ok(Json(view.groups))
}

#[derive(Debug, Deserialize)]
pub(crate) struct CreateGroupBody {
    pub group_id: u64,
    pub replica_id: u64,
    pub nodes: Vec<NodeId>,
}

/// `POST /api/stores/:store_id/groups`. Create a group across the listed
/// nodes. Delegates to `ops::kv_logical::add_group` which handles
/// local group creation, remote wiring, sysdata recording, and rollback.
///
/// # Errors
/// Returns `409` if the cluster is not initialized (non-zero store),
/// `502` if any upstream RPC fails, `500` if config persistence fails.
pub(crate) async fn http_add_group(
    State(state): State<AppState>,
    Path(sid): Path<u64>,
    Json(body): Json<CreateGroupBody>,
) -> Result<(StatusCode, Json<serde_json::Value>), (StatusCode, Json<ErrorBody>)> {
    if sid != 0 && !cluster_initialized(&state).await {
        return Err((
            StatusCode::CONFLICT,
            Json(ErrorBody {
                error: "cluster not initialized; call POST /api/cluster/init first".into(),
            }),
        ));
    }
    let ctx = state.op_context().await.map_err(|e| err_502(format!("{e}")))?;
    ops::kv_logical::add_group(&ctx, sid, body.group_id, body.replica_id, &body.nodes)
        .await
        .map_err(map_config_err)?;
    state.commit_op_context(&ctx).map_err(map_persist_err)?;

    // Refresh the monitor cache for all target nodes so health badges
    // and RPC endpoint resolution reflect the new group.
    for nid in &body.nodes {
        refresh_node_cache(&state, *nid).await;
    }

    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({
            "store_id": sid,
            "group_id": body.group_id,
            "nodes": body.nodes,
        })),
    ))
}

/// `GET /api/stores/:store_id/groups/:group_id`. Aggregated group view
/// from cache. Refreshes all nodes hosting the store first so role /
/// leader info reflects the most recent election state.
///
/// # Errors
/// Returns `404` if the group is not found.
pub(crate) async fn http_get_group(
    State(state): State<AppState>,
    Path((sid, gid)): Path<(u64, u64)>,
    Recursive(_depth): Recursive,
) -> Result<Json<GroupView>, (StatusCode, Json<ErrorBody>)> {
    // Refresh the cache for every node currently believed to host this
    // store so role / leader info reflects the most recent topology.
    let node_ids: Vec<NodeId> = {
        let snap = state.monitor_cache.snapshot().await;
        snap.iter()
            .filter_map(|(nid, rec)| {
                if rec.stores.contains_key(&sid) {
                    Some(*nid)
                } else {
                    None
                }
            })
            .collect()
    };
    for nid in &node_ids {
        refresh_node_cache(&state, *nid).await;
    }
    state
        .monitor_cache
        .resolve_group(sid, gid)
        .await
        .map(Json)
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ErrorBody {
                    error: format!("group {gid} in store {sid} not found"),
                }),
            )
        })
}

/// `DELETE /api/stores/:store_id/groups/:group_id`. Delete the group
/// across every hosting node. Delegates to `ops::kv_logical::remove_group`
/// which handles fan-out + sysdata cleanup + config update.
///
/// # Errors
/// Returns `409` if removing group 0 in store 0,
/// `500` if config persistence fails.
pub(crate) async fn http_remove_group(
    State(state): State<AppState>,
    Path((sid, gid)): Path<(u64, u64)>,
) -> Result<StatusCode, (StatusCode, Json<ErrorBody>)> {
    if sid == 0 && gid == 0 {
        return Err((
            StatusCode::CONFLICT,
            Json(ErrorBody {
                error: "group 0 in store 0 is the system group; use POST /api/cluster/reset to tear down the entire cluster".into(),
            }),
        ));
    }
    let ctx = state.op_context().await.map_err(|e| err_502(format!("{e}")))?;
    // Resolve hosting nodes from group-0 sysdata before removal
    // so we can refresh their caches afterwards.
    let hosting_nodes: Vec<NodeId> = ctx
        .sysmd()
        .list_replicas_in_group(sid, gid)
        .await
        .map_err(|e| map_config_err(e.into()))?
        .iter()
        .map(|r| r.node_id)
        .collect();
    ops::kv_logical::remove_group(&ctx, sid, gid)
        .await
        .map_err(map_config_err)?;
    state.commit_op_context(&ctx).map_err(map_persist_err)?;

    for nid in &hosting_nodes {
        refresh_node_cache(&state, *nid).await;
    }
    Ok(StatusCode::NO_CONTENT)
}
