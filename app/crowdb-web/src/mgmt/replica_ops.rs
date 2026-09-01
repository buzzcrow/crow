// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! A7: Logical replica plane — writes delegate to `ops::kv_logical`,
//! reads from the monitor cache (live role/leader info).

use crate::error::{err_502, map_config_err, map_persist_err, ErrorBody};
use crate::expand::Recursive;
use crate::mgmt::refresh_node_cache;
use crate::state::AppState;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use crowdb_console_shared::cluster::{NodeId, ReplicaView};
use crowdb_console_shared::ops;
use serde::Deserialize;

/// `GET /api/stores/:s/groups/:g/replicas`. Unified replica list from
/// the monitor cache.
///
/// # Errors
/// Returns `404` if the group is not found.
pub(crate) async fn http_list_replicas(
    State(state): State<AppState>,
    Path((sid, gid)): Path<(u64, u64)>,
    Recursive(_depth): Recursive,
) -> Result<Json<Vec<ReplicaView>>, (StatusCode, Json<ErrorBody>)> {
    let view = state.monitor_cache.resolve_group(sid, gid).await.ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorBody {
                error: format!("group {gid} in store {sid} not found"),
            }),
        )
    })?;
    Ok(Json(view.replicas))
}

/// `GET /api/stores/:s/groups/:g/replicas/:rid`. Single replica detail
/// (logical view) from the monitor cache.
///
/// # Errors
/// Returns `404` if the group or replica is not found.
pub(crate) async fn http_get_replica(
    State(state): State<AppState>,
    Path((sid, gid, rid)): Path<(u64, u64, u64)>,
    Recursive(_depth): Recursive,
) -> Result<Json<ReplicaView>, (StatusCode, Json<ErrorBody>)> {
    let view = state.monitor_cache.resolve_group(sid, gid).await.ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorBody {
                error: format!("group {gid} in store {sid} not found"),
            }),
        )
    })?;
    let replica = view
        .replicas
        .iter()
        .find(|r| r.replica_id == rid)
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ErrorBody {
                    error: format!("replica {rid} not found in group {gid}"),
                }),
            )
        })?;
    Ok(Json(replica.clone()))
}

#[derive(Debug, Deserialize)]
pub(crate) struct AddReplicaBody {
    pub node_id: NodeId,
    #[serde(default)]
    pub replica_id: Option<u64>,
}

/// `POST /api/stores/:s/groups/:g/replicas`. Add a replica to an
/// existing group. Delegates to `ops::kv_logical::add_replica` which
/// handles local group creation, bidirectional remote wiring, sysdata
/// recording, and rollback.
///
/// # Errors
/// Returns `404` if the group doesn't exist, `409` if the replica ID
/// is already in use, `502` if any upstream RPC fails,
/// `500` if config persistence fails.
pub(crate) async fn http_add_replica(
    State(state): State<AppState>,
    Path((sid, gid)): Path<(u64, u64)>,
    Json(body): Json<AddReplicaBody>,
) -> Result<(StatusCode, Json<serde_json::Value>), (StatusCode, Json<ErrorBody>)> {
    let ctx = state.op_context().await.map_err(|e| err_502(format!("{e}")))?;
    // Resolve existing replicas from group-0 sysdata for cache refresh
    // after the add. The ops function validates group existence.
    let existing_replicas: Vec<NodeId> = ctx
        .sysmd()
        .list_replicas_in_group(sid, gid)
        .await
        .map_err(|e| map_config_err(e.into()))?
        .iter()
        .map(|r| r.node_id)
        .collect();

    let new_rid = ops::kv_logical::add_replica(&ctx, sid, gid, body.node_id, body.replica_id)
        .await
        .map_err(map_config_err)?;
    state.commit_op_context(&ctx).map_err(map_persist_err)?;

    // Refresh the monitor cache for the target node + all peers so
    // health badges and RPC endpoint resolution reflect the new replica.
    refresh_node_cache(&state, body.node_id).await;
    for nid in &existing_replicas {
        if *nid != body.node_id {
            refresh_node_cache(&state, *nid).await;
        }
    }

    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({
            "store_id": sid,
            "group_id": gid,
            "replica_id": new_rid,
            "node_id": body.node_id,
        })),
    ))
}

/// `DELETE /api/stores/:s/groups/:g/replicas/:rid`. Remove a replica.
/// Delegates to `ops::kv_logical::remove_replica` which handles
/// step-down, peer deregistration, local group deletion, sysdata
/// cleanup, and config update.
///
/// # Errors
/// Returns `404` if the group or replica is not found,
/// `500` if config persistence fails.
pub(crate) async fn http_remove_replica(
    State(state): State<AppState>,
    Path((sid, gid, rid)): Path<(u64, u64, u64)>,
) -> Result<StatusCode, (StatusCode, Json<ErrorBody>)> {
    let ctx = state.op_context().await.map_err(|e| err_502(format!("{e}")))?;
    // Resolve the target + peers from group-0 sysdata before removal
    // so we can refresh their caches afterwards. The ops function
    // validates group/replica existence.
    let replicas = ctx
        .sysmd()
        .list_replicas_in_group(sid, gid)
        .await
        .map_err(|e| map_config_err(e.into()))?;
    let target_node = replicas.iter().find(|r| r.replica_id == rid).map(|r| r.node_id);
    let peers: Vec<NodeId> = replicas
        .iter()
        .filter(|r| r.replica_id != rid)
        .map(|r| r.node_id)
        .collect();

    ops::kv_logical::remove_replica(&ctx, sid, gid, rid)
        .await
        .map_err(map_config_err)?;
    state.commit_op_context(&ctx).map_err(map_persist_err)?;

    if let Some(target) = target_node {
        refresh_node_cache(&state, target).await;
    }
    for nid in &peers {
        refresh_node_cache(&state, *nid).await;
    }
    Ok(StatusCode::NO_CONTENT)
}
