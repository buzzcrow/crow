// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! A5: Logical store plane — writes delegate to `ops::kv_logical`,
//! reads from the monitor cache (live role/leader info).

use crate::error::{err_502, map_config_err, map_persist_err, ErrorBody};
use crate::expand::Recursive;
use crate::mgmt::{cluster_initialized, refresh_node_cache};
use crate::state::AppState;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use crowdb_console_shared::cluster::{GroupSummary, NodeId, StoreView};
use crowdb_console_shared::ops;
use serde::Deserialize;

/// `GET /api/stores`. List stores aggregated from the monitor cache.
///
/// # Panics
/// Panics if the `RwLock` is poisoned (inside `snapshot()`).
pub(crate) async fn http_list_stores(
    State(state): State<AppState>,
    Recursive(_depth): Recursive,
) -> Json<Vec<StoreView>> {
    let snap = state.monitor_cache.snapshot().await;
    let mut seen: std::collections::BTreeMap<u64, StoreView> = std::collections::BTreeMap::new();
    for (node_id, rec) in &snap {
        for (sid, ns) in &rec.stores {
            let entry = seen.entry(*sid).or_insert_with(|| StoreView {
                store_id: *sid,
                name: None,
                nodes: Vec::new(),
                groups: Vec::new(),
            });
            entry.nodes.push(*node_id);
            for g in &ns.groups {
                if !entry.groups.iter().any(|gs| gs.group_id == g.group_id) {
                    entry.groups.push(GroupSummary {
                        group_id: g.group_id,
                        replica_count: 1,
                        leader: g.leader_hint,
                    });
                } else if let Some(gs) = entry.groups.iter_mut().find(|gs| gs.group_id == g.group_id) {
                    gs.replica_count += 1;
                    if gs.leader.is_none() {
                        gs.leader = g.leader_hint;
                    }
                }
            }
        }
    }
    for entry in seen.values_mut() {
        entry.groups.sort_by_key(|g| g.group_id);
    }
    Json(seen.into_values().collect())
}

#[derive(Debug, Deserialize)]
pub(crate) struct CreateStoreBody {
    pub store_id: u64,
    #[serde(default)]
    pub nodes: Vec<NodeId>,
}

/// `POST /api/stores`. Create an empty store across the listed nodes
/// (or the first node with a running server if `nodes` is empty).
/// Delegates to `ops::kv_logical::add_store` which handles fan-out +
/// rollback + sysdata recording.
///
/// # Errors
/// Returns `409` if the cluster is not initialized (non-zero store),
/// `502` if no nodes are available or any upstream RPC fails,
/// `500` if config persistence fails.
pub(crate) async fn http_add_store(
    State(state): State<AppState>,
    Json(body): Json<CreateStoreBody>,
) -> Result<(StatusCode, Json<serde_json::Value>), (StatusCode, Json<ErrorBody>)> {
    if body.store_id != 0 && !cluster_initialized(&state).await {
        return Err((
            StatusCode::CONFLICT,
            Json(ErrorBody {
                error: "cluster not initialized; call POST /api/cluster/init first".into(),
            }),
        ));
    }
    let ctx = state.op_context().await.map_err(|e| err_502(format!("{e}")))?;
    let succeeded = ops::kv_logical::add_store(&ctx, body.store_id, &body.nodes)
        .await
        .map_err(map_config_err)?;
    state.commit_op_context(&ctx).map_err(map_persist_err)?;

    // Refresh the monitor cache for affected nodes so health badges
    // and RPC endpoint resolution reflect the new store.
    futures::future::join_all(succeeded.iter().map(|&nid| refresh_node_cache(&state, nid))).await;

    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({ "store_id": body.store_id, "nodes": succeeded })),
    ))
}

/// `GET /api/stores/:store_id`. Aggregated store view from cache.
///
/// # Errors
/// Returns `404` if the store is not found.
pub(crate) async fn http_get_store(
    State(state): State<AppState>,
    Path(sid): Path<u64>,
    Recursive(_depth): Recursive,
) -> Result<Json<StoreView>, (StatusCode, Json<ErrorBody>)> {
    state
        .monitor_cache
        .resolve_store(sid)
        .await
        .map(Json)
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ErrorBody {
                    error: format!("store {sid} not found"),
                }),
            )
        })
}

/// `DELETE /api/stores/:store_id`. Delete the store across every hosting
/// node. Delegates to `ops::kv_logical::remove_store` which handles
/// fan-out + sysdata cleanup + config update.
///
/// # Errors
/// Returns `409` if `store_id` is 0 (the system store),
/// `500` if config persistence fails.
pub(crate) async fn http_remove_store(
    State(state): State<AppState>,
    Path(sid): Path<u64>,
) -> Result<StatusCode, (StatusCode, Json<ErrorBody>)> {
    if sid == 0 {
        return Err((
            StatusCode::CONFLICT,
            Json(ErrorBody {
                error:
                    "store 0 is the system store; use POST /api/cluster/reset to tear down the entire cluster"
                        .into(),
            }),
        ));
    }
    let ctx = state.op_context().await.map_err(|e| err_502(format!("{e}")))?;
    // Resolve hosting nodes from group-0 sysdata before removal
    // so we can refresh their caches afterwards.
    let hosting_nodes = ctx
        .sysmd()
        .get_store(sid)
        .await
        .map_err(|e| map_config_err(e.into()))?
        .map(|s| s.node_ids)
        .unwrap_or_default();
    ops::kv_logical::remove_store(&ctx, sid)
        .await
        .map_err(map_config_err)?;
    state.commit_op_context(&ctx).map_err(map_persist_err)?;

    futures::future::join_all(hosting_nodes.iter().map(|&nid| refresh_node_cache(&state, nid))).await;
    Ok(StatusCode::NO_CONTENT)
}
