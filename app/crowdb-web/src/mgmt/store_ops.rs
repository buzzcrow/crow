// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! A5: Logical store plane — orchestrated store create/delete.

use crate::error::{err_400, err_409, err_500, err_502, ErrorBody};
use crate::expand::Recursive;
use crate::mgmt::{build_server_client, cluster_initialized, mgmt_url_for_node, refresh_node_cache};
use crate::state::AppState;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use crowdb_console_shared::clients::http::ServerClient;
use crowdb_console_shared::cluster::{GroupSummary, NodeId, StoreView};
use crowdb_console_shared::mgmt::AddStoreRequest;
use serde::Deserialize;
use std::collections::HashSet;

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

/// `POST /api/stores`. Create an empty store across the listed nodes (or the
/// first node with a running server if `nodes` is empty). Orchestrated:
/// fans out `add_store` to each node, rolls back on partial failure.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns an error if no nodes are available or any upstream RPC fails.
pub(crate) async fn http_add_store(
    State(state): State<AppState>,
    Json(body): Json<CreateStoreBody>,
) -> Result<(StatusCode, Json<serde_json::Value>), (StatusCode, Json<ErrorBody>)> {
    if body.store_id != 0 && !cluster_initialized(&state).await {
        return Err(err_409(
            "cluster not initialized; call POST /api/cluster/init first",
        ));
    }
    let mut target_nodes = if body.nodes.is_empty() {
        let cfg = state.config.read().unwrap();
        let first = cfg
            .servers
            .iter()
            .find_map(|s| s.node_id)
            .ok_or_else(|| err_400("no nodes with deployed servers"))?;
        vec![first]
    } else {
        body.nodes.clone()
    };

    let mut seen = HashSet::<NodeId>::new();
    target_nodes.retain(|node_id| seen.insert(*node_id));

    let mut reachable_targets: Vec<(NodeId, ServerClient)> = Vec::with_capacity(target_nodes.len());
    for nid in &target_nodes {
        let url = mgmt_url_for_node(&state, *nid)?;
        let client = build_server_client(url.clone())?;
        client.health().await.map_err(|e| {
            err_502(format!(
                "selected node {nid} is not currently reachable at {url}: {e}"
            ))
        })?;
        reachable_targets.push((*nid, client));
    }

    let mut succeeded: Vec<NodeId> = Vec::new();
    for (nid, client) in &reachable_targets {
        let req = AddStoreRequest {
            store_id: body.store_id,
            port: None,
        };
        match client.add_store(&req).await {
            Ok(_) => succeeded.push(*nid),
            Err(e) => {
                // Roll back successful creations.
                for ok_nid in &succeeded {
                    if let Ok(u) = mgmt_url_for_node(&state, *ok_nid) {
                        if let Ok(c) = build_server_client(u) {
                            let _ = c.remove_store(body.store_id).await;
                        }
                    }
                }
                return Err(err_502(format!("store create failed on node {nid}: {e}")));
            }
        }
    }

    // Refresh cache for all affected nodes.
    for nid in &succeeded {
        refresh_node_cache(&state, *nid).await;
    }

    {
        let mut cfg = state.config.write().unwrap();
        cfg.record_store(body.store_id, succeeded.clone());
    }
    state
        .persist()
        .map_err(|e| err_500(format!("persist config: {e}")))?;

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
/// node. Idempotent on per-node 404.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns `404` if the store is not found in the cache.
pub(crate) async fn http_remove_store(
    State(state): State<AppState>,
    Path(sid): Path<u64>,
) -> Result<StatusCode, (StatusCode, Json<ErrorBody>)> {
    if sid == 0 {
        return Err(err_409(
            "store 0 is the system store; use POST /api/cluster/reset to tear down the entire cluster",
        ));
    }
    let view = state.monitor_cache.resolve_store(sid).await.ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorBody {
                error: format!("store {sid} not found"),
            }),
        )
    })?;
    for nid in &view.nodes {
        if let Ok(url) = mgmt_url_for_node(&state, *nid) {
            if let Ok(client) = build_server_client(url) {
                let _ = client.remove_store(sid).await;
            }
        }
    }
    for nid in &view.nodes {
        refresh_node_cache(&state, *nid).await;
    }
    {
        let mut cfg = state.config.write().unwrap();
        cfg.remove_store_record(sid);
    }
    state
        .persist()
        .map_err(|e| err_500(format!("persist config: {e}")))?;
    Ok(StatusCode::NO_CONTENT)
}
