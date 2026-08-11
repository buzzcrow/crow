// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Logical tree: store and group planes (A5/A6).
//!
//! Key work: orchestrated store create/delete and group create/delete
//! built on top of the A4 per-node primitives. Reads aggregate from
//! the monitor cache; writes fan out to every listed node.

use crate::error::{err_400, err_409, err_500, err_502, ErrorBody};
use crate::state::AppState;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use crow_console_shared::clients::http::ServerClient;
use crow_console_shared::cluster::{GroupSummary, GroupView, ReplicaView, StoreView};
use crow_console_shared::config::{GroupEntry, NodeEntry, ReplicaEntry, ServerEntry, StoreEntry};
use crow_console_shared::error::Error as SharedError;
use crow_console_shared::lifecycle::{self, DeployRequest};
use crow_console_shared::mgmt::{
    AddGroupInitialRole, AddGroupRequest, AddStoreRequest, RemoteReplicaInfo, StepDownRequest,
    TopologyFinalizeRequest, TopologyGroupInput, TopologyNodeInput, TopologyRackInput, TopologyReplicaInput,
    TopologyStoreInput,
};
use crow_console_shared::MetricsResponse;
use serde::Deserialize;
use std::collections::HashSet;
use std::time::Duration;

use crate::expand::Recursive;
use tracing::{info, warn};

// ── Helpers ───────────────────────────────────────────────────────────

pub(crate) fn mgmt_url_for_node(
    state: &AppState,
    node_id: &str,
) -> Result<String, (StatusCode, Json<ErrorBody>)> {
    let cfg = state.config.read().unwrap();
    let entry = cfg
        .server_for_node(node_id.parse().unwrap())
        .ok_or_else(|| err_502(format!("node {node_id} has no deployed server")))?;
    Ok(entry.url.clone())
}

pub(crate) fn build_server_client(url: String) -> Result<ServerClient, (StatusCode, Json<ErrorBody>)> {
    ServerClient::new(url).map_err(|e| err_500(format!("client build: {e}")))
}

/// Poll `mgmt_url`'s `/stores/{sid}` until `(sid, gid)` reports a leader
/// other than `excluded_leader_id` (the node being stepped down/removed),
/// or the timeout elapses. Deliberately stricter than
/// `lifecycle::wait_for_leader`, which only checks for a *non-zero*
/// leader -- a survivor can still be reporting the stale, just-stepped-
/// down leader until its own election timeout fires. Best-effort: a
/// `false` return does not block the caller, it only means the leader-
/// less window will close via lease expiry instead of immediately.
async fn wait_for_new_leader(
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
async fn cluster_initialized(state: &AppState) -> bool {
    let node_ids: Vec<String> = {
        let cfg = state.config.read().unwrap();
        cfg.servers
            .iter()
            .filter_map(|s| s.node_id.map(|id| id.to_string()))
            .collect()
    };
    if node_ids.is_empty() {
        return true; // No servers deployed yet; allow first-run flows.
    }
    for nid in &node_ids {
        let Ok(url) = mgmt_url_for_node(state, nid) else {
            continue;
        };
        let Ok(client) = build_server_client(url) else {
            continue;
        };
        if let Ok(resp) = client.topology_ready().await {
            if resp.ready {
                return true;
            }
        }
    }
    // If no node has group 0 ready, but group 0 exists in the console
    // config, treat it as initialized (covers restart-before-finalize).
    let cfg = state.config.read().unwrap();
    cfg.group(0, 0).is_some()
}

pub(crate) async fn refresh_node_cache(state: &AppState, node_id: &str) {
    let url = {
        let cfg = state.config.read().unwrap();
        cfg.server_for_node(node_id.parse().unwrap())
            .map(|s| s.url.clone())
    };
    if let Some(url) = url {
        if let Ok(client) = ServerClient::new(url) {
            match client.topology().await {
                Ok(stores) => {
                    let rec = crow_console_shared::monitor::NodeRecord {
                        health: crow_console_shared::cluster::NodeHealth::Up,
                        last_seen_ms: 1,
                        stores: crow_console_shared::monitor::legacy_topology_to_node_stores(
                            node_id, &stores,
                        ),
                        last_error: None,
                    };
                    state
                        .monitor_cache
                        .set_node_report(node_id.to_string(), rec)
                        .await;
                }
                Err(e) => {
                    // Node is unreachable — mark it down so leader_for
                    // skips its stale leader record instead of routing
                    // KV traffic to a dead endpoint.
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

fn rpc_is_not_found(err: &SharedError) -> bool {
    matches!(err, SharedError::UpstreamRpc { status, .. } if status.contains("HTTP 404"))
}

fn rpc_is_conflict(err: &SharedError) -> bool {
    matches!(err, SharedError::UpstreamRpc { status, .. } if status.contains("HTTP 409"))
}

async fn ensure_server_running(
    state: &AppState,
    node: &NodeEntry,
    server: &ServerEntry,
) -> Result<(), String> {
    let client = ServerClient::new(server.url.clone()).map_err(|e| e.to_string())?;
    if client.health().await.is_ok() {
        refresh_node_cache(state, &node.id.to_string()).await;
        return Ok(());
    }
    if !server.auto_start {
        return Ok(());
    }
    let mgmt_port = server
        .mgmt_port
        .ok_or_else(|| format!("server {} missing persisted mgmt_port", server.id))?;
    let grpc_port = server
        .grpc_port
        .ok_or_else(|| format!("server {} missing persisted grpc_port", server.id))?;
    let req = DeployRequest {
        server_id: server.id.clone(),
        mgmt_port,
        grpc_port,
        election_profile: server.election_profile.clone(),
        binary: server.binary.clone().map(std::path::PathBuf::from),
        ..Default::default()
    };
    let deployed = if node.ssh_enabled() {
        let server_bin = server.binary.clone().unwrap_or_else(|| {
            std::env::var("CROW_KV_SERVER_BIN").unwrap_or_else(|_| "crow-kv-server".to_string())
        });
        crow_console_shared::ssh::deploy_via_ssh(&req, node, &server_bin)
            .await
            .map_err(|e| e.to_string())?
    } else {
        let workspace_dir = state
            .prepare_node_workspace(node.id.to_string())
            .map_err(|e| e.to_string())?;
        lifecycle::deploy_local_in_dir(&req, node, &workspace_dir)
            .await
            .map_err(|e| e.to_string())?
    };
    state.set_runtime_pid(node.id.to_string(), deployed.pid);
    refresh_node_cache(state, &node.id.to_string()).await;
    Ok(())
}

async fn ensure_store_on_node(state: &AppState, node_id: &str, store_id: u64) -> Result<(), String> {
    let url = mgmt_url_for_node(state, node_id).map_err(|(_, body)| body.0.error.clone())?;
    let client = ServerClient::new(url).map_err(|e| e.to_string())?;
    match client.get_store(store_id).await {
        Ok(_) => Ok(()),
        Err(err) if rpc_is_not_found(&err) => {
            client
                .add_store(&AddStoreRequest { store_id, port: None })
                .await
                .map_err(|e| e.to_string())?;
            refresh_node_cache(state, node_id).await;
            Ok(())
        }
        Err(err) => Err(err.to_string()),
    }
}

async fn ensure_group_local(
    state: &AppState,
    node_id: &str,
    store_id: u64,
    group_id: u64,
    replica_id: u64,
    initial_role: AddGroupInitialRole,
    // `Some(false)` for multi-replica groups so the server does not self-elect
    // at `quorum == 1` before remotes are wired; the
    // following `ensure_group_remotes` rebuild starts the driver with a correct
    // quorum. `None`/`Some(true)` for single-replica groups (no remote-wiring
    // step to start the driver).
    start_election: Option<bool>,
) -> Result<(), String> {
    let url = mgmt_url_for_node(state, node_id).map_err(|(_, body)| body.0.error.clone())?;
    let client = ServerClient::new(url).map_err(|e| e.to_string())?;
    match client.list_groups(store_id).await {
        Ok(groups)
            if groups
                .iter()
                .any(|g| g.group_id == group_id && g.local_replica_id == replica_id) =>
        {
            Ok(())
        }
        Ok(_) => {
            client
                .add_group(
                    store_id,
                    &AddGroupRequest {
                        group_id,
                        replica_id,
                        initial_role: Some(initial_role),
                        start_election,
                    },
                )
                .await
                .map_err(|e| e.to_string())?;
            refresh_node_cache(state, node_id).await;
            Ok(())
        }
        Err(err) if rpc_is_not_found(&err) => {
            ensure_store_on_node(state, node_id, store_id).await?;
            client
                .add_group(
                    store_id,
                    &AddGroupRequest {
                        group_id,
                        replica_id,
                        initial_role: Some(initial_role),
                        start_election,
                    },
                )
                .await
                .map_err(|e| e.to_string())?;
            refresh_node_cache(state, node_id).await;
            Ok(())
        }
        Err(err) if rpc_is_conflict(&err) => Ok(()),
        Err(err) => Err(err.to_string()),
    }
}

async fn ensure_group_remotes(state: &AppState, group: &GroupEntry) -> Result<(), String> {
    for replica in &group.replicas {
        refresh_node_cache(state, &replica.node_id.to_string()).await;
    }
    for replica in &group.replicas {
        let url = mgmt_url_for_node(state, &replica.node_id.to_string())
            .map_err(|(_, body)| body.0.error.clone())?;
        let client = ServerClient::new(url).map_err(|e| e.to_string())?;
        let existing = client
            .list_remote_replicas(group.store_id, group.group_id)
            .await
            .map_err(|e| e.to_string())?;
        let mut to_update = Vec::new();
        for peer in &group.replicas {
            if peer.replica_id == replica.replica_id {
                continue;
            }
            let Some(current_endpoint) =
                grpc_endpoint_for_node(state, &peer.node_id.to_string(), group.store_id).await
            else {
                // Peer's store is not up yet; skip rather than overwriting
                // the correct persisted-config endpoint with a stale one.
                continue;
            };
            let existing_entry = existing.iter().find(|r| r.replica_id == peer.replica_id);
            let needs_update = match existing_entry {
                None => true,
                Some(r) => r.endpoint != current_endpoint,
            };
            if needs_update {
                to_update.push(RemoteReplicaInfo {
                    replica_id: peer.replica_id,
                    endpoint: current_endpoint,
                });
            }
        }
        if !to_update.is_empty() {
            client
                .add_remote_replicas(group.store_id, group.group_id, &to_update)
                .await
                .map_err(|e| e.to_string())?;
            refresh_node_cache(state, &replica.node_id.to_string()).await;
        }
    }
    Ok(())
}

/// Result of the three-way group 0 state check at console startup.
enum Group0State {
    /// No nodes deployed yet — first-run scenario.
    NoNodes,
    /// Group 0 not found on any reachable node — phase 1 (TOML mode).
    Missing,
    /// Group 0 exists but `/topology/ready` not set — TOML mode with warning.
    NotReady,
    /// Group 0 exists and `/topology/ready` is set — group 0 authoritative.
    Ready,
}

/// Check group 0 state across all deployed nodes to determine the
/// topology source at console startup.
async fn check_group0_state(state: &AppState) -> Group0State {
    let node_ids: Vec<String> = {
        let cfg = state.config.read().unwrap();
        cfg.servers
            .iter()
            .filter_map(|s| s.node_id.map(|id| id.to_string()))
            .collect()
    };
    if node_ids.is_empty() {
        return Group0State::NoNodes;
    }
    let mut found_group0 = false;
    for nid in &node_ids {
        let Ok(url) = mgmt_url_for_node(state, nid) else {
            continue;
        };
        let Ok(client) = build_server_client(url) else {
            continue;
        };
        // Check if group 0 exists by listing stores.
        if let Ok(stores) = client.list_stores().await {
            if stores.iter().any(|s| s.store_id == 0) {
                found_group0 = true;
                if let Ok(resp) = client.topology_ready().await {
                    if resp.ready {
                        return Group0State::Ready;
                    }
                }
            }
        }
    }
    if found_group0 {
        Group0State::NotReady
    } else {
        Group0State::Missing
    }
}

/// Console startup three-way fallback. Checks group 0 state and picks
/// the right topology source before restoring.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
pub async fn startup_topology_check(state: &AppState) {
    match check_group0_state(state).await {
        Group0State::NoNodes => {
            info!("no nodes deployed; first-run scenario, skipping topology restore");
        }
        Group0State::Missing => {
            info!("group 0 not found on any node; TOML mode (phase 1)");
            restore_persisted_topology(state).await;
        }
        Group0State::NotReady => {
            warn!("group 0 exists but not finalized; using TOML mode with warning");
            restore_persisted_topology(state).await;
        }
        Group0State::Ready => {
            info!("group 0 is ready; loading topology from group 0 KV");
            // In phase 2, topology is authoritative from group 0.
            // For now, still restore from TOML (the cutover to reading
            // group 0 KV into console config is T7's reconciliation).
            // The key difference: we know the cluster is initialized,
            // so data store/group creation is unblocked.
            restore_persisted_topology(state).await;
        }
    }
}

/// Restore persisted topology (servers, stores, groups, replicas) on startup.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
pub async fn restore_persisted_topology(state: &AppState) {
    let (nodes, servers, stores, groups) = {
        let cfg = state.config.read().unwrap();
        (
            cfg.nodes.clone(),
            cfg.servers.clone(),
            cfg.stores.clone(),
            cfg.groups.clone(),
        )
    };
    for server in &servers {
        let Some(node_id) = server.node_id else {
            continue;
        };
        let Some(node) = nodes.iter().find(|n| n.id == node_id) else {
            warn!(
                server_id = server.id,
                node_id, "skipping restore for server with missing node"
            );
            continue;
        };
        if let Err(err) = ensure_server_running(state, node, server).await {
            warn!(server_id = server.id, node_id, error = %err, "failed to restore server process");
        }
    }
    for StoreEntry { store_id, nodes } in &stores {
        for node_id in nodes {
            if let Err(err) = ensure_store_on_node(state, &node_id.to_string(), *store_id).await {
                warn!(store_id, node_id, error = %err, "failed to restore store");
            }
        }
    }
    for group in &groups {
        let mut replicas = group.replicas.clone();
        replicas.sort_by_key(|r| r.replica_id);
        // Defer the election driver for multi-replica groups until remotes are
        // wired.
        let start_election = Some(replicas.len() <= 1);
        for (index, replica) in replicas.iter().enumerate() {
            let initial_role = if index == 0 {
                AddGroupInitialRole::Leader
            } else {
                AddGroupInitialRole::Follower
            };
            if let Err(err) = ensure_group_local(
                state,
                &replica.node_id.to_string(),
                group.store_id,
                group.group_id,
                replica.replica_id,
                initial_role,
                start_election,
            )
            .await
            {
                warn!(
                    store_id = group.store_id,
                    group_id = group.group_id,
                    replica_id = replica.replica_id,
                    node_id = replica.node_id,
                    error = %err,
                    "failed to restore local group replica"
                );
            }
        }
        if let Err(err) = ensure_group_remotes(state, group).await {
            warn!(store_id = group.store_id, group_id = group.group_id, error = %err, "failed to restore group remotes");
        }
    }
    for server in &servers {
        if let Some(node_id) = server.node_id {
            refresh_node_cache(state, &node_id.to_string()).await;
        }
    }
    info!(
        servers = servers.len(),
        stores = stores.len(),
        groups = groups.len(),
        "restore reconcile finished"
    );
}

/// Restores persisted topology (stores and groups) for a specific node.
///
/// This function ensures that all stores and groups configured for the given node
/// are properly set up on the node after a restart.
///
/// # Panics
/// Panics if the config read lock is poisoned (should not happen in normal operation).
///
/// # Errors
/// Returns an error if store or group restoration fails.
pub async fn restore_persisted_topology_for_node(state: &AppState, node_id: &str) -> Result<(), String> {
    let nid: u64 = node_id.parse().unwrap();
    let (stores, groups) = {
        let cfg = state.config.read().unwrap();
        (cfg.stores.clone(), cfg.groups.clone())
    };

    for store in stores
        .iter()
        .filter(|store| store.nodes.iter().any(|id| id == &nid))
    {
        ensure_store_on_node(state, node_id, store.store_id).await?;
    }

    for group in groups
        .iter()
        .filter(|group| group.replicas.iter().any(|replica| replica.node_id == nid))
    {
        let Some(local_replica) = group.replicas.iter().find(|replica| replica.node_id == nid) else {
            continue;
        };
        ensure_group_local(
            state,
            node_id,
            group.store_id,
            group.group_id,
            local_replica.replica_id,
            AddGroupInitialRole::Follower,
            // Defer for multi-replica groups until remotes are wired.
            Some(group.replicas.len() <= 1),
        )
        .await?;
        if let Err(err) = ensure_group_remotes(state, group).await {
            warn!(
                store_id = group.store_id,
                group_id = group.group_id,
                node_id,
                error = %err,
                "failed to restore group remotes for restarted node"
            );
        }
    }

    refresh_node_cache(state, node_id).await;
    Ok(())
}

// ── A5: Logical store plane ─────────────────────────────────────────

/// `GET /api/stores`. List stores aggregated from the monitor cache.
///
/// # Panics
/// Panics if the `RwLock` is poisoned (inside `snapshot()`).
pub async fn http_list_stores(
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
            entry.nodes.push(node_id.clone());
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
pub struct CreateStoreBody {
    pub store_id: u64,
    #[serde(default)]
    pub nodes: Vec<String>,
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
pub async fn http_add_store(
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
            .find_map(|s| s.node_id.map(|id| id.to_string()))
            .ok_or_else(|| err_400("no nodes with deployed servers"))?;
        vec![first]
    } else {
        body.nodes.clone()
    };

    let mut seen = HashSet::new();
    target_nodes.retain(|node_id| seen.insert(node_id.clone()));

    let mut reachable_targets: Vec<(String, ServerClient)> = Vec::with_capacity(target_nodes.len());
    for nid in &target_nodes {
        let url = mgmt_url_for_node(&state, nid)?;
        let client = build_server_client(url.clone())?;
        client.health().await.map_err(|e| {
            err_502(format!(
                "selected node {nid} is not currently reachable at {url}: {e}"
            ))
        })?;
        reachable_targets.push((nid.clone(), client));
    }

    let mut succeeded: Vec<String> = Vec::new();
    for (nid, client) in &reachable_targets {
        let req = AddStoreRequest {
            store_id: body.store_id,
            port: None,
        };
        match client.add_store(&req).await {
            Ok(_) => succeeded.push(nid.clone()),
            Err(e) => {
                // Roll back successful creations.
                for ok_nid in &succeeded {
                    if let Ok(u) = mgmt_url_for_node(&state, ok_nid) {
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
        refresh_node_cache(&state, nid).await;
    }

    {
        let mut cfg = state.config.write().unwrap();
        cfg.record_store(
            body.store_id,
            succeeded.iter().map(|s| s.parse().unwrap()).collect(),
        );
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
pub async fn http_get_store(
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
pub async fn http_remove_store(
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
        if let Ok(url) = mgmt_url_for_node(&state, nid) {
            if let Ok(client) = build_server_client(url) {
                let _ = client.remove_store(sid).await;
            }
        }
    }
    for nid in &view.nodes {
        refresh_node_cache(&state, nid).await;
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

// ── A6: Logical group plane ─────────────────────────────────────────

/// `GET /api/stores/:store_id/groups`. List groups from cache.
///
/// # Errors
/// Returns `404` if the store is not found.
pub async fn http_list_groups(
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
pub struct CreateGroupBody {
    pub group_id: u64,
    pub replica_id: u64,
    pub nodes: Vec<String>,
}

/// `POST /api/stores/:store_id/groups`. Create a group across the listed
/// nodes. Orchestrated: creates a local `PxGroup` on each node and wires
/// remote-replica entries. Rolls back on partial failure.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns an error if the nodes list is empty or any upstream RPC fails.
pub async fn http_add_group(
    State(state): State<AppState>,
    Path(sid): Path<u64>,
    Json(body): Json<CreateGroupBody>,
) -> Result<(StatusCode, Json<serde_json::Value>), (StatusCode, Json<ErrorBody>)> {
    if sid != 0 && !cluster_initialized(&state).await {
        return Err(err_409(
            "cluster not initialized; call POST /api/cluster/init first",
        ));
    }
    if body.nodes.is_empty() {
        return Err(err_400("nodes list must not be empty"));
    }

    // Phase 1: create the group on each node.
    let mut succeeded: Vec<(String, u64)> = Vec::new(); // (node_id, replica_id)
    let base_rid = body.replica_id;
    for (i, nid) in body.nodes.iter().enumerate() {
        let url = mgmt_url_for_node(&state, nid)?;
        let client = build_server_client(url)?;
        let rid = base_rid + i as u64;
        let req = AddGroupRequest {
            group_id: body.group_id,
            replica_id: rid,
            initial_role: Some(if i == 0 {
                AddGroupInitialRole::Leader
            } else {
                AddGroupInitialRole::Follower
            }),
            // Multi-node groups defer the driver until Phase-2 wires remotes;
            // single-node groups start it now.
            start_election: Some(body.nodes.len() <= 1),
        };
        match client.add_group(sid, &req).await {
            Ok(()) => succeeded.push((nid.clone(), rid)),
            Err(e) => {
                for (ok_nid, _) in &succeeded {
                    if let Ok(u) = mgmt_url_for_node(&state, ok_nid) {
                        if let Ok(c) = build_server_client(u) {
                            let _ = c.remove_group(sid, body.group_id).await;
                        }
                    }
                }
                return Err(err_502(format!("group create failed on node {nid}: {e}")));
            }
        }
    }

    // Refresh the cache for each node before wiring remotes so the per-store
    // `listen_addr` (each `PxKvStore` binds its own port) is known to the
    // monitor cache used by `grpc_endpoint_for_node`.
    for (nid, _) in &succeeded {
        refresh_node_cache(&state, nid).await;
    }

    // Phase 2: wire remote replicas. Each node gets every other node's
    // replica as a remote.
    for (i, (nid, _rid)) in succeeded.iter().enumerate() {
        let Ok(url) = mgmt_url_for_node(&state, nid) else {
            continue;
        };
        let Ok(client) = build_server_client(url) else {
            continue;
        };
        let mut remotes: Vec<RemoteReplicaInfo> = Vec::new();
        for (j, (peer_nid, peer_rid)) in succeeded.iter().enumerate() {
            if j == i {
                continue;
            }
            if let Some(ep) = grpc_endpoint_for_node(&state, peer_nid, sid).await {
                remotes.push(RemoteReplicaInfo {
                    replica_id: *peer_rid,
                    endpoint: ep,
                });
            }
        }
        if !remotes.is_empty() {
            let _ = client.add_remote_replicas(sid, body.group_id, &remotes).await;
        }
    }

    for (nid, _) in &succeeded {
        refresh_node_cache(&state, nid).await;
    }

    {
        let mut cfg = state.config.write().unwrap();
        cfg.record_group(
            sid,
            body.group_id,
            succeeded
                .iter()
                .map(|(node_id, replica_id)| ReplicaEntry {
                    replica_id: *replica_id,
                    node_id: node_id.parse().unwrap(),
                })
                .collect(),
        );
        for (node_id, _) in &succeeded {
            cfg.ensure_store_node(sid, node_id.parse().unwrap());
        }
    }
    state
        .persist()
        .map_err(|e| err_500(format!("persist config: {e}")))?;

    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({
            "store_id": sid,
            "group_id": body.group_id,
            "nodes": body.nodes,
        })),
    ))
}

/// `GET /api/stores/:store_id/groups/:group_id`. Aggregated group view.
///
/// # Errors
/// Returns `404` if the group is not found.
pub async fn http_get_group(
    State(state): State<AppState>,
    Path((sid, gid)): Path<(u64, u64)>,
    Recursive(_depth): Recursive,
) -> Result<Json<GroupView>, (StatusCode, Json<ErrorBody>)> {
    // Refresh the cache for every node currently believed to host this
    // store so role / leader info reflects the most recent topology
    // (elections happen asynchronously after the last write that
    // refreshed the cache; without this read-side refresh the response
    // would otherwise show stale `Follower` roles for the actual
    // leader). Bounded by the number of nodes hosting the store.
    let node_ids: Vec<String> = {
        let snap = state.monitor_cache.snapshot().await;
        snap.iter()
            .filter_map(|(nid, rec)| {
                if rec.stores.contains_key(&sid) {
                    Some(nid.clone())
                } else {
                    None
                }
            })
            .collect()
    };
    for nid in &node_ids {
        refresh_node_cache(&state, nid).await;
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
/// across every hosting node.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns `404` if the group is not found in the cache.
pub async fn http_remove_group(
    State(state): State<AppState>,
    Path((sid, gid)): Path<(u64, u64)>,
) -> Result<StatusCode, (StatusCode, Json<ErrorBody>)> {
    if sid == 0 && gid == 0 {
        return Err(err_409(
            "group 0 in store 0 is the system group; use POST /api/cluster/reset to tear down the entire cluster",
        ));
    }
    let view = state.monitor_cache.resolve_group(sid, gid).await.ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorBody {
                error: format!("group {gid} in store {sid} not found"),
            }),
        )
    })?;
    let node_ids: Vec<String> = view.replicas.iter().map(|r| r.node_id.clone()).collect();
    for nid in &node_ids {
        if let Ok(url) = mgmt_url_for_node(&state, nid) {
            if let Ok(client) = build_server_client(url) {
                let _ = client.remove_group(sid, gid).await;
            }
        }
    }
    for nid in &node_ids {
        refresh_node_cache(&state, nid).await;
    }
    {
        let mut cfg = state.config.write().unwrap();
        cfg.remove_group_record(sid, gid);
    }
    state
        .persist()
        .map_err(|e| err_500(format!("persist config: {e}")))?;
    Ok(StatusCode::NO_CONTENT)
}

// ── A7: Logical replica plane ─────────────────────────────────────

/// Roll back a partially-wired replica: deregister from `peer_nodes`,
/// then delete the local group on `target_node`. If
/// `created_store_on_target` is true, also drop the store on
/// `target_node` (we created it atomically in step 1).
async fn rollback_replica(
    state: &AppState,
    sid: u64,
    gid: u64,
    rid: u64,
    peer_nodes: &[String],
    target_node: &str,
    created_store_on_target: bool,
) {
    for nid in peer_nodes {
        if let Ok(u) = mgmt_url_for_node(state, nid) {
            if let Ok(c) = build_server_client(u) {
                let _ = c.remove_remote_replica(sid, gid, rid).await;
            }
        }
    }
    if let Ok(u) = mgmt_url_for_node(state, target_node) {
        if let Ok(c) = build_server_client(u) {
            if created_store_on_target {
                // `remove_store` cascades the group on that node.
                let _ = c.remove_store(sid).await;
            } else {
                let _ = c.remove_group(sid, gid).await;
            }
        }
    }
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
async fn grpc_endpoint_for_node(state: &AppState, node_id: &str, store_id: u64) -> Option<String> {
    let snap = state.monitor_cache.snapshot().await;
    if let Some(rec) = snap.get(node_id) {
        if let Some(addr) = rec.stores.get(&store_id).and_then(|s| s.listen_addr.clone()) {
            return Some(strip_scheme(remap_zero_host(&addr)));
        }
    }
    // Cache miss: the node's store is not up yet (or the monitor hasn't
    // observed it). Returning None lets callers skip wiring this peer
    // rather than overwriting the correct persisted-config endpoint with
    // a stale bootstrap port.
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

fn remap_zero_host(addr: &str) -> String {
    // `PxKvStore` binds to `0.0.0.0:port`; remap to a routable loopback
    // address for cross-process connections on the same host.
    addr.strip_prefix("0.0.0.0:")
        .map_or_else(|| addr.to_string(), |port| format!("127.0.0.1:{port}"))
}

/// `GET /api/stores/:s/groups/:g/replicas`. Unified replica list from
/// the monitor cache.
///
/// # Errors
/// Returns `404` if the group is not found.
pub async fn http_list_replicas(
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
/// (logical view).
///
/// # Errors
/// Returns `404` if the group or replica is not found.
pub async fn http_get_replica(
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
pub struct AddReplicaBody {
    pub node_id: String,
    #[serde(default)]
    pub replica_id: Option<u64>,
}

/// `POST /api/stores/:s/groups/:g/replicas`. Add a replica to an
/// existing group. Orchestrated per §6.4.3:
///
/// 1. Create a local `PxGroup` on the target `node_id`.
/// 2. Register the new replica as a remote on every existing peer.
/// 3. Register every existing peer as a remote on the new replica.
/// 4. On any step-2/3 failure, deregister and delete to roll back.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns an error if the group doesn't exist, the node has no server,
/// or any upstream RPC fails.
#[allow(clippy::too_many_lines)]
pub async fn http_add_replica(
    State(state): State<AppState>,
    Path((sid, gid)): Path<(u64, u64)>,
    Json(body): Json<AddReplicaBody>,
) -> Result<(StatusCode, Json<serde_json::Value>), (StatusCode, Json<ErrorBody>)> {
    // Resolve existing group to get the current replica set.
    let view = state.monitor_cache.resolve_group(sid, gid).await.ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorBody {
                error: format!("group {gid} in store {sid} not found"),
            }),
        )
    })?;

    // Auto-assign replica_id if not provided: max existing + 1.
    let new_rid = body
        .replica_id
        .unwrap_or_else(|| view.replicas.iter().map(|r| r.replica_id).max().unwrap_or(0) + 1);

    // Check for conflict.
    if view.replicas.iter().any(|r| r.replica_id == new_rid) {
        return Err((
            StatusCode::CONFLICT,
            Json(ErrorBody {
                error: format!("replica {new_rid} already exists in group {gid}"),
            }),
        ));
    }

    let target_node = &body.node_id;

    // Step 1: ensure the target node hosts the store, then place the
    // new replica's local PxGroup on it.
    //
    // `crow-kv-server` has no separate "add store" + "add group"
    // sequence for a fresh store: `add_store` creates store + initial
    // group + initial replica atomically, while `add_group` requires
    // the store to already exist. We branch on whether the monitor
    // cache shows this node already hosting the store.
    let target_has_store = state
        .monitor_cache
        .snapshot()
        .await
        .get(target_node.as_str())
        .is_some_and(|rec| rec.stores.contains_key(&sid));

    let url = mgmt_url_for_node(&state, target_node)?;
    let client = build_server_client(url)?;
    let created_store_on_target = if target_has_store {
        let req = AddGroupRequest {
            group_id: gid,
            replica_id: new_rid,
            initial_role: Some(AddGroupInitialRole::Follower),
            // The new replica joins a multi-replica group; remotes are wired in
            // step 3, which starts the driver. Defer.
            start_election: Some(false),
        };
        client
            .add_group(sid, &req)
            .await
            .map_err(|e| err_502(format!("create local group on node {target_node}: {e}")))?;
        false
    } else {
        let req = AddStoreRequest {
            store_id: sid,
            port: None,
        };
        client
            .add_store(&req)
            .await
            .map_err(|e| err_502(format!("create local store on node {target_node}: {e}")))?;
        let req = AddGroupRequest {
            group_id: gid,
            replica_id: new_rid,
            initial_role: Some(AddGroupInitialRole::Follower),
            // The new replica joins a multi-replica group; remotes are wired in
            // step 3, which starts the driver. Defer.
            start_election: Some(false),
        };
        client
            .add_group(sid, &req)
            .await
            .map_err(|e| err_502(format!("create local group on node {target_node}: {e}")))?;
        true
    };

    // Refresh the target node's cache so the new store's `listen_addr` is
    // known before we wire it as a remote on existing peers. Each
    // `PxKvStore` on a `crow-kv-server` binds its own port, so we cannot
    // rely on the bootstrap `grpc_url` configured at deploy time.
    refresh_node_cache(&state, target_node).await;

    // Step 2: Register the new replica as a remote on every existing peer.
    let Some(new_endpoint) = grpc_endpoint_for_node(&state, target_node, sid).await else {
        return Err(err_502(format!(
            "could not determine gRPC endpoint for new replica on node {target_node}"
        )));
    };
    let new_remote = RemoteReplicaInfo {
        replica_id: new_rid,
        endpoint: new_endpoint,
    };
    let mut wired_peers: Vec<String> = Vec::new();
    for existing in &view.replicas {
        let Ok(peer_url) = mgmt_url_for_node(&state, &existing.node_id) else {
            continue;
        };
        let Ok(peer_client) = build_server_client(peer_url) else {
            continue;
        };
        if let Err(e) = peer_client
            .add_remote_replicas(sid, gid, std::slice::from_ref(&new_remote))
            .await
        {
            rollback_replica(
                &state,
                sid,
                gid,
                new_rid,
                &wired_peers,
                target_node,
                created_store_on_target,
            )
            .await;
            return Err(err_502(format!(
                "wire new replica on peer {}: {e}",
                existing.node_id
            )));
        }
        wired_peers.push(existing.node_id.clone());
    }

    // Step 3: Register every existing peer as a remote on the new replica.
    let mut existing_remotes: Vec<RemoteReplicaInfo> = Vec::with_capacity(view.replicas.len());
    for r in &view.replicas {
        if let Some(ep) = grpc_endpoint_for_node(&state, &r.node_id, sid).await {
            existing_remotes.push(RemoteReplicaInfo {
                replica_id: r.replica_id,
                endpoint: ep,
            });
        }
    }
    if !existing_remotes.is_empty() {
        let new_url = mgmt_url_for_node(&state, target_node)?;
        let new_client = build_server_client(new_url)?;
        if let Err(e) = new_client.add_remote_replicas(sid, gid, &existing_remotes).await {
            let all_peers: Vec<String> = view.replicas.iter().map(|r| r.node_id.clone()).collect();
            rollback_replica(
                &state,
                sid,
                gid,
                new_rid,
                &all_peers,
                target_node,
                created_store_on_target,
            )
            .await;
            return Err(err_502(format!("wire existing peers on new replica: {e}")));
        }
    }

    // Refresh cache for all affected nodes.
    refresh_node_cache(&state, target_node).await;
    for existing in &view.replicas {
        refresh_node_cache(&state, &existing.node_id).await;
    }

    {
        let mut cfg = state.config.write().unwrap();
        cfg.ensure_store_node(sid, target_node.parse().unwrap());
        cfg.add_group_replica(
            sid,
            gid,
            ReplicaEntry {
                replica_id: new_rid,
                node_id: target_node.parse().unwrap(),
            },
        );
    }
    state
        .persist()
        .map_err(|e| err_500(format!("persist config: {e}")))?;

    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({
            "store_id": sid,
            "group_id": gid,
            "replica_id": new_rid,
            "node_id": target_node,
        })),
    ))
}

/// `DELETE /api/stores/:s/groups/:g/replicas/:rid`. Remove a replica:
///
/// 1. Deregister it as a remote from every other replica's node.
/// 2. Delete the local `PxGroup` on the hosting node.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns `404` if the group or replica is not found.
pub async fn http_remove_replica(
    State(state): State<AppState>,
    Path((sid, gid, rid)): Path<(u64, u64, u64)>,
) -> Result<StatusCode, (StatusCode, Json<ErrorBody>)> {
    let view = state.monitor_cache.resolve_group(sid, gid).await.ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorBody {
                error: format!("group {gid} in store {sid} not found"),
            }),
        )
    })?;

    let target = view
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
    let target_node = target.node_id.clone();

    // Step 0: if the replica being removed is currently the leader, ask
    // it to step down first and wait (bounded) for a survivor to win a
    // fresh election, instead of leaving the group leaderless until the
    // old leader's lease expires. Best-effort: a timeout here doesn't
    // block the removal -- the lease-expiry fallback still applies.
    if view.leader_id() == Some(rid) {
        if let Ok(url) = mgmt_url_for_node(&state, &target_node) {
            if let Ok(client) = build_server_client(url) {
                match client
                    .step_down(
                        sid,
                        gid,
                        &StepDownRequest {
                            reason: format!("replica {rid} removal"),
                        },
                    )
                    .await
                {
                    Ok(result) if result.accepted => {
                        if let Some(survivor) = view.replicas.iter().find(|r| r.replica_id != rid) {
                            if let Ok(survivor_url) = mgmt_url_for_node(&state, &survivor.node_id) {
                                if !wait_for_new_leader(&survivor_url, sid, gid, rid, Duration::from_secs(5))
                                    .await
                                {
                                    warn!(
                                        store_id = sid,
                                        group_id = gid,
                                        replica_id = rid,
                                        "leader step-down accepted but no new leader observed within timeout; proceeding anyway"
                                    );
                                }
                            }
                        }
                    }
                    Ok(_) => {
                        // Not leader anymore by the time the call landed
                        // (already stepped down / re-elected away) -- fine,
                        // nothing to wait for.
                    }
                    Err(e) => {
                        warn!(
                            store_id = sid,
                            group_id = gid,
                            replica_id = rid,
                            error = %e,
                            "step-down request failed; proceeding with removal, leader-less window will close via lease expiry"
                        );
                    }
                }
            }
        }
    }

    // Step 1: Deregister this replica as a remote from every peer.
    for peer in &view.replicas {
        if peer.replica_id == rid {
            continue;
        }
        if let Ok(url) = mgmt_url_for_node(&state, &peer.node_id) {
            if let Ok(client) = build_server_client(url) {
                let _ = client.remove_remote_replica(sid, gid, rid).await;
            }
        }
    }

    // Step 2: Delete the local group on the target node.
    if let Ok(url) = mgmt_url_for_node(&state, &target_node) {
        if let Ok(client) = build_server_client(url) {
            let _ = client.remove_group(sid, gid).await;
        }
    }

    // Refresh all affected nodes.
    refresh_node_cache(&state, &target_node).await;
    for peer in &view.replicas {
        if peer.replica_id != rid {
            refresh_node_cache(&state, &peer.node_id).await;
        }
    }

    {
        let mut cfg = state.config.write().unwrap();
        cfg.remove_group_replica(sid, gid, rid);
    }
    state
        .persist()
        .map_err(|e| err_500(format!("persist config: {e}")))?;

    Ok(StatusCode::NO_CONTENT)
}

// ── Cluster init (R2) ───────────────────────────────────────────────

/// Request body for `POST /api/cluster/init`.
#[derive(Debug, Deserialize)]
pub struct ClusterInitBody {
    /// Node IDs to include in the system group (store 0, group 0).
    /// Must be non-empty. For a single node, group 0 self-elects.
    /// For multiple nodes, remotes are wired and election starts after.
    pub nodes: Vec<String>,
}

/// `POST /api/cluster/init` — initialize the cluster by bootstrapping
/// the system group (store 0, group 0) on the selected nodes, wiring
/// remotes, and auto-finalizing the topology cutover.
///
/// # Errors
/// Returns `400` if `nodes` is empty, `502` if a node is unreachable or
/// `system/init` fails, `500` if config persistence fails.
///
/// # Panics
/// Does not panic; panics in inner helpers are not reachable.
#[allow(clippy::too_many_lines)]
pub async fn http_cluster_init(
    State(state): State<AppState>,
    Json(body): Json<ClusterInitBody>,
) -> Result<(StatusCode, Json<serde_json::Value>), (StatusCode, Json<ErrorBody>)> {
    if body.nodes.is_empty() {
        return Err(err_400("nodes list must not be empty"));
    }

    let mut seen = HashSet::new();
    let mut target_nodes = body.nodes.clone();
    target_nodes.retain(|nid| seen.insert(nid.clone()));

    let single_node = target_nodes.len() == 1;

    // Phase 1: call /system/init on each node.
    let mut succeeded: Vec<(String, u64)> = Vec::new();
    for (i, nid) in target_nodes.iter().enumerate() {
        let url = mgmt_url_for_node(&state, nid)?;
        let client = build_server_client(url)?;
        client
            .health()
            .await
            .map_err(|e| err_502(format!("node {nid} not reachable: {e}")))?;

        let replica_id = 1 + i as u64;
        let req = crow_console_shared::mgmt::SystemInitRequest {
            replica_id,
            start_election: single_node,
        };
        match client.system_init(&req).await {
            Ok(resp) => {
                info!(
                    node_id = nid,
                    replica_id = resp.replica_id,
                    listen_addr = resp.listen_addr.as_deref().unwrap_or("?"),
                    "system/init succeeded"
                );
                succeeded.push((nid.clone(), replica_id));
            }
            Err(e) => {
                // 409 Conflict means group 0 already exists — the node was
                // already initialized. Treat as success and continue.
                let is_already_init = matches!(
                    &e,
                    SharedError::UpstreamRpc { status, .. }
                    if status.contains("409")
                );
                if is_already_init {
                    info!(node_id = nid, "system/init: group 0 already exists, skipping");
                    succeeded.push((nid.clone(), replica_id));
                    continue;
                }
                // Rollback: remove group 0 on nodes that succeeded.
                for (ok_nid, _) in &succeeded {
                    if let Ok(u) = mgmt_url_for_node(&state, ok_nid) {
                        if let Ok(c) = build_server_client(u) {
                            let _ = c.remove_group(0, 0).await;
                        }
                    }
                }
                return Err(err_502(format!("system/init failed on node {nid}: {e}")));
            }
        }
    }

    // Phase 2: refresh caches so we can resolve gRPC endpoints.
    for (nid, _) in &succeeded {
        refresh_node_cache(&state, nid).await;
    }

    // Phase 3: wire remotes for multi-node.
    if !single_node {
        for (i, (nid, _rid)) in succeeded.iter().enumerate() {
            let Ok(url) = mgmt_url_for_node(&state, nid) else {
                continue;
            };
            let Ok(client) = build_server_client(url) else {
                continue;
            };
            let mut remotes: Vec<RemoteReplicaInfo> = Vec::new();
            for (j, (peer_nid, peer_rid)) in succeeded.iter().enumerate() {
                if j == i {
                    continue;
                }
                if let Some(ep) = grpc_endpoint_for_node(&state, peer_nid, 0).await {
                    remotes.push(RemoteReplicaInfo {
                        replica_id: *peer_rid,
                        endpoint: ep,
                    });
                }
            }
            if !remotes.is_empty() {
                let _ = client.add_remote_replicas(0, 0, &remotes).await;
            }
        }

        // Refresh caches after remote wiring.
        for (nid, _) in &succeeded {
            refresh_node_cache(&state, nid).await;
        }
    }

    // Phase 4: persist topology in console config.
    {
        let mut cfg = state.config.write().unwrap();
        let store_nodes: Vec<u64> = succeeded.iter().map(|(n, _)| n.parse().unwrap()).collect();
        cfg.record_store(0, store_nodes);
        let replicas: Vec<ReplicaEntry> = succeeded
            .iter()
            .map(|(nid, rid)| ReplicaEntry {
                replica_id: *rid,
                node_id: nid.parse().unwrap(),
            })
            .collect();
        cfg.record_group(0, 0, replicas);
    }
    state
        .persist()
        .map_err(|e| err_500(format!("persist config: {e}")))?;

    // Phase 5: finalize topology — write all topology metadata into
    // group 0 KV and set the /topology/ready flag. Try each succeeded
    // node until one accepts (the leader will).
    let finalize_body = {
        let cfg = state.config.read().unwrap();
        build_topology_finalize_body(&cfg, &succeeded)
    };
    let mut finalized = false;
    for (nid, _) in &succeeded {
        let Ok(url) = mgmt_url_for_node(&state, nid) else {
            continue;
        };
        let Ok(client) = build_server_client(url) else {
            continue;
        };
        match client.topology_finalize(&finalize_body).await {
            Ok(resp) => {
                info!(
                    node_id = nid,
                    already_finalized = resp.already_finalized,
                    "topology/finalize succeeded"
                );
                finalized = true;
                break;
            }
            Err(e) => {
                warn!(node_id = nid, error = %e, "topology/finalize failed; trying next node");
            }
        }
    }
    if !finalized {
        warn!("topology/finalize failed on all nodes; cluster init succeeded but topology not written to group 0");
    }

    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({
            "store_id": 0,
            "group_id": 0,
            "nodes": succeeded.iter().map(|(n, r)| serde_json::json!({
                "node_id": n,
                "replica_id": r,
            })).collect::<Vec<_>>(),
        })),
    ))
}

/// Build the `TopologyFinalizeRequest` from the console config, capturing
/// racks, nodes, stores, groups, and replicas for writing into group 0 KV.
fn build_topology_finalize_body(
    cfg: &crow_console_shared::config::ConsoleConfig,
    succeeded: &[(String, u64)],
) -> TopologyFinalizeRequest {
    let racks: Vec<TopologyRackInput> = cfg
        .racks
        .iter()
        .map(|r| TopologyRackInput {
            rack_id: r.id.to_string(),
            name: r.name.clone(),
        })
        .collect();

    let nodes: Vec<TopologyNodeInput> = cfg
        .nodes
        .iter()
        .filter_map(|n| {
            let server = cfg.server_for_node(n.id)?;
            Some(TopologyNodeInput {
                node_id: n.id.to_string(),
                rack_id: n.rack_id.to_string(),
                host: n.host.clone(),
                mgmt_endpoint: server.url.clone(),
                grpc_endpoint: server.grpc_url.clone().unwrap_or_default(),
                election_profile: server.election_profile.clone(),
                auto_start: server.auto_start,
            })
        })
        .collect();

    let stores: Vec<TopologyStoreInput> = cfg
        .stores
        .iter()
        .map(|s| TopologyStoreInput {
            store_id: s.store_id,
            nodes: s.nodes.iter().map(std::string::ToString::to_string).collect(),
        })
        .collect();

    let groups: Vec<TopologyGroupInput> = cfg
        .groups
        .iter()
        .map(|g| TopologyGroupInput {
            group_id: g.group_id,
            store_id: g.store_id,
        })
        .collect();

    // Build replicas from group entries. For the system group (0/0),
    // use the succeeded list from init; for other groups, use config.
    let mut replicas: Vec<TopologyReplicaInput> = Vec::new();
    for (nid, rid) in succeeded {
        let server = cfg.server_for_node(nid.parse().unwrap());
        let endpoint = server.and_then(|s| s.grpc_url.clone()).unwrap_or_default();
        let role = if *rid == succeeded.first().map_or(1, |(_, r)| *r) {
            "leader"
        } else {
            "follower"
        };
        replicas.push(TopologyReplicaInput {
            group_id: 0,
            replica_id: *rid,
            node_id: nid.clone(),
            role: role.to_string(),
            voting: true,
            endpoint,
        });
    }
    for g in &cfg.groups {
        if g.store_id == 0 && g.group_id == 0 {
            continue; // already handled above
        }
        for r in &g.replicas {
            let server = cfg.server_for_node(r.node_id);
            let endpoint = server.and_then(|s| s.grpc_url.clone()).unwrap_or_default();
            replicas.push(TopologyReplicaInput {
                group_id: g.group_id,
                replica_id: r.replica_id,
                node_id: r.node_id.to_string(),
                role: "follower".to_string(),
                voting: true,
                endpoint,
            });
        }
    }

    TopologyFinalizeRequest {
        racks,
        nodes,
        stores,
        groups,
        replicas,
    }
}

// ── Metrics proxy (R11) ───────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct MetricsQuery {
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
pub async fn http_node_metrics(
    State(state): State<AppState>,
    Path(node_id): Path<String>,
    Query(q): Query<MetricsQuery>,
) -> Result<Json<MetricsResponse>, (StatusCode, Json<ErrorBody>)> {
    let url = mgmt_url_for_node(&state, &node_id)?;
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
pub async fn http_group_metrics(
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
    let url = mgmt_url_for_node(&state, &node_id)?;
    let client = build_server_client(url)?;
    // If the caller supplied a prefix, prepend the group scope; otherwise
    // default to the group's own prefix.
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
pub async fn http_store_metrics(
    State(state): State<AppState>,
    Path(sid): Path<u64>,
    Query(q): Query<MetricsQuery>,
) -> Result<Json<MetricsResponse>, (StatusCode, Json<ErrorBody>)> {
    // Collect all group IDs for this store from the monitor cache.
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

    // Fetch from each group's leader node. Deduplicate by node (a single
    // node may host multiple groups in the store; one fetch with the
    // store prefix covers all of them).
    let mut seen_nodes: HashSet<String> = HashSet::new();
    let mut merged: Vec<crow_console_shared::MetricPointView> = Vec::new();
    let mut window_secs = 5.0_f64;
    let mut timestamp = String::new();

    for gid in &group_ids {
        let Some((_rid, node_id)) = state.monitor_cache.leader_for(sid, *gid).await else {
            continue;
        };
        if !seen_nodes.insert(node_id.clone()) {
            continue;
        }
        let Ok(url) = mgmt_url_for_node(&state, &node_id) else {
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
