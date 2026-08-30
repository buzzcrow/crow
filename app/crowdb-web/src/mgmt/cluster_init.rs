// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! R2: Cluster initialization — system group bootstrap.

use crate::error::{err_400, err_500, err_502, ErrorBody};
use crate::mgmt::{build_server_client, mgmt_url_for_node, refresh_node_cache, rpc_endpoint_for_node};
use crate::state::AppState;
use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;
use crowdb_console_shared::cluster::NodeId;
use crowdb_console_shared::config::ReplicaEntry;
use crowdb_console_shared::error::Error as SharedError;
use crowdb_console_shared::mgmt::RemoteReplicaInfo;
use serde::Deserialize;
use std::collections::HashSet;
use tracing::{info, warn};

/// Request body for `POST /api/cluster/init`.
#[derive(Debug, Deserialize)]
pub(crate) struct ClusterInitBody {
    /// Node IDs to include in the system group (store 0, group 0).
    /// Must be non-empty. For a single node, group 0 self-elects.
    /// For multiple nodes, remotes are wired and election starts after.
    pub nodes: Vec<NodeId>,
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
pub(crate) async fn http_cluster_init(
    State(state): State<AppState>,
    Json(body): Json<ClusterInitBody>,
) -> Result<(StatusCode, Json<serde_json::Value>), (StatusCode, Json<ErrorBody>)> {
    if body.nodes.is_empty() {
        return Err(err_400("nodes list must not be empty"));
    }

    let mut seen = HashSet::new();
    let mut target_nodes = body.nodes.clone();
    target_nodes.retain(|nid| seen.insert(*nid));

    let single_node = target_nodes.len() == 1;

    // Phase 1: call /system/init on each node.
    let mut succeeded: Vec<(NodeId, u64)> = Vec::new();
    for (i, nid) in target_nodes.iter().enumerate() {
        let url = mgmt_url_for_node(&state, *nid)?;
        let client = build_server_client(url)?;
        client
            .health()
            .await
            .map_err(|e| err_502(format!("node {nid} not reachable: {e}")))?;

        let replica_id = 1 + i as u64;
        let req = crowdb_console_shared::mgmt::SystemInitRequest {
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
                succeeded.push((*nid, replica_id));
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
                    succeeded.push((*nid, replica_id));
                    continue;
                }
                // Rollback: remove group 0 on nodes that succeeded.
                for (ok_nid, _) in &succeeded {
                    if let Ok(u) = mgmt_url_for_node(&state, *ok_nid) {
                        if let Ok(c) = build_server_client(u) {
                            let _ = c.remove_group(0, 0).await;
                        }
                    }
                }
                return Err(err_502(format!("system/init failed on node {nid}: {e}")));
            }
        }
    }

    // Phase 2: refresh caches so we can resolve crowdb-rpc endpoints.
    for (nid, _) in &succeeded {
        refresh_node_cache(&state, *nid).await;
    }

    // Phase 3: wire remotes for multi-node.
    if !single_node {
        for (i, (nid, _rid)) in succeeded.iter().enumerate() {
            let Ok(url) = mgmt_url_for_node(&state, *nid) else {
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
                if let Some(ep) = rpc_endpoint_for_node(&state, *peer_nid, 0).await {
                    remotes.push(RemoteReplicaInfo {
                        replica_id: *peer_rid,
                        endpoint: ep,
                        voting: true,
                    });
                }
            }
            if !remotes.is_empty() {
                let _ = client.add_remote_replicas(0, 0, &remotes).await;
            }
        }

        // Refresh caches after remote wiring.
        for (nid, _) in &succeeded {
            refresh_node_cache(&state, *nid).await;
        }
    }

    // Phase 4: persist topology in console config.
    {
        let mut cfg = state.config.write().unwrap();
        let store_nodes: Vec<u64> = succeeded.iter().map(|(n, _)| *n).collect();
        cfg.record_store(0, store_nodes);
        let replicas: Vec<ReplicaEntry> = succeeded
            .iter()
            .map(|(nid, rid)| ReplicaEntry {
                replica_id: *rid,
                node_id: *nid,
            })
            .collect();
        cfg.record_group(0, 0, replicas);
    }
    state
        .persist()
        .map_err(|e| err_500(format!("persist config: {e}")))?;

    // Phase 5: write hardware hierarchy + KV-cluster topology into
    // group 0 via HardwareClient + KVClusterMetaClient. Build a
    // CrowdbKvClient seeded with all group-0 mgmt URLs as topology
    // discovery seeds, plus the first crowdb-rpc endpoint as the initial
    // leader hint.
    let cfg_snapshot = state.config.read().unwrap().clone();
    let mgmt_seeds: Vec<String> = succeeded
        .iter()
        .filter_map(|(nid, _)| mgmt_url_for_node(&state, *nid).ok())
        .collect();
    let mut topology_written = false;
    for (nid, _) in &succeeded {
        let Some(rpc_ep) = rpc_endpoint_for_node(&state, *nid, 0).await else {
            continue;
        };
        let kv_client =
            crowdb_kv_client::CrowdbKvClient::new(crowdb_kv_client::ClientConfig::new(mgmt_seeds.clone()));
        kv_client.seed_leader(0, 0, rpc_ep.clone());
        let kv_client2 =
            crowdb_kv_client::CrowdbKvClient::new(crowdb_kv_client::ClientConfig::new(mgmt_seeds.clone()));
        kv_client2.seed_leader(0, 0, rpc_ep.clone());
        let hw = crowdb_kv_client::HardwareClient::new(kv_client);
        let meta = crowdb_kv_client::KVClusterMetaClient::new(kv_client2);

        // Write hardware hierarchy.
        let mut hw_ok = true;
        for rack in &cfg_snapshot.racks {
            let value = crowdb_protocol::common::RackValue {
                status: crowdb_protocol::common::HwStatus::Up as i32,
                node_ids: Vec::new(),
            };
            if let Err(e) = hw.add_rack(rack.id, &value).await {
                warn!(rack_id = rack.id, error = %e, "Phase 5: add_rack failed");
                hw_ok = false;
            }
        }
        for node in &cfg_snapshot.nodes {
            let value = crowdb_protocol::common::NodeValue {
                status: crowdb_protocol::common::HwStatus::Up as i32,
                last_used_dg_id: 0,
                disk_group_ids: Vec::new(),
                status_changed_at_ms: 0,
                temp_failure_since_ms: None,
            };
            if let Err(e) = hw.add_node(node.rack_id, node.id, &value).await {
                warn!(node_id = node.id, error = %e, "Phase 5: add_node failed");
                hw_ok = false;
            }
        }

        // Write KV-cluster topology.
        let mut meta_ok = true;
        for store in &cfg_snapshot.stores {
            if let Err(e) = meta.add_store(store.store_id, &store.nodes).await {
                warn!(store_id = store.store_id, error = %e, "Phase 5: add_store failed");
                meta_ok = false;
            }
        }
        for group in &cfg_snapshot.groups {
            if let Err(e) = meta.add_group(group.store_id, group.group_id).await {
                warn!(error = %e, "Phase 5: add_group failed");
                meta_ok = false;
            }
            for replica in &group.replicas {
                let server = cfg_snapshot.server_for_node(replica.node_id);
                let endpoint = server.and_then(|s| s.rpc_url.clone()).unwrap_or_default();
                let value = crowdb_protocol::common::ReplicaValue {
                    store_id: group.store_id,
                    group_id: group.group_id,
                    replica_id: replica.replica_id,
                    node_id: replica.node_id,
                    role: String::new(),
                    voting: true,
                    endpoint,
                };
                if let Err(e) = meta.add_replica(&value).await {
                    warn!(error = %e, "Phase 5: add_replica failed");
                    meta_ok = false;
                }
            }
        }

        if hw_ok && meta_ok {
            info!(node_id = nid, "Phase 5: topology written to group 0");
            topology_written = true;
            break;
        }
        warn!(node_id = nid, "Phase 5: partial write; trying next node");
    }
    if !topology_written {
        warn!("Phase 5: topology write failed on all nodes; cluster init succeeded but topology not fully written to group 0");
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
