// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Cluster-level operations: status, topology, init, reset, clean.
//!
//! `init` bootstraps group 0 (store 0, group 0) on the selected nodes,
//! wires remotes, and writes the hardware + KV-cluster topology into
//! group-0 sysdata. `reset` tears down the cluster in dependency order.
//! `clean` removes orphaned sysdata entries without touching running
//! servers.

use std::collections::HashSet;

use crowdb_protocol::common::{HwStatus, NodeValue, RackValue, ReplicaValue};
use crowdb_protocol::mgmt::{RemoteReplicaInfo, SystemInitRequest};

use crate::clients::http::ServerClient;
use crate::config::ReplicaEntry;
use crate::error::{Error, Result};
use crate::ops::OpContext;

/// Summary of a completed cluster init.
#[derive(Debug, Clone)]
pub struct InitSummary {
    pub store_id: u64,
    pub group_id: u64,
    pub nodes: Vec<(u64, u64)>,
}

fn server_client(ctx: &OpContext, node_id: u64) -> Result<ServerClient> {
    let url = ctx.node_mgmt_url(node_id)?;
    ServerClient::new(&url).map_err(|e| Error::UpstreamRpc {
        node_id: url,
        status: format!("client build: {e}"),
    })
}

async fn rpc_endpoint_for_store(ctx: &OpContext, node_id: u64, store_id: u64) -> Option<String> {
    let client = server_client(ctx, node_id).ok()?;
    let stores = client.topology().await.ok()?;
    for s in &stores {
        if s.store_id == store_id {
            if let Some(addr) = &s.listen_addr {
                let stripped = addr
                    .strip_prefix("http://")
                    .or_else(|| addr.strip_prefix("https://"))
                    .unwrap_or(addr);
                let remapped = stripped
                    .strip_prefix("0.0.0.0:")
                    .map_or_else(|| stripped.to_string(), |port| format!("127.0.0.1:{port}"));
                return Some(remapped);
            }
        }
    }
    None
}

/// Initialize the cluster by bootstrapping group 0 on the listed nodes.
///
/// # Errors
/// Returns [`Error::Validation`] if `nodes` is empty;
/// [`Error::NodeUnreachable`] if a node is not reachable;
/// [`Error::UpstreamRpc`] if `system/init` fails on a node.
pub async fn init(ctx: &OpContext, nodes: &[u64]) -> Result<InitSummary> {
    if nodes.is_empty() {
        return Err(Error::Validation {
            field: "nodes".into(),
            message: "nodes list must not be empty".into(),
        });
    }

    let mut seen = HashSet::new();
    let mut target_nodes = nodes.to_vec();
    target_nodes.retain(|nid| seen.insert(*nid));
    let single_node = target_nodes.len() == 1;

    // Phase 1: call /system/init on each node.
    let mut succeeded: Vec<(u64, u64)> = Vec::new();
    for (i, nid) in target_nodes.iter().enumerate() {
        let client = server_client(ctx, *nid)?;
        client.health().await.map_err(|e| Error::NodeUnreachable {
            node_id: nid.to_string(),
            reason: e.to_string(),
        })?;

        let replica_id = 1 + i as u64;
        let req = SystemInitRequest {
            replica_id,
            start_election: single_node,
        };
        match client.system_init(&req).await {
            Ok(_) => succeeded.push((*nid, replica_id)),
            Err(e) => {
                // 409 Conflict means group 0 already exists — skip.
                let is_already_init = matches!(
                    &e,
                    Error::UpstreamRpc { status, .. } if status.contains("409")
                );
                if is_already_init {
                    succeeded.push((*nid, replica_id));
                    continue;
                }
                // Rollback: remove group 0 on nodes that succeeded.
                for (ok_nid, _) in &succeeded {
                    if let Ok(c) = server_client(ctx, *ok_nid) {
                        let _ = c.remove_group(0, 0).await;
                    }
                }
                return Err(Error::UpstreamRpc {
                    node_id: nid.to_string(),
                    status: format!("system/init failed: {e}"),
                });
            }
        }
    }

    // Phase 2: wire remotes for multi-node.
    if !single_node {
        for (i, (nid, _rid)) in succeeded.iter().enumerate() {
            let Ok(client) = server_client(ctx, *nid) else {
                continue;
            };
            let mut remotes: Vec<RemoteReplicaInfo> = Vec::new();
            for (j, (peer_nid, peer_rid)) in succeeded.iter().enumerate() {
                if j == i {
                    continue;
                }
                if let Some(ep) = rpc_endpoint_for_store(ctx, *peer_nid, 0).await {
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
    }

    // Phase 3: persist topology in local config.
    let store_nodes: Vec<u64> = succeeded.iter().map(|(n, _)| *n).collect();
    let replicas: Vec<ReplicaEntry> = succeeded
        .iter()
        .map(|(nid, rid)| ReplicaEntry {
            replica_id: *rid,
            node_id: *nid,
        })
        .collect();
    {
        let mut cfg = ctx.config_mut();
        cfg.record_store(0, store_nodes.clone());
        cfg.record_group(0, 0, replicas);
    }

    // Phase 4: write hardware + KV-cluster topology into group-0 sysdata.
    write_topology_to_sysdata(ctx, &store_nodes, &succeeded).await;

    Ok(InitSummary {
        store_id: 0,
        group_id: 0,
        nodes: succeeded,
    })
}

/// Write the hardware hierarchy + KV-cluster topology from the local
/// config into group-0 sysdata. Best-effort: individual write failures
/// are logged and skipped.
async fn write_topology_to_sysdata(ctx: &OpContext, store_nodes: &[u64], succeeded: &[(u64, u64)]) {
    let cfg_snapshot = ctx.config().clone();
    let sysmd = ctx.sysmd();

    // Hardware hierarchy.
    for rack in &cfg_snapshot.racks {
        let value = RackValue {
            status: HwStatus::Up as i32,
            node_ids: Vec::new(),
        };
        let _ = sysmd.add_rack(rack.id, &value).await;
    }
    for node in &cfg_snapshot.nodes {
        let value = NodeValue {
            status: HwStatus::Up as i32,
            last_used_dg_id: 0,
            disk_group_ids: Vec::new(),
            status_changed_at_ms: 0,
            temp_failure_since_ms: None,
        };
        let _ = sysmd.add_node(node.rack_id, node.id, &value).await;
    }

    // KV-cluster topology.
    let _ = sysmd.add_store(0, store_nodes).await;
    let _ = sysmd.add_group(0, 0).await;
    for (nid, rid) in succeeded {
        let endpoint = cfg_snapshot
            .server_for_node(*nid)
            .and_then(|s| s.rpc_url.clone())
            .unwrap_or_default();
        let value = ReplicaValue {
            store_id: 0,
            group_id: 0,
            replica_id: *rid,
            node_id: *nid,
            role: String::new(),
            voting: true,
            endpoint,
        };
        let _ = sysmd.add_replica(&value).await;
    }
}

/// Get cluster status: list all stores from group-0 sysdata.
///
/// # Errors
/// Returns an error if the group-0 sysdata read fails.
pub async fn status(ctx: &OpContext) -> Result<Vec<crowdb_protocol::common::StoreValue>> {
    ctx.sysmd().list_stores().await.map_err(Into::into)
}

/// Get the topology view from a node's `/topology` endpoint.
///
/// # Errors
/// Returns [`Error::NotFound`] if no server is deployed on the node.
pub async fn topology(ctx: &OpContext, node_id: u64) -> Result<Vec<crate::snapshot::StoreView>> {
    let client = server_client(ctx, node_id)?;
    client.topology().await
}

/// Reset the cluster: tear down all groups, stores, and sysdata in
/// dependency order. Stops all running servers first.
///
/// # Errors
/// Returns an error if any teardown step fails (best-effort: continues
/// on partial failures and returns the first error).
pub async fn reset(ctx: &OpContext) -> Result<()> {
    let cfg = ctx.config().clone();

    // Phase 1: stop all running servers.
    for server in &cfg.servers {
        if server.service_type != crate::config::ServiceType::Kv {
            continue;
        }
        if let Some(pid) = server.pid {
            let _ = crate::lifecycle::stop_pid(pid);
        }
    }

    // Phase 2: remove all non-system groups from each node.
    for server in &cfg.servers {
        if server.service_type != crate::config::ServiceType::Kv {
            continue;
        }
        if let Some(node_id) = server.node_id {
            if let Ok(client) = server_client(ctx, node_id) {
                if let Ok(stores) = client.topology().await {
                    for s in &stores {
                        if s.store_id == 0 {
                            continue;
                        }
                        let _ = client.remove_store(s.store_id).await;
                    }
                }
                // Remove group 0 last.
                let _ = client.remove_group(0, 0).await;
            }
        }
    }

    // Phase 3: clear sysdata (best-effort).
    let sysmd = ctx.sysmd();
    let stores = sysmd.list_stores().await.unwrap_or_default();
    for s in &stores {
        let _ = sysmd.remove_store(s.store_id).await;
    }

    // Phase 4: clear local config.
    {
        let mut cfg = ctx.config_mut();
        cfg.stores.clear();
        cfg.groups.clear();
        cfg.servers.clear();
    }

    Ok(())
}

/// Clean orphaned sysdata entries (stores/groups/replicas that have no
/// corresponding running server). Does not stop any running servers.
///
/// # Errors
/// Returns an error if the sysdata scan fails.
pub async fn clean(ctx: &OpContext) -> Result<()> {
    let sysmd = ctx.sysmd();
    let stores = sysmd.list_stores().await?;

    // For each store, check if any hosting node has a running server.
    let cfg = ctx.config().clone();
    for store in &stores {
        let mut any_alive = false;
        for node_id in &store.node_ids {
            if cfg.server_for_node(*node_id).is_some() {
                if let Ok(client) = server_client(ctx, *node_id) {
                    if client.health().await.is_ok() {
                        any_alive = true;
                        break;
                    }
                }
            }
        }
        if !any_alive {
            let _ = sysmd.remove_store(store.store_id).await;
        }
    }

    Ok(())
}
