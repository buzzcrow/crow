// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! KV-cluster logical plane: store/group/replica orchestration.
//!
//! Mirrors the web handlers' fan-out + rollback logic, but calls the
//! kv-server mgmt endpoints directly via [`ServerClient`] and reads
//! topology from group-0 sysdata via [`CrowdbSysmdClient`] instead of
//! the monitor cache.

use std::collections::HashSet;

use crowdb_protocol::mgmt::{
    AddGroupInitialRole, AddGroupRequest, AddStoreRequest, RemoteReplicaInfo, StepDownRequest,
};

use crate::clients::http::ServerClient;
use crate::config::ReplicaEntry;
use crate::error::{Error, Result};
use crate::ops::OpContext;

/// Build a [`ServerClient`] for a node's deployed kv-server.
fn server_client(ctx: &OpContext, node_id: u64) -> Result<ServerClient> {
    let url = ctx.node_mgmt_url(node_id)?;
    ServerClient::new(&url).map_err(|e| Error::UpstreamRpc {
        node_id: url,
        status: format!("client build: {e}"),
    })
}

/// Resolve the crowdb-rpc endpoint for a store on a node by calling
/// the node's `/topology` endpoint. Returns `None` if the store is not
/// hosted on the node or has no `listen_addr`.
async fn rpc_endpoint_for_store(ctx: &OpContext, node_id: u64, store_id: u64) -> Option<String> {
    let client = server_client(ctx, node_id).ok()?;
    let stores = client.topology().await.ok()?;
    for s in &stores {
        if s.store_id == store_id {
            if let Some(addr) = &s.listen_addr {
                return Some(strip_scheme(&remap_zero_host(addr)));
            }
        }
    }
    None
}

fn strip_scheme(s: &str) -> String {
    s.strip_prefix("http://")
        .or_else(|| s.strip_prefix("https://"))
        .unwrap_or(s)
        .to_string()
}

fn remap_zero_host(addr: &str) -> String {
    addr.strip_prefix("0.0.0.0:")
        .map_or_else(|| addr.to_string(), |port| format!("127.0.0.1:{port}"))
}

// ── store ───────────────────────────────────────────────────────

/// Create an empty store across the listed nodes. Fans out `add_store`
/// to each node, rolls back on partial failure, and records the store
/// in group-0 sysdata + the local config.
///
/// If `nodes` is empty, picks the first node with a deployed server.
///
/// # Errors
/// Returns an error if no nodes are available or any upstream RPC fails.
pub async fn add_store(ctx: &OpContext, store_id: u64, nodes: &[u64]) -> Result<Vec<u64>> {
    let mut target_nodes = if nodes.is_empty() {
        let cfg = ctx.config();
        let first = cfg
            .servers
            .iter()
            .find_map(|s| s.node_id)
            .ok_or_else(|| Error::Validation {
                field: "nodes".into(),
                message: "no nodes with deployed servers".into(),
            })?;
        vec![first]
    } else {
        nodes.to_vec()
    };

    let mut seen = HashSet::new();
    target_nodes.retain(|nid| seen.insert(*nid));

    let mut succeeded: Vec<u64> = Vec::new();
    for nid in &target_nodes {
        let client = server_client(ctx, *nid)?;
        client.health().await.map_err(|e| Error::NodeUnreachable {
            node_id: nid.to_string(),
            reason: e.to_string(),
        })?;
        let req = AddStoreRequest { store_id, port: None };
        match client.add_store(&req).await {
            Ok(_) => succeeded.push(*nid),
            Err(e) => {
                // Roll back successful creations.
                for ok_nid in &succeeded {
                    if let Ok(c) = server_client(ctx, *ok_nid) {
                        let _ = c.remove_store(store_id).await;
                    }
                }
                return Err(Error::UpstreamRpc {
                    node_id: nid.to_string(),
                    status: format!("store create failed: {e}"),
                });
            }
        }
    }

    // Record in group-0 sysdata + local config.
    let _ = ctx.sysmd().add_store(store_id, &succeeded).await;
    {
        let mut cfg = ctx.config_mut();
        cfg.record_store(store_id, succeeded.clone());
    }
    Ok(succeeded)
}

/// Remove a store from every hosting node. Idempotent on per-node 404.
///
/// # Errors
/// Returns [`Error::Validation`] if `store_id` is 0 (the system store).
pub async fn remove_store(ctx: &OpContext, store_id: u64) -> Result<()> {
    if store_id == 0 {
        return Err(Error::Validation {
            field: "store_id".into(),
            message: "store 0 is the system store; use cluster reset".into(),
        });
    }
    // Find hosting nodes from group-0 sysdata.
    let store = ctx.sysmd().get_store(store_id).await?;
    let node_ids = store.map(|s| s.node_ids).unwrap_or_default();
    for nid in &node_ids {
        if let Ok(client) = server_client(ctx, *nid) {
            let _ = client.remove_store(store_id).await;
        }
    }
    let _ = ctx.sysmd().remove_store(store_id).await;
    {
        let mut cfg = ctx.config_mut();
        cfg.remove_store_record(store_id);
    }
    Ok(())
}

/// List stores from group-0 sysdata.
///
/// # Errors
/// Returns an error if the group-0 sysdata read fails.
pub async fn list_stores(ctx: &OpContext) -> Result<Vec<crowdb_protocol::common::StoreValue>> {
    ctx.sysmd().list_stores().await.map_err(Into::into)
}

// ── group ───────────────────────────────────────────────────────

/// Create a Paxos group across the listed nodes. Creates a local
/// `PxGroup` on each node, wires remote-replica entries, and records
/// the group in group-0 sysdata + the local config. Rolls back on
/// partial failure.
///
/// # Errors
/// Returns an error if `nodes` is empty or any upstream RPC fails.
pub async fn add_group(
    ctx: &OpContext,
    store_id: u64,
    group_id: u64,
    replica_id: u64,
    nodes: &[u64],
) -> Result<()> {
    if nodes.is_empty() {
        return Err(Error::Validation {
            field: "nodes".into(),
            message: "nodes list must not be empty".into(),
        });
    }

    // Phase 1: create the group on each node.
    let mut succeeded: Vec<(u64, u64)> = Vec::new(); // (node_id, replica_id)
    for (i, nid) in nodes.iter().enumerate() {
        let client = server_client(ctx, *nid)?;
        let rid = replica_id + i as u64;
        let req = AddGroupRequest {
            group_id,
            replica_id: rid,
            initial_role: Some(if i == 0 {
                AddGroupInitialRole::Leader
            } else {
                AddGroupInitialRole::Follower
            }),
            start_election: Some(nodes.len() <= 1),
        };
        match client.add_group(store_id, &req).await {
            Ok(()) => succeeded.push((*nid, rid)),
            Err(e) => {
                for (ok_nid, _) in &succeeded {
                    if let Ok(c) = server_client(ctx, *ok_nid) {
                        let _ = c.remove_group(store_id, group_id).await;
                    }
                }
                return Err(Error::UpstreamRpc {
                    node_id: nid.to_string(),
                    status: format!("group create failed: {e}"),
                });
            }
        }
    }

    // Phase 2: wire remote replicas for multi-node.
    if succeeded.len() > 1 {
        for (i, (nid, _rid)) in succeeded.iter().enumerate() {
            let Ok(client) = server_client(ctx, *nid) else {
                continue;
            };
            let mut remotes: Vec<RemoteReplicaInfo> = Vec::new();
            for (j, (peer_nid, peer_rid)) in succeeded.iter().enumerate() {
                if j == i {
                    continue;
                }
                if let Some(ep) = rpc_endpoint_for_store(ctx, *peer_nid, store_id).await {
                    remotes.push(RemoteReplicaInfo {
                        replica_id: *peer_rid,
                        endpoint: ep,
                        voting: true,
                    });
                }
            }
            if !remotes.is_empty() {
                let _ = client.add_remote_replicas(store_id, group_id, &remotes).await;
            }
        }
    }

    // Record in group-0 sysdata + local config.
    let _ = ctx.sysmd().add_group(store_id, group_id).await;
    let replicas: Vec<ReplicaEntry> = succeeded
        .iter()
        .map(|(node_id, replica_id)| ReplicaEntry {
            replica_id: *replica_id,
            node_id: *node_id,
        })
        .collect();
    {
        let mut cfg = ctx.config_mut();
        cfg.record_group(store_id, group_id, replicas);
        for (node_id, _) in &succeeded {
            cfg.ensure_store_node(store_id, *node_id);
        }
    }
    Ok(())
}

/// Remove a Paxos group from every hosting node.
///
/// # Errors
/// Returns [`Error::Validation`] if removing group 0 in store 0.
pub async fn remove_group(ctx: &OpContext, store_id: u64, group_id: u64) -> Result<()> {
    if store_id == 0 && group_id == 0 {
        return Err(Error::Validation {
            field: "group_id".into(),
            message: "group 0 in store 0 is the system group; use cluster reset".into(),
        });
    }
    // Find hosting nodes from group-0 sysdata.
    let replicas = ctx.sysmd().list_replicas_in_group(store_id, group_id).await?;
    let node_ids: Vec<u64> = replicas.iter().map(|r| r.node_id).collect();
    for nid in &node_ids {
        if let Ok(client) = server_client(ctx, *nid) {
            let _ = client.remove_group(store_id, group_id).await;
        }
    }
    let _ = ctx.sysmd().remove_group(store_id, group_id).await;
    {
        let mut cfg = ctx.config_mut();
        cfg.remove_group_record(store_id, group_id);
    }
    Ok(())
}

/// List groups in a store from group-0 sysdata.
///
/// # Errors
/// Returns an error if the group-0 sysdata read fails.
pub async fn list_groups(ctx: &OpContext, store_id: u64) -> Result<Vec<crowdb_protocol::common::GroupValue>> {
    ctx.sysmd()
        .list_groups_in_store(store_id)
        .await
        .map_err(Into::into)
}

// ── replica ─────────────────────────────────────────────────────

/// Roll back a partially-wired replica: deregister from `wired_peers`,
/// then delete the local group on `target_node`.
async fn rollback_replica(
    ctx: &OpContext,
    store_id: u64,
    group_id: u64,
    new_rid: u64,
    wired_peers: &[u64],
    target_node: u64,
) {
    for wp in wired_peers {
        if let Ok(c) = server_client(ctx, *wp) {
            let _ = c.remove_remote_replica(store_id, group_id, new_rid).await;
        }
    }
    if let Ok(c) = server_client(ctx, target_node) {
        let _ = c.remove_group(store_id, group_id).await;
    }
}

/// Add a replica to an existing group on a target node. Creates a local
/// `PxGroup` on the target, registers the new replica as a remote on
/// every existing peer, and registers every peer as a remote on the new
/// replica. Rolls back on partial failure.
///
/// # Errors
/// Returns an error if the group or node is not found, or any RPC fails.
pub async fn add_replica(
    ctx: &OpContext,
    store_id: u64,
    group_id: u64,
    node_id: u64,
    replica_id: Option<u64>,
) -> Result<u64> {
    // Resolve existing replicas from group-0 sysdata.
    let existing = ctx.sysmd().list_replicas_in_group(store_id, group_id).await?;
    if existing.is_empty() {
        return Err(Error::NotFound {
            kind: "group".into(),
            id: format!("{store_id}/{group_id}"),
        });
    }
    let new_rid = replica_id.unwrap_or_else(|| existing.iter().map(|r| r.replica_id).max().unwrap_or(0) + 1);
    if existing.iter().any(|r| r.replica_id == new_rid) {
        return Err(Error::Conflict {
            kind: "replica".into(),
            id: new_rid.to_string(),
        });
    }

    // Step 1: create local PxGroup on the target node.
    let client = server_client(ctx, node_id)?;
    let req = AddGroupRequest {
        group_id,
        replica_id: new_rid,
        initial_role: Some(AddGroupInitialRole::Follower),
        start_election: Some(false),
    };
    client
        .add_group(store_id, &req)
        .await
        .map_err(|e| Error::UpstreamRpc {
            node_id: node_id.to_string(),
            status: format!("create local group: {e}"),
        })?;

    // Step 2: Register the new replica as a remote on every existing peer.
    let Some(new_endpoint) = rpc_endpoint_for_store(ctx, node_id, store_id).await else {
        return Err(Error::NodeUnreachable {
            node_id: node_id.to_string(),
            reason: "could not determine crowdb-rpc endpoint".into(),
        });
    };
    let new_remote = RemoteReplicaInfo {
        replica_id: new_rid,
        endpoint: new_endpoint,
        voting: true,
    };
    let mut wired_peers: Vec<u64> = Vec::new();
    for existing_replica in &existing {
        if let Ok(peer_client) = server_client(ctx, existing_replica.node_id) {
            if let Err(e) = peer_client
                .add_remote_replicas(store_id, group_id, std::slice::from_ref(&new_remote))
                .await
            {
                rollback_replica(ctx, store_id, group_id, new_rid, &wired_peers, node_id).await;
                return Err(Error::UpstreamRpc {
                    node_id: existing_replica.node_id.to_string(),
                    status: format!("wire new replica on peer: {e}"),
                });
            }
            wired_peers.push(existing_replica.node_id);
        }
    }

    // Step 3: Register every existing peer as a remote on the new replica.
    let mut existing_remotes: Vec<RemoteReplicaInfo> = Vec::new();
    for r in &existing {
        if let Some(ep) = rpc_endpoint_for_store(ctx, r.node_id, store_id).await {
            existing_remotes.push(RemoteReplicaInfo {
                replica_id: r.replica_id,
                endpoint: ep,
                voting: true,
            });
        }
    }
    if !existing_remotes.is_empty() {
        if let Err(e) = client
            .add_remote_replicas(store_id, group_id, &existing_remotes)
            .await
        {
            rollback_replica(ctx, store_id, group_id, new_rid, &wired_peers, node_id).await;
            return Err(Error::UpstreamRpc {
                node_id: node_id.to_string(),
                status: format!("wire existing peers on new replica: {e}"),
            });
        }
    }

    // Record in group-0 sysdata + local config.
    record_replica(ctx, store_id, group_id, new_rid, node_id).await;
    Ok(new_rid)
}

/// Record a new replica in group-0 sysdata + the local config.
async fn record_replica(ctx: &OpContext, store_id: u64, group_id: u64, replica_id: u64, node_id: u64) {
    let value = crowdb_protocol::common::ReplicaValue {
        store_id,
        group_id,
        replica_id,
        node_id,
        role: String::new(),
        voting: true,
        endpoint: String::new(),
    };
    let _ = ctx.sysmd().add_replica(&value).await;
    {
        let mut cfg = ctx.config_mut();
        cfg.ensure_store_node(store_id, node_id);
        cfg.add_group_replica(store_id, group_id, ReplicaEntry { replica_id, node_id });
    }
}

/// Remove a replica: deregister from peers, delete local group, step
/// down if it was the leader.
///
/// # Errors
/// Returns [`Error::NotFound`] if the replica does not exist.
pub async fn remove_replica(ctx: &OpContext, store_id: u64, group_id: u64, replica_id: u64) -> Result<()> {
    let replicas = ctx.sysmd().list_replicas_in_group(store_id, group_id).await?;
    let target = replicas
        .iter()
        .find(|r| r.replica_id == replica_id)
        .ok_or_else(|| Error::NotFound {
            kind: "replica".into(),
            id: replica_id.to_string(),
        })?;
    let target_node = target.node_id;

    // Step 0: step down if this replica is the leader (best-effort).
    if let Ok(client) = server_client(ctx, target_node) {
        let _ = client
            .step_down(
                store_id,
                group_id,
                &StepDownRequest {
                    reason: format!("replica {replica_id} removal"),
                },
            )
            .await;
    }

    // Step 1: Deregister from every peer.
    for peer in &replicas {
        if peer.replica_id == replica_id {
            continue;
        }
        if let Ok(client) = server_client(ctx, peer.node_id) {
            let _ = client.remove_remote_replica(store_id, group_id, replica_id).await;
        }
    }

    // Step 2: Delete the local group on the target node.
    if let Ok(client) = server_client(ctx, target_node) {
        let _ = client.remove_group(store_id, group_id).await;
    }

    // Record in group-0 sysdata + local config.
    let _ = ctx.sysmd().remove_replica(store_id, group_id, replica_id).await;
    {
        let mut cfg = ctx.config_mut();
        cfg.remove_group_replica(store_id, group_id, replica_id);
    }
    Ok(())
}

/// List replicas in a group from group-0 sysdata.
///
/// # Errors
/// Returns an error if the group-0 sysdata read fails.
pub async fn list_replicas(
    ctx: &OpContext,
    store_id: u64,
    group_id: u64,
) -> Result<Vec<crowdb_protocol::common::ReplicaValue>> {
    ctx.sysmd()
        .list_replicas_in_group(store_id, group_id)
        .await
        .map_err(Into::into)
}
