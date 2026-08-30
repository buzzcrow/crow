// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `crowdb-kv-server` lifecycle: deploy, restart, stop, delete, list.
//!
//! Each function mutates the local TOML [`ConsoleConfig`] (adding /
//! removing [`ServerEntry`] records) and spawns / stops the server
//! process via [`lifecycle`] (local-fork) or [`ssh`] (remote SSH).

use std::path::PathBuf;

use crowdb_protocol::NodeId;

use crate::config::{ServerEntry, ServiceType};
use crate::error::{Error, Result};
use crate::lifecycle::{self, DeployRequest, DeployedServer};
use crate::ops::OpContext;

/// Deploy a `crowdb-kv-server` on a node.
///
/// # Errors
/// Returns [`Error::NotFound`] if the node does not exist;
/// [`Error::Conflict`] if a server is already deployed on the node;
/// [`Error::NodeUnreachable`] if the deploy fails.
pub async fn deploy(
    ctx: &OpContext,
    node_id: NodeId,
    rest_port: u16,
    rpc_port: u16,
    binary: Option<String>,
) -> Result<DeployedServer> {
    let node = ctx.node_entry(node_id)?;

    // Check for existing deployment.
    if ctx.config().server_for_node(node_id).is_some() {
        return Err(Error::Conflict {
            kind: "server".into(),
            id: format!("node {node_id} already hosts a deployed server"),
        });
    }

    let req = DeployRequest {
        server_id: node_id.to_string(),
        rest_port,
        rpc_port,
        binary: binary.as_ref().map(PathBuf::from),
        ..Default::default()
    };

    let deployed = if node.ssh_enabled() {
        let server_bin = binary.clone().unwrap_or_else(|| {
            std::env::var("CROWDB_KV_SERVER_BIN").unwrap_or_else(|_| "crowdb-kv-server".into())
        });
        crate::ssh::deploy_via_ssh(&req, &node, &server_bin)
            .await
            .map_err(|e| Error::NodeUnreachable {
                node_id: node_id.to_string(),
                reason: format!("ssh deploy: {e}"),
            })?
    } else {
        lifecycle::deploy_local(&req, &node)
            .await
            .map_err(|e| Error::NodeUnreachable {
                node_id: node_id.to_string(),
                reason: format!("local deploy: {e}"),
            })?
    };

    let entry = ServerEntry {
        id: node_id.to_string(),
        url: deployed.mgmt_url.clone(),
        node_id: Some(node_id),
        rpc_url: Some(deployed.rpc_url.clone()),
        rest_port: Some(rest_port),
        rpc_port: Some(rpc_port),
        auto_start: true,
        binary,
        election_profile: None,
        pid: Some(deployed.pid),
        service_type: ServiceType::Kv,
        rpc_workers: None,
        no_fsync: false,
    };
    {
        let mut cfg = ctx.config_mut();
        cfg.add_server(entry).map_err(|e| Error::Config(e.to_string()))?;
    }
    Ok(deployed)
}

/// Stop the `crowdb-kv-server` on a node (keep the deployment record).
///
/// # Errors
/// Returns [`Error::NotFound`] if no server is deployed on the node.
pub async fn stop(ctx: &OpContext, node_id: NodeId) -> Result<bool> {
    let node = ctx.node_entry(node_id)?;
    let entry = ctx.server_for_node(node_id)?;
    let pid = entry.pid.ok_or_else(|| Error::NotFound {
        kind: "server".into(),
        id: format!("node {node_id} has no tracked pid"),
    })?;

    let sent = if node.ssh_enabled() {
        crate::ssh::stop_via_ssh(&node, pid)
            .await
            .map_err(|e| Error::NodeUnreachable {
                node_id: node_id.to_string(),
                reason: format!("ssh stop: {e}"),
            })?
    } else {
        tokio::task::spawn_blocking(move || lifecycle::stop_pid(pid))
            .await
            .map_err(|e| Error::Io(std::io::Error::other(e)))??
    };

    // Clear the PID in the config.
    {
        let mut cfg = ctx.config_mut();
        if let Some(s) = cfg.server_for_node_mut(node_id) {
            s.pid = None;
        }
    }
    Ok(sent)
}

/// Restart the `crowdb-kv-server` on a node: stop (if running) and
/// re-deploy on the same recorded ports.
///
/// # Errors
/// Returns [`Error::NotFound`] if no server is deployed on the node.
pub async fn restart(ctx: &OpContext, node_id: NodeId) -> Result<DeployedServer> {
    let node = ctx.node_entry(node_id)?;
    let entry = ctx.server_for_node(node_id)?;

    let rest_port = entry
        .rest_port
        .ok_or_else(|| Error::Config(format!("server entry for node {node_id} has no rest_port")))?;
    let rpc_port = entry
        .rpc_port
        .ok_or_else(|| Error::Config(format!("server entry for node {node_id} has no rpc_port")))?;

    // Stop the existing process if a PID is tracked.
    if let Some(pid) = entry.pid {
        if node.ssh_enabled() {
            let _ = crate::ssh::stop_via_ssh(&node, pid).await;
        } else {
            let _ = tokio::task::spawn_blocking(move || lifecycle::stop_pid(pid)).await;
        }
    }

    let req = DeployRequest {
        server_id: node_id.to_string(),
        rest_port,
        rpc_port,
        binary: entry.binary.as_ref().map(PathBuf::from),
        election_profile: entry.election_profile.clone(),
        rpc_workers: entry.rpc_workers,
        no_fsync: entry.no_fsync,
        ..Default::default()
    };

    let deployed = if node.ssh_enabled() {
        let server_bin = std::env::var("CROWDB_KV_SERVER_BIN").unwrap_or_else(|_| "crowdb-kv-server".into());
        crate::ssh::deploy_via_ssh(&req, &node, &server_bin)
            .await
            .map_err(|e| Error::NodeUnreachable {
                node_id: node_id.to_string(),
                reason: format!("ssh redeploy: {e}"),
            })?
    } else {
        lifecycle::deploy_local(&req, &node)
            .await
            .map_err(|e| Error::NodeUnreachable {
                node_id: node_id.to_string(),
                reason: format!("local redeploy: {e}"),
            })?
    };

    // Update the server entry with the new PID + URLs.
    {
        let mut cfg = ctx.config_mut();
        let _ = cfg.remove_server_for_node(node_id);
        let new_entry = ServerEntry {
            id: node_id.to_string(),
            url: deployed.mgmt_url.clone(),
            node_id: Some(node_id),
            rpc_url: Some(deployed.rpc_url.clone()),
            rest_port: Some(rest_port),
            rpc_port: Some(rpc_port),
            auto_start: entry.auto_start,
            binary: entry.binary.clone(),
            election_profile: entry.election_profile.clone(),
            pid: Some(deployed.pid),
            service_type: entry.service_type,
            rpc_workers: entry.rpc_workers,
            no_fsync: entry.no_fsync,
        };
        cfg.add_server(new_entry)
            .map_err(|e| Error::Config(e.to_string()))?;
    }
    Ok(deployed)
}

/// Delete the server deployment on a node: stop the process and remove
/// the [`ServerEntry`] from the config.
///
/// # Errors
/// Returns [`Error::NotFound`] if no server is deployed on the node.
pub async fn delete(ctx: &OpContext, node_id: NodeId) -> Result<()> {
    let node = ctx.node_entry(node_id)?;
    let entry = ctx.server_for_node(node_id)?;

    if let Some(pid) = entry.pid {
        if node.ssh_enabled() {
            let _ = crate::ssh::stop_via_ssh(&node, pid).await;
        } else {
            let _ = tokio::task::spawn_blocking(move || lifecycle::stop_pid(pid)).await;
        }
    }

    {
        let mut cfg = ctx.config_mut();
        let _ = cfg.remove_server_for_node(node_id);
        cfg.purge_node_topology(node_id);
    }
    Ok(())
}

/// List all deployed servers from the local config.
pub fn list(ctx: &OpContext) -> Vec<ServerEntry> {
    ctx.config().servers.clone()
}
