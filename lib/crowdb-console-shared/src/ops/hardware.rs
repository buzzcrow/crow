// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Hardware hierarchy operations: rack/node/disk-group/disk CRUD.
//!
//! Each function writes to the local TOML [`ConsoleConfig`] first (the
//! operator's intent), then best-effort syncs the change into group-0
//! sysdata via [`CrowdbSysmdClient`]. The sysdata sync is best-effort
//! because the cluster may not be initialized yet (e.g. the first rack
//! and node are added before `cluster init`).

use crowdb_protocol::common::{HwStatus, NodeValue, RackValue};

use crate::config::{NodeEntry, RackEntry};
use crate::error::Result;
use crate::ops::OpContext;

// ── rack ────────────────────────────────────────────────────────

/// Add a rack to the local config and group-0 sysdata.
///
/// # Errors
/// Returns [`Error::Conflict`] if the rack id already exists.
pub async fn add_rack(ctx: &OpContext, rack_id: u64, name: &str) -> Result<RackEntry> {
    let entry = RackEntry {
        id: rack_id,
        name: name.to_string(),
    };
    {
        let mut cfg = ctx.config_mut();
        cfg.add_rack(entry.clone())?;
    }
    // Best-effort sysdata sync.
    let value = RackValue {
        status: HwStatus::Up as i32,
        node_ids: Vec::new(),
    };
    let _ = ctx.sysmd().add_rack(rack_id, &value).await;
    Ok(entry)
}

/// Remove a rack from the local config and cascade-remove from sysdata.
///
/// # Errors
/// Returns [`Error::NotFound`] if the rack does not exist; [`Error::Conflict`]
/// if any node still references it.
pub async fn remove_rack(ctx: &OpContext, rack_id: u64) -> Result<()> {
    {
        let mut cfg = ctx.config_mut();
        cfg.remove_rack(rack_id)?;
    }
    let _ = ctx.sysmd().remove_rack_cascade(rack_id).await;
    Ok(())
}

/// List racks from the local config.
pub fn list_racks(ctx: &OpContext) -> Vec<RackEntry> {
    ctx.config().racks.clone()
}

// ── node ────────────────────────────────────────────────────────

/// Add a node to the local config and group-0 sysdata.
///
/// # Errors
/// Returns [`Error::Conflict`] if the node id already exists;
/// [`Error::Validation`] if the rack does not exist.
pub async fn add_node(ctx: &OpContext, entry: NodeEntry) -> Result<NodeEntry> {
    {
        let mut cfg = ctx.config_mut();
        cfg.add_node(entry.clone())?;
    }
    let value = NodeValue {
        status: HwStatus::Up as i32,
        last_used_dg_id: 0,
        disk_group_ids: Vec::new(),
        status_changed_at_ms: 0,
        temp_failure_since_ms: None,
    };
    let _ = ctx.sysmd().add_node(entry.rack_id, entry.id, &value).await;
    Ok(entry)
}

/// Remove a node from the local config and cascade-remove from sysdata.
///
/// # Errors
/// Returns [`Error::NotFound`] if the node does not exist; [`Error::Conflict`]
/// if a server is still deployed on the node.
pub async fn remove_node(ctx: &OpContext, node_id: u64) -> Result<()> {
    let rack_id = {
        let cfg = ctx.config();
        cfg.node(node_id).map(|n| n.rack_id)
    };
    {
        let mut cfg = ctx.config_mut();
        cfg.remove_node(node_id)?;
    }
    if let Some(rack_id) = rack_id {
        let _ = ctx.sysmd().remove_node_cascade(rack_id, node_id).await;
    }
    Ok(())
}

/// List nodes from the local config, optionally filtered by rack.
pub fn list_nodes(ctx: &OpContext, rack_id: Option<u64>) -> Vec<NodeEntry> {
    let cfg = ctx.config();
    match rack_id {
        Some(rid) => cfg.nodes.iter().filter(|n| n.rack_id == rid).cloned().collect(),
        None => cfg.nodes.clone(),
    }
}
