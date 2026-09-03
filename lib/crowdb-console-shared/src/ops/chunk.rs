// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Chunk/diskdb operations: diskdb lifecycle, maintenance, chunkdb/diskio stubs.

use std::sync::Arc;

use crowdb_kv_client::ServiceDiscoveryClient;

use crate::error::{Error, Result};
use crate::ops::OpContext;

/// Discover a living diskdb instance's RPC endpoint via the group-0
/// service registry. If `explicit_endpoint` is `Some`, return it
/// directly (override / test exception — no discovery query). If
/// `None`, use the context's `ServiceDiscoveryClient` to find a
/// living diskdb instance (round-robin selection).
///
/// # Errors
/// - [`Error::NotImplemented`] if discovery is not configured and no
///   explicit endpoint was provided.
/// - [`Error::KvClient`] if the group-0 registry is unreachable.
/// - [`Error::KvClient`] with `NoLivingInstances` if no living diskdb
///   instances are registered.
pub async fn discover_diskdb_endpoint(ctx: &OpContext, explicit_endpoint: Option<&str>) -> Result<String> {
    if let Some(ep) = explicit_endpoint {
        return Ok(ep.to_string());
    }
    let discovery: &Arc<ServiceDiscoveryClient> = ctx.discovery_or_error()?;
    let instance = discovery.discover_one("diskdb").await.map_err(Error::KvClient)?;
    Ok(instance.rpc_endpoint)
}

/// Discover all living diskdb instances via the group-0 service
/// registry. Returns a vector of `(instance_id, rpc_endpoint)` pairs.
///
/// # Errors
/// - [`Error::NotImplemented`] if discovery is not configured.
/// - [`Error::KvClient`] if the group-0 registry is unreachable.
pub async fn discover_all_diskdb_endpoints(ctx: &OpContext) -> Result<Vec<(u64, String)>> {
    let discovery = ctx.discovery_or_error()?;
    let instances = discovery.discover_all("diskdb").await.map_err(Error::KvClient)?;
    Ok(instances
        .into_iter()
        .map(|(id, v)| (id, v.rpc_endpoint))
        .collect())
}

/// Deploy a diskdb instance on a node. Stub — not yet implemented.
/// The actual deploy logic is in `app/crowdb-web/src/diskdb_lifecycle.rs`
/// (web-only); this ops helper will be wired when the CLI gains
/// diskdb deploy capability.
///
/// # Errors
/// Always returns [`Error::NotImplemented`] until the chunk domain is wired.
pub fn diskdb_deploy(_ctx: &OpContext, node_id: u64) -> Result<()> {
    Err(Error::NotImplemented(format!("diskdb_deploy on node {node_id}")))
}
