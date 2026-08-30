// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Cluster-level operations: status, topology, init, reset, clean.

use crate::error::Result;
use crate::ops::OpContext;

/// Initialize the cluster by bootstrapping group 0 on the listed nodes.
///
/// # Errors
/// Returns an error if any node is unreachable or `system/init` fails.
pub async fn init(_ctx: &OpContext, nodes: &[u64]) -> Result<()> {
    let _ = nodes;
    todo!("Phase 1f: implement cluster init")
}
