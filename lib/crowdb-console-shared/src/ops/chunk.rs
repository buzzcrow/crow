// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Chunk/diskdb operations: diskdb lifecycle, maintenance, chunkdb/diskio stubs.

use crate::error::{Error, Result};
use crate::ops::OpContext;

/// Deploy a diskdb instance on a node. Stub — not yet implemented.
///
/// # Errors
/// Always returns [`Error::NotImplemented`] until the chunk domain is wired.
pub fn diskdb_deploy(_ctx: &OpContext, node_id: u64) -> Result<()> {
    Err(Error::NotImplemented(format!("diskdb_deploy on node {node_id}")))
}
