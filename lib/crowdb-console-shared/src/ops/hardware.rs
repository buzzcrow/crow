// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Hardware hierarchy operations: rack/node/disk-group/disk CRUD.
//!
//! Each function writes to group-0 sysdata via [`CrowdbSysmdClient`] and
//! mirrors the change into the local TOML [`ConsoleConfig`] so the
//! console can resolve nodes/servers during bootstrap.

use crate::error::Result;
use crate::ops::OpContext;

/// Add a rack to group-0 sysdata and the local config.
///
/// # Errors
/// Returns an error if the sysdata write or config update fails.
pub async fn add_rack(_ctx: &OpContext, rack_id: u64, name: &str) -> Result<()> {
    let _ = (rack_id, name);
    todo!("Phase 1b: implement add_rack")
}
