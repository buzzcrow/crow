// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_protocol::diskdb_fb::FBDiskdbRetCode;

use crate::model::disk_group_container::DdbDiskGroupContainer;

pub(super) fn validate(container: &DdbDiskGroupContainer) -> Result<(), (FBDiskdbRetCode, &'static str)> {
    if !container.lifecycle_phase().allows_mutating_rpcs() {
        return Err((FBDiskdbRetCode::Unavailable, "diskdb not ready"));
    }
    if container.is_degraded() {
        return Err((FBDiskdbRetCode::Degraded, "diskdb in degraded mode"));
    }
    Ok(())
}
