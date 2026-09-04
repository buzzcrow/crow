// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Least-loaded immutable disk-group owner selection.

use crowdb_protocol::sysdata::DiskdbOwnerEntry;

#[must_use]
pub fn pick_least_loaded_instance(instance_ids: &[u64], owners: &[DiskdbOwnerEntry]) -> Option<u64> {
    if instance_ids.is_empty() {
        return None;
    }
    let mut counts = std::collections::HashMap::<u64, usize>::new();
    for owner in owners {
        *counts.entry(owner.instance_id).or_default() += 1;
    }
    instance_ids
        .iter()
        .map(|id| (*id, counts.get(id).copied().unwrap_or(0)))
        .min_by(|left, right| left.1.cmp(&right.1).then(left.0.cmp(&right.0)))
        .map(|(id, _)| id)
}
