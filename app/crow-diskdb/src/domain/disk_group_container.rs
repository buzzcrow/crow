// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! `DdbDiskGroupContainer` — per-instance singleton managing all owned disk-groups.

use super::disk_group::DdbDiskGroup;
use crow_protocol::DiskGroupId;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use tracing::warn;

/// Per-instance singleton managing all owned disk-groups.
pub struct DdbDiskGroupContainer {
    disk_groups: RwLock<HashMap<DiskGroupId, Arc<DdbDiskGroup>>>,
    pub(crate) instance_id: u64,
    pub(crate) degraded: AtomicBool,
}

impl DdbDiskGroupContainer {
    pub fn new(instance_id: u64) -> Self {
        Self {
            disk_groups: RwLock::new(HashMap::new()),
            instance_id,
            degraded: AtomicBool::new(false),
        }
    }

    pub(crate) fn add_disk_group(&self, dg: Arc<DdbDiskGroup>) {
        let dg_id = dg.disk_group_id;
        self.disk_groups.write().unwrap().insert(dg_id, dg);
    }

    pub(crate) fn remove_disk_group(&self, dg_id: DiskGroupId) {
        self.disk_groups.write().unwrap().remove(&dg_id);
    }

    pub fn get_disk_group(&self, dg_id: DiskGroupId) -> Option<Arc<DdbDiskGroup>> {
        self.disk_groups.read().unwrap().get(&dg_id).cloned()
    }

    pub fn disk_group_ids(&self) -> Vec<DiskGroupId> {
        self.disk_groups.read().unwrap().keys().copied().collect()
    }

    pub(crate) fn enter_degraded_mode(&self) {
        let prev = self.degraded.swap(true, Ordering::SeqCst);
        if !prev {
            warn!("entering degraded mode");
        }
    }

    pub(crate) fn exit_degraded_mode(&self) {
        let prev = self.degraded.swap(false, Ordering::SeqCst);
        if prev {
            warn!("exiting degraded mode");
        }
    }

    #[allow(dead_code)]
    pub(crate) fn is_degraded(&self) -> bool {
        self.degraded.load(Ordering::SeqCst)
    }
}
