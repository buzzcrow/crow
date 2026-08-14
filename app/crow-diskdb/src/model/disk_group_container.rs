// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! `DdbDiskGroupContainer` — per-instance singleton managing all owned disk-groups.

use super::disk_group::DdbDiskGroup;
use crate::liveness::lifecycle::LifecycleState;
use crow_protocol::DiskGroupId;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::warn;

/// Per-instance singleton managing all owned disk-groups.
pub struct DdbDiskGroupContainer {
    disk_groups: RwLock<HashMap<DiskGroupId, Arc<DdbDiskGroup>>>,
    pub(crate) instance_id: u64,
    pub(crate) degraded: AtomicBool,
    pub(crate) lifecycle: LifecycleState,
    /// Epoch millis of the last successful keepalive sync (R74
    /// `last_sync_age_secs` gauge). Initialized to construction time
    /// so the age starts at 0.
    last_sync_at_ms: AtomicU64,
}

impl DdbDiskGroupContainer {
    pub fn new(instance_id: u64) -> Self {
        Self {
            disk_groups: RwLock::new(HashMap::new()),
            instance_id,
            degraded: AtomicBool::new(false),
            lifecycle: LifecycleState::new(),
            last_sync_at_ms: AtomicU64::new(now_ms()),
        }
    }

    pub(crate) fn add_disk_group(&self, dg: Arc<DdbDiskGroup>) {
        let dg_id = dg.disk_group_id;
        self.disk_groups.write().unwrap().insert(dg_id, dg);
    }

    /// Replace an existing disk-group with a recovered one (same
    /// `disk_group_id`). Used by startup recovery to swap in the
    /// fully-reconstructed disk-group.
    pub fn replace_disk_group(&self, dg: Arc<DdbDiskGroup>) {
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

    pub fn enter_degraded_mode(&self) {
        let prev = self.degraded.swap(true, Ordering::SeqCst);
        if !prev {
            warn!("entering degraded mode");
        }
    }

    pub fn exit_degraded_mode(&self) {
        let prev = self.degraded.swap(false, Ordering::SeqCst);
        if prev {
            warn!("exiting degraded mode");
        }
    }

    /// Whether the instance is in degraded mode (missed heartbeats).
    pub fn is_degraded(&self) -> bool {
        self.degraded.load(Ordering::SeqCst)
    }

    /// Record a successful keepalive sync (called by the keepalive
    /// loop on each successful tick). Updates `last_sync_at_ms`.
    pub fn record_sync_success(&self) {
        self.last_sync_at_ms.store(now_ms(), Ordering::Release);
    }

    /// Seconds since the last successful sync (R74 `last_sync_age_secs`).
    #[must_use]
    pub fn last_sync_age_secs(&self) -> u64 {
        let last = self.last_sync_at_ms.load(Ordering::Acquire);
        let now = now_ms();
        (now.saturating_sub(last)) / 1000
    }

    /// Number of owned disk-groups (R74 `owned_disk_group_count` gauge).
    #[must_use]
    pub fn disk_group_count(&self) -> usize {
        self.disk_groups.read().unwrap().len()
    }

    /// Current startup phase.
    pub fn lifecycle_phase(&self) -> crate::liveness::lifecycle::StartupPhase {
        self.lifecycle.get()
    }

    /// Set the startup phase.
    pub fn set_lifecycle_phase(&self, phase: crate::liveness::lifecycle::StartupPhase) {
        self.lifecycle.set(phase);
    }
}

/// Current epoch time in milliseconds.
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis().try_into().unwrap_or(u64::MAX))
}
