// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! `KeepAlive` — keep-alive + periodic hardware sync from group 0.
//!
//! Each tick: heartbeat, read ownership map, read bind map, read
//! member disks per owned disk-group, reconcile in-memory state
//! (disk-add init, status changes, removals).

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, RwLock};

use crow_common::metrics::Counter;
use crow_kv_client::{HardwareClient, ServiceRegistryClient};
use crow_protocol::common::{DiskGroupUsageSummary, DiskId, HwStatus};
use crow_protocol::diskdb::rpc::DiskValue;
use crow_protocol::DiskIdExt;
use crow_protocol::{DiskGroupId, NodeId, RackId};
use tracing::{info, warn};

use crate::bg_task::{BackgroundTask, BgCtx, CycleFut, Trigger};
use crate::ddb_config::KeepAliveConfig;
use crate::ddb_kv_client::DdbKvClient;
use crate::liveness::state_machine::HwStateMachine;
use crate::metrics::{DiskMetrics, DiskdbMetrics};
use crate::model::disk::DdbDisk;
use crate::model::disk_group::DdbDiskGroup;
use crate::model::disk_group_container::DdbDiskGroupContainer;
use crate::model::zone::DdbZone;
use crate::recovery::disk_recovery::{recover_disk_to_up, RecoveryScanTask};

/// Elapsed millis as u64 (saturating cast from u128).
fn elapsed_ms(start: std::time::Instant) -> u64 {
    start.elapsed().as_millis().try_into().unwrap_or(u64::MAX)
}

/// Elapsed nanos as u64 (saturating cast from u128).
fn elapsed_ns(start: std::time::Instant) -> u64 {
    start.elapsed().as_nanos().try_into().unwrap_or(u64::MAX)
}

/// Outcome of one sync tick.
#[derive(Debug, Default, Clone)]
pub struct KeepAliveOutcome {
    pub groups_added: usize,
    pub groups_removed: usize,
    pub disks_added: usize,
    pub disks_removed: usize,
    pub status_changes: usize,
    pub sync_duration_ms: u64,
}

/// Handle to a running per-disk recovery scan task. The cancel flag
/// is set on `HwStatus::Up` recovery; the join handle is awaited on
/// shutdown.
struct RecoveryScanHandle {
    cancel: Arc<AtomicBool>,
    join: tokio::task::JoinHandle<()>,
}

/// Background sync loop: keep-alive + hardware read + disk-add init.
pub struct KeepAlive {
    hw: HardwareClient,
    svc: ServiceRegistryClient,
    container: Arc<DdbDiskGroupContainer>,
    config: KeepAliveConfig,
    status_machine: HwStateMachine,
    missed_count: AtomicU32,
    /// Optional `DdbKvClient` for writing baseline `ZoneValue`
    /// records during disk-add init. When `None`, disk-add init
    /// skips the baseline write (test mode).
    kv: Option<DdbKvClient>,
    /// Optional CAS retry counter handle, attached to each `Zone`
    /// during disk-add init via `Zone::with_cas_retry_metric`.
    cas_retry_metric: Option<Arc<Counter>>,
    /// Optional shared config handle for live-apply of the timer
    /// interval. When set, `trigger()` returns `TimerFn` reading
    /// `heartbeat.interval_secs` from this handle each tick; when
    /// `None`, falls back to the fixed `config.interval` snapshot.
    config_handle: Option<Arc<arc_swap::ArcSwap<crate::ddb_config::DdbConfig>>>,
    /// gRPC endpoint to register with the service registry (R74
    /// keepalive piggyback). When empty, passes `""` (test mode).
    grpc_endpoint: String,
    /// Optional metrics handle for sync latency/success/failure
    /// observations (R74 §11).
    metrics: Option<DiskdbMetrics>,
    /// Optional sync trigger notify handle. When set, the keepalive
    /// uses `Trigger::TimerOrEvent` — waking on either the timer
    /// (safety-net polling) or the notify (woken by the `NotifyHandler`
    /// on a `WatchNotify` frame).
    sync_trigger: Option<Arc<tokio::sync::Notify>>,
    /// Per-disk consecutive miss counts (R76 Missing→Bad confirmation).
    /// Incremented when a disk is absent from sync; reset on
    /// rediscovery. When count >= `miss_threshold`, the disk
    /// transitions `Missing → Bad` and a recovery scan starts.
    disk_miss_counts: RwLock<HashMap<DiskId, u32>>,
    /// Running per-disk recovery scan tasks (R76). Keyed by `DiskId`;
    /// removed when the task completes or is cancelled on recovery.
    recovery_scans: RwLock<HashMap<DiskId, RecoveryScanHandle>>,
}

impl KeepAlive {
    pub fn new(
        hw: HardwareClient,
        svc: ServiceRegistryClient,
        container: Arc<DdbDiskGroupContainer>,
        config: KeepAliveConfig,
    ) -> Self {
        let status_machine = HwStateMachine::new(config.temp_failure_timeout_secs);
        Self {
            hw,
            svc,
            container,
            config,
            status_machine,
            missed_count: AtomicU32::new(0),
            kv: None,
            cas_retry_metric: None,
            config_handle: None,
            grpc_endpoint: String::new(),
            metrics: None,
            sync_trigger: None,
            disk_miss_counts: RwLock::new(HashMap::new()),
            recovery_scans: RwLock::new(HashMap::new()),
        }
    }

    /// Attach a `DdbKvClient` for disk-add init baseline writes.
    #[must_use]
    pub fn with_ddb_kv_client(mut self, kv: DdbKvClient) -> Self {
        self.kv = Some(kv);
        self
    }

    /// Attach a CAS retry counter handle for `Zone::with_cas_retry_metric`.
    #[must_use]
    pub fn with_cas_retry_metric(mut self, counter: Arc<Counter>) -> Self {
        self.cas_retry_metric = Some(counter);
        self
    }

    /// Attach a shared config handle for live-apply of the timer
    /// interval. When set, the keep-alive tick interval is read from
    /// the handle each tick, so config reloads take effect without
    /// restart.
    #[must_use]
    pub fn with_config_handle(
        mut self,
        handle: Arc<arc_swap::ArcSwap<crate::ddb_config::DdbConfig>>,
    ) -> Self {
        self.config_handle = Some(handle);
        self
    }

    /// Attach the gRPC endpoint to register with the service registry
    /// (R74 keepalive piggyback). When set, `heartbeat` passes this
    /// endpoint + per-disk-group usage summaries to
    /// `heartbeat_diskdb`.
    #[must_use]
    pub fn with_grpc_endpoint(mut self, endpoint: String) -> Self {
        self.grpc_endpoint = endpoint;
        self
    }

    /// Attach a metrics handle for sync latency/success/failure
    /// observations (R74 §11).
    #[must_use]
    pub fn with_metrics(mut self, metrics: DiskdbMetrics) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Attach a sync trigger notify handle. When set, the keepalive
    /// uses `Trigger::TimerOrEvent` — waking on either the timer
    /// (safety-net polling) or the notify (woken by the `NotifyHandler`
    /// on a `WatchNotify` frame).
    #[must_use]
    pub fn with_sync_trigger(mut self, notify: Arc<tokio::sync::Notify>) -> Self {
        self.sync_trigger = Some(notify);
        self
    }

    /// Wake the keepalive sync loop immediately (called by the
    /// `NotifyHandler` on each `WatchNotify` frame). No-op if no sync
    /// trigger is set.
    pub fn trigger_now(&self) {
        if let Some(ref notify) = self.sync_trigger {
            notify.notify_one();
        }
    }

    /// Cancel + await all running recovery scan tasks. Called on
    /// shutdown to ensure clean task termination.
    pub async fn shutdown_recovery_scans(&self) {
        let handles: Vec<RecoveryScanHandle> = {
            let mut scans = self.recovery_scans.write().unwrap();
            scans.drain().map(|(_, h)| h).collect()
        };
        for handle in handles {
            handle.cancel.store(true, Ordering::Release);
            let _ = handle.join.await;
        }
    }

    /// Run one sync tick — a thin orchestrator calling the four
    /// concerns in order: heartbeat, observe ownership, observe
    /// disks. Returns the aggregate outcome.
    pub async fn tick(&self) -> KeepAliveOutcome {
        let start = std::time::Instant::now();

        // a. Keep-alive heartbeat.
        if !self.heartbeat().await {
            if let Some(m) = &self.metrics {
                m.sync_failure_total.inc();
            }
            return KeepAliveOutcome {
                sync_duration_ms: elapsed_ms(start),
                ..Default::default()
            };
        }

        // b+c. Observe ownership (owner map + bind map).
        let Some((owned, groups_added, groups_removed)) = self.observe_ownership().await else {
            if let Some(m) = &self.metrics {
                m.sync_failure_total.inc();
            }
            return KeepAliveOutcome {
                sync_duration_ms: elapsed_ms(start),
                ..Default::default()
            };
        };

        // d+e+f. Reconcile disk-groups + observe disks.
        let mut outcome = self.observe_disks(&owned).await;
        outcome.groups_added = groups_added;
        outcome.groups_removed = groups_removed;

        // h. Reset missed count on success.
        let prev = self.missed_count.swap(0, Ordering::SeqCst);
        if prev > 0 {
            self.container.exit_degraded_mode();
        }

        // Record successful sync.
        self.container.record_sync_success();
        if let Some(m) = &self.metrics {
            m.sync_success_total.inc();
            m.sync_latency.observe(elapsed_ns(start));
        }

        outcome.sync_duration_ms = elapsed_ms(start);
        info!(
            groups_added = outcome.groups_added,
            groups_removed = outcome.groups_removed,
            disks_added = outcome.disks_added,
            duration_ms = outcome.sync_duration_ms,
            "sync complete"
        );
        outcome
    }

    /// Heartbeat to the service registry. Returns `false` on failure
    /// (caller skips the rest of the tick). Tracks missed count and
    /// enters degraded mode on threshold breach.
    async fn heartbeat(&self) -> bool {
        let instance_id = self.container.instance_id;
        // Compute per-disk-group usage summaries from the in-memory
        // bitmap (R74 §8 keepalive piggyback). Recomputed each tick
        // (not cached); derived, not a source of truth.
        let owned_dg_ids: Vec<u64> = self.container.disk_group_ids();
        let group_usages: Vec<DiskGroupUsageSummary> = self
            .container
            .disk_group_ids()
            .into_iter()
            .filter_map(|dg_id| {
                let dg = self.container.get_disk_group(dg_id)?;
                let u = dg.aggregate_usage();
                Some(DiskGroupUsageSummary {
                    disk_group_id: dg_id,
                    capacity_bytes: u.capacity_bytes,
                    used_bytes: u.busy_bytes,
                    free_bytes: u.free_bytes,
                    disk_count: u.disk_count,
                    allocatable_disk_count: u.allocatable_disk_count,
                })
            })
            .collect();
        if let Err(e) = self
            .svc
            .heartbeat_diskdb(instance_id, &self.grpc_endpoint, &owned_dg_ids, &group_usages)
            .await
        {
            warn!(error = %e, "sync: heartbeat failed");
            let count = self.missed_count.fetch_add(1, Ordering::SeqCst) + 1;
            if count >= self.config.miss_threshold {
                self.container.enter_degraded_mode();
            }
            return false;
        }
        true
    }

    /// Read the owner map + bind map from group 0, filter to owned
    /// disk-groups, and reconcile the container (add new, update
    /// binds, remove gone). Returns `None` on I/O failure (caller
    /// skips the rest of the tick). Returns `(owned, groups_added,
    /// groups_removed)` on success.
    async fn observe_ownership(&self) -> Option<(Vec<crow_protocol::DiskdbOwnerEntry>, usize, usize)> {
        let instance_id = self.container.instance_id;

        // Read ownership map.
        let owners = match self.hw.list_owners().await {
            Ok(o) => o,
            Err(e) => {
                warn!(error = %e, "sync: read owner map failed");
                let count = self.missed_count.fetch_add(1, Ordering::SeqCst) + 1;
                if count >= self.config.miss_threshold {
                    self.container.enter_degraded_mode();
                }
                return None;
            }
        };

        // Read bind map.
        let binds = match self.hw.list_binds().await {
            Ok(b) => b,
            Err(e) => {
                warn!(error = %e, "sync: read bind map failed");
                let count = self.missed_count.fetch_add(1, Ordering::SeqCst) + 1;
                if count >= self.config.miss_threshold {
                    self.container.enter_degraded_mode();
                }
                return None;
            }
        };
        let bind_map: HashMap<DiskGroupId, (u64, u64)> = binds
            .into_iter()
            .map(|b| (b.dg_id, (b.store_id, b.group_id)))
            .collect();

        // Filter to owned disk-groups.
        let owned: Vec<_> = owners
            .into_iter()
            .filter(|o| o.instance_id == instance_id)
            .collect();

        // Reconcile disk-groups: add new, update binds, remove gone.
        let current_ids: Vec<_> = self.container.disk_group_ids();
        let mut groups_added = 0usize;
        let mut groups_removed = 0usize;

        for entry in &owned {
            if !current_ids.contains(&entry.dg_id) {
                // New disk-group assigned.
                let dg = Arc::new(DdbDiskGroup::new(entry.dg_id, entry.node_id, entry.rack_id));
                // Set bind from the bind map.
                if let Some(&(store_id, group_id)) = bind_map.get(&entry.dg_id) {
                    *dg.bind.write().unwrap() = (store_id, group_id);
                }
                self.container.add_disk_group(dg);
                groups_added += 1;
            } else if let Some(dg) = self.container.get_disk_group(entry.dg_id) {
                // Update bind if changed.
                if let Some(&(store_id, group_id)) = bind_map.get(&entry.dg_id) {
                    let mut bind = dg.bind.write().unwrap();
                    if *bind != (store_id, group_id) {
                        *bind = (store_id, group_id);
                    }
                }
            }
        }

        // Detect removed disk-groups.
        for &id in &current_ids {
            if !owned.iter().any(|o| o.dg_id == id) {
                self.container.remove_disk_group(id);
                groups_removed += 1;
            }
        }

        Some((owned, groups_added, groups_removed))
    }

    /// For each owned disk-group, read member disks from group 0 and
    /// reconcile (disk-add init, status changes, removals). Drives
    /// the `HwStateMachine` on status changes.
    async fn observe_disks(&self, owned: &[crow_protocol::DiskdbOwnerEntry]) -> KeepAliveOutcome {
        let mut outcome = KeepAliveOutcome::default();
        for entry in owned {
            let Some(dg) = self.container.get_disk_group(entry.dg_id) else {
                continue;
            };
            let disks = match self
                .hw
                .list_disks_in_group(entry.rack_id, entry.node_id, entry.dg_id)
                .await
            {
                Ok(d) => d,
                Err(e) => {
                    warn!(error = %e, dg_id = entry.dg_id, "sync: list disks failed");
                    continue;
                }
            };
            self.reconcile_disks(
                &dg,
                entry.rack_id,
                entry.node_id,
                entry.dg_id,
                &disks,
                &mut outcome,
            )
            .await;
        }
        outcome
    }

    /// Reconcile the disk list for one disk-group: add new disks,
    /// update status on existing disks, detect removed disks. Drives
    /// the `HwStateMachine` on status changes + the R76 Missing→Bad
    /// confirmation + recovery scan spawn + recovery-to-Up path.
    async fn reconcile_disks(
        &self,
        dg: &Arc<DdbDiskGroup>,
        rack_id: RackId,
        node_id: NodeId,
        dg_id: DiskGroupId,
        disks: &[(DiskId, DiskValue)],
        outcome: &mut KeepAliveOutcome,
    ) {
        let _ = (rack_id, node_id, dg_id);
        let current_disk_ids: Vec<DiskId> = {
            let disks_guard = dg.disks.read().unwrap();
            disks_guard.iter().map(|d| d.disk_id).collect()
        };

        for (disk_id, disk_value) in disks {
            if current_disk_ids.contains(disk_id) {
                // Existing disk — reset miss count (present in sync).
                self.disk_miss_counts.write().unwrap().remove(disk_id);
                self.reconcile_existing_disk(dg, disk_id, disk_value, outcome)
                    .await;
            } else {
                // New disk — disk-add init flow.
                self.disk_add_init(dg, *disk_id, disk_value).await;
                outcome.disks_added += 1;
            }
        }

        // Detect removed disks (present in memory but absent from sync).
        for disk_id in &current_disk_ids {
            if !disks.iter().any(|(id, _)| id == disk_id) {
                self.reconcile_absent_disk(dg, disk_id, outcome);
            }
        }
    }

    /// Reconcile an existing disk present in the sync response: update
    /// status if changed, resume recovery scan if still Bad.
    async fn reconcile_existing_disk(
        &self,
        dg: &Arc<DdbDiskGroup>,
        disk_id: &DiskId,
        disk_value: &DiskValue,
        outcome: &mut KeepAliveOutcome,
    ) {
        let disk = {
            let disks_guard = dg.disks.read().unwrap();
            disks_guard.iter().find(|d| d.disk_id == *disk_id).cloned()
        };
        let Some(disk) = disk else { return };
        let old_status = *disk.effective_status.read().unwrap();
        let new_status = HwStatus::try_from(disk_value.status).unwrap_or(HwStatus::Up);
        if old_status != new_status {
            // R76: unified recovery path for → Up transitions.
            if new_status == HwStatus::Up
                && matches!(old_status, HwStatus::Missing | HwStatus::Bad | HwStatus::Offline)
            {
                self.recover_disk_to_up(dg, &disk).await;
                outcome.status_changes += 1;
            } else {
                match self.status_machine.transition_disk(&disk, new_status) {
                    Ok(_) => {
                        dg.rebuild_allocating_disks();
                        outcome.status_changes += 1;
                    }
                    Err(e) => {
                        warn!(
                            disk = ?disk.disk_id,
                            from = ?old_status,
                            to = ?new_status,
                            error = %e,
                            "illegal disk status transition; keeping current"
                        );
                    }
                }
            }
        } else if old_status == HwStatus::Bad {
            // R76: on restart, a disk that is still Bad needs its
            // recovery scan resumed (if not already running).
            let scan_running = self.recovery_scans.read().unwrap().contains_key(&disk.disk_id);
            if !scan_running {
                info!(disk = ?disk.disk_id, "disk still Bad on sync — resuming recovery scan");
                self.spawn_recovery_scan(dg, &disk);
            }
        }
    }

    /// Reconcile a disk absent from the sync response: track miss
    /// counts, transition Up→Missing→Bad, spawn recovery scan on Bad.
    fn reconcile_absent_disk(
        &self,
        dg: &Arc<DdbDiskGroup>,
        disk_id: &DiskId,
        outcome: &mut KeepAliveOutcome,
    ) {
        let disk = {
            let disks_guard = dg.disks.read().unwrap();
            disks_guard.iter().find(|d| d.disk_id == *disk_id).cloned()
        };
        let Some(disk) = disk else { return };
        let old_status = *disk.effective_status.read().unwrap();

        // Only track miss counts for disks that are not already Bad.
        if old_status == HwStatus::Bad {
            return;
        }

        // Increment consecutive miss count.
        let miss_count = {
            let mut counts = self.disk_miss_counts.write().unwrap();
            let c = counts.entry(*disk_id).or_insert(0);
            *c += 1;
            *c
        };

        if old_status != HwStatus::Missing {
            // First absence → transition Up → Missing.
            match self.status_machine.transition_disk(&disk, HwStatus::Missing) {
                Ok(_) => {
                    dg.rebuild_allocating_disks();
                    outcome.status_changes += 1;
                    info!(disk = ?disk_id, "disk absent from sync → Missing");
                }
                Err(e) => {
                    warn!(
                        disk = ?disk_id,
                        from = ?old_status,
                        to = ?HwStatus::Missing,
                        error = %e,
                        "illegal disk status transition; keeping current"
                    );
                }
            }
        } else if miss_count >= self.config.miss_threshold {
            // Nth consecutive absence → Missing → Bad + spawn recovery scan.
            match self.status_machine.transition_disk(&disk, HwStatus::Bad) {
                Ok(_) => {
                    dg.rebuild_allocating_disks();
                    outcome.status_changes += 1;
                    info!(
                        disk = ?disk_id,
                        miss_count = miss_count,
                        "disk absent for N ticks → Bad; starting recovery scan"
                    );
                    self.spawn_recovery_scan(dg, &disk);
                    self.disk_miss_counts.write().unwrap().remove(disk_id);
                }
                Err(e) => {
                    warn!(
                        disk = ?disk_id,
                        from = ?old_status,
                        to = ?HwStatus::Bad,
                        error = %e,
                        "illegal disk status transition; keeping current"
                    );
                }
            }
        }
    }

    /// Spawn a per-disk recovery scan task (R76). The task runs
    /// independently (`tokio::spawn`) and is tracked in
    /// `recovery_scans` for cancellation on recovery.
    fn spawn_recovery_scan(&self, dg: &Arc<DdbDiskGroup>, disk: &Arc<DdbDisk>) {
        let Some(ref kv) = self.kv else {
            warn!(disk = ?disk.disk_id, "recovery scan: no kv client, skipping");
            return;
        };
        let bind = *dg.bind.read().unwrap();
        let cancel = Arc::new(AtomicBool::new(false));
        let Some(m) = &self.metrics else {
            warn!(disk = ?disk.disk_id, "recovery scan: no metrics handle, skipping");
            return;
        };
        let gauge = Arc::clone(&m.disk_bad_impacted_blocks);
        let task = RecoveryScanTask::new(Arc::clone(disk), bind, kv.clone(), Arc::clone(&cancel), gauge);
        let disk_id = disk.disk_id;
        let join = tokio::spawn(async move {
            task.run().await;
        });
        self.recovery_scans
            .write()
            .unwrap()
            .insert(disk_id, RecoveryScanHandle { cancel, join });
    }

    /// Unified recovery path for `Missing → Up`, `Bad → Up`, `Offline
    /// → Up` (R76). Cancels the recovery scan (if running), runs
    /// compaction on the disk's zones, rebuilds active zones, and
    /// re-includes the disk in the allocating set.
    async fn recover_disk_to_up(&self, dg: &Arc<DdbDiskGroup>, disk: &Arc<DdbDisk>) {
        // Cancel any running recovery scan.
        let scan_handle = self.recovery_scans.write().unwrap().remove(&disk.disk_id);
        if let Some(handle) = scan_handle {
            handle.cancel.store(true, Ordering::Release);
            // The task will see the cancel flag on its next zone
            // boundary and exit. We don't await it here — the scan
            // persists its own progress on cancel.
            info!(disk = ?disk.disk_id, "recovery scan cancelled (disk → Up)");
        }

        // Run the recovery-to-Up path: compaction + rebuild.
        if let Some(ref kv) = self.kv {
            let bind = *dg.bind.read().unwrap();
            recover_disk_to_up(disk, bind, kv, self.config.zone_rotate_count).await;
        } else {
            // No kv client (test mode) — just set status + rebuild.
            disk.set_effective_status(HwStatus::Up);
            disk.rebuild_active_zones(self.config.zone_rotate_count);
        }
        dg.rebuild_allocating_disks();
        info!(disk = ?disk.disk_id, "disk recovered → Up");
    }

    /// Disk-add init flow (§3.5): create `DdbDisk` + `DdbZone`s, write
    /// baseline `ZoneValue` records, rebuild active zones, add to
    /// `DdbDiskGroup.disks`.
    #[allow(clippy::cast_possible_truncation)]
    async fn disk_add_init(&self, dg: &Arc<DdbDiskGroup>, disk_id: DiskId, disk_value: &DiskValue) {
        let zone_count = disk_value.zone_count;
        let zone_size_units = disk_value.zone_size_units;
        let unit_size_bytes = disk_value.unit_size_bytes;
        let _ = unit_size_bytes;

        let mut disk = DdbDisk::new(disk_id, dg.disk_group_id, dg.node_id, dg.rack_id, *disk_value);
        // Attach per-disk hot-path counters (R74 §3).
        disk.metrics = Some(Arc::new(DiskMetrics::new()));
        let disk = Arc::new(disk);

        for zi in 0..zone_count {
            // Last zone may be smaller; round down to multiple of 64.
            let unit_capacity = if zi == zone_count - 1 {
                let remaining = disk_value.capacity_units - (u64::from(zi) * zone_size_units);
                let rounded = (remaining / 64) * 64;
                rounded as u32
            } else {
                zone_size_units as u32
            };
            let zone = DdbZone::new(disk_id, zi, dg.disk_group_id, unit_capacity);
            let zone = if let Some(ref counter) = self.cas_retry_metric {
                zone.with_cas_retry_metric(Arc::clone(counter))
            } else {
                zone
            };
            disk.add_zone(Arc::new(zone));
        }

        // Write baseline ZoneValue records (empty bitmap, snapshot_slot=0)
        // — but only if no snapshot already exists (R73: previously-owned
        // disk-groups have real snapshots that must not be overwritten;
        // recovery runs after tick to replay the journal from
        // snapshot_slot).
        if let Some(ref kv) = self.kv {
            let bind = *dg.bind.read().unwrap();
            // Check the first zone only — if it has a snapshot, the
            // disk was previously initialized (disk_add_init writes
            // baseline snapshots for all zones atomically per zone).
            let snapshots_exist = crate::recovery::zone_snapshots_exist(kv, bind, &disk_id, zone_count).await;
            if snapshots_exist {
                info!(
                    disk = %disk_id.to_display_string(),
                    "disk-add init: snapshots already exist, skipping baseline write (recovery will replay)"
                );
            } else {
                let zone_values: Vec<(u32, crow_protocol::diskdb::rpc::ZoneValue)> = {
                    let zones = disk.zones.read().unwrap();
                    zones
                        .iter()
                        .enumerate()
                        .map(|(zi, zone)| {
                            #[allow(clippy::cast_possible_truncation)]
                            (zi as u32, zone.to_zone_value())
                        })
                        .collect()
                };
                for (zi, zv) in &zone_values {
                    if let Err(e) = kv.put_zone(bind, &disk_id, *zi, zv).await {
                        warn!(error = %e, disk = %disk_id.to_display_string(), zone = zi, "disk-add init: put_zone failed");
                    }
                }
            }
        }

        disk.rebuild_active_zones(self.config.zone_rotate_count);
        dg.add_disk(disk);
        dg.rebuild_allocating_disks();
    }
}

impl BackgroundTask for KeepAlive {
    fn run_cycle<'a>(&'a self, _ctx: &'a BgCtx) -> CycleFut<'a> {
        Box::pin(async move {
            let _ = self.tick().await;
            Ok(())
        })
    }

    fn trigger(&self) -> Trigger {
        match &self.config_handle {
            Some(handle) => {
                let handle = Arc::clone(handle);
                let interval_fn = Box::new(move || {
                    std::time::Duration::from_secs(u64::from(handle.load().heartbeat.interval_secs))
                });
                if let Some(ref notify) = self.sync_trigger {
                    Trigger::TimerOrEvent {
                        interval_fn,
                        notify: Arc::clone(notify),
                    }
                } else {
                    Trigger::TimerFn(interval_fn)
                }
            }
            None => Trigger::Timer(self.config.interval),
        }
    }

    fn name(&self) -> &'static str {
        "keepalive"
    }
}
