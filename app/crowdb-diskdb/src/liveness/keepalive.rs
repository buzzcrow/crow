// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `KeepAlive` — keep-alive + periodic hardware sync from group 0.
//!
//! Each tick: heartbeat, read ownership map, read bind map, read
//! member disks per owned disk-group, reconcile in-memory state
//! (disk-add init, status changes, removals).

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, RwLock};

use crowdb_common::metrics::Counter;
use crowdb_kv_client::{HardwareClient, ServiceRegistryClient};
use crowdb_protocol::common::{DiskGroupUsageSummary, DiskId, HwStatus};
use crowdb_protocol::diskdb::rpc::DiskValue;
use crowdb_protocol::{DiskGroupId, NodeId, RackId};
use tracing::{info, warn};

use crate::bg_task::{BackgroundTask, BgCtx, CycleFut, Trigger};
use crate::ddb_config::KeepAliveConfig;
use crate::ddb_kv_client::{Bind, DdbKvClient};
use crate::liveness::state_machine::HwStateMachine;
use crate::metrics::{DiskMetrics, DiskdbMetrics};
use crate::model::disk::DdbDisk;
use crate::model::disk_group::DdbDiskGroup;
use crate::model::disk_group_container::DdbDiskGroupContainer;
use crate::recovery::disk_recovery::{recover_disk_to_up, ImpactedBlocksGauge, RecoveryScanTask};
use crate::recovery::{full_scan, journal_replay, unit_capacity_for_zone};

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
    /// crowdb-rpc endpoint to register with the service registry (R74
    /// keepalive piggyback). When empty, passes `""` (test mode).
    rpc_endpoint: String,
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
    /// Cluster-aggregated `disk.bad.impacted_blocks` gauge. Each
    /// recovery scan reports its per-disk count; the gauge sums across
    /// all concurrently bad disks. Set in `with_metrics`.
    impacted_blocks: Option<Arc<ImpactedBlocksGauge>>,
    /// Per-disk Suspect-since timestamps (A.3). Tracks when a disk
    /// entered `Suspect` for `check_suspect_timeout` (Suspect → Offline
    /// after `temp_failure_timeout`). Removed when the disk leaves
    /// Suspect.
    disk_suspect_since: RwLock<HashMap<DiskId, std::time::Instant>>,
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
            rpc_endpoint: String::new(),
            metrics: None,
            sync_trigger: None,
            disk_miss_counts: RwLock::new(HashMap::new()),
            recovery_scans: RwLock::new(HashMap::new()),
            impacted_blocks: None,
            disk_suspect_since: RwLock::new(HashMap::new()),
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

    /// Attach the crowdb-rpc endpoint to register with the service registry
    /// (R74 keepalive piggyback). When set, `heartbeat` passes this
    /// endpoint + per-disk-group usage summaries to
    /// `heartbeat_diskdb`.
    #[must_use]
    pub fn with_rpc_endpoint(mut self, endpoint: String) -> Self {
        self.rpc_endpoint = endpoint;
        self
    }

    /// Attach a metrics handle for sync latency/success/failure
    /// observations (R74 §11). Also wraps the
    /// `disk.bad.impacted_blocks` gauge in an `ImpactedBlocksGauge`
    /// aggregator so concurrent per-disk recovery scans sum into the
    /// gauge instead of overwriting each other.
    #[must_use]
    pub fn with_metrics(mut self, metrics: DiskdbMetrics) -> Self {
        self.impacted_blocks = Some(Arc::new(ImpactedBlocksGauge::new(Arc::clone(
            &metrics.disk_bad_impacted_blocks,
        ))));
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
        let read_start = std::time::Instant::now();
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
        if let Some(m) = &self.metrics {
            m.sync_read_group0_latency.observe(elapsed_ns(read_start));
        }
        outcome.groups_added = groups_added;
        outcome.groups_removed = groups_removed;

        // h. Reset missed count on success.
        let apply_start = std::time::Instant::now();
        let prev = self.missed_count.swap(0, Ordering::SeqCst);
        if prev > 0 {
            self.container.exit_degraded_mode();
        }

        // Record successful sync.
        self.container.record_sync_success();
        if let Some(m) = &self.metrics {
            m.sync_success_total.inc();
            m.sync_latency.observe(elapsed_ns(start));
            m.sync_apply_changes_latency.observe(elapsed_ns(apply_start));
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
            .heartbeat_diskdb(instance_id, &self.rpc_endpoint, &owned_dg_ids, &group_usages)
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
    async fn observe_ownership(&self) -> Option<(Vec<crowdb_protocol::DiskdbOwnerEntry>, usize, usize)> {
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
    async fn observe_disks(&self, owned: &[crowdb_protocol::DiskdbOwnerEntry]) -> KeepAliveOutcome {
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
    ///
    /// A.1: fetches `NodeValue` and `DiskGroupValue` from group 0 to
    /// compute the three-level effective status `max(node, group,
    /// disk)`. The group status is applied to `DdbDiskGroup` via
    /// `transition_disk_group`; the effective status is stored on each
    /// `DdbDisk`.
    async fn reconcile_disks(
        &self,
        dg: &Arc<DdbDiskGroup>,
        rack_id: RackId,
        node_id: NodeId,
        dg_id: DiskGroupId,
        disks: &[(DiskId, DiskValue)],
        outcome: &mut KeepAliveOutcome,
    ) {
        // A.1: fetch node + disk-group status from group 0.
        let node_status = match self.hw.get_node(rack_id, node_id).await {
            Ok(Some(nv)) => HwStatus::try_from(nv.status).unwrap_or(HwStatus::Up),
            Ok(None) => {
                warn!(rack_id, node_id, "sync: node value absent; assuming Up");
                HwStatus::Up
            }
            Err(e) => {
                warn!(error = %e, rack_id, node_id, "sync: get_node failed; assuming Up");
                HwStatus::Up
            }
        };
        let group_status = match self.hw.get_disk_group(rack_id, node_id, dg_id).await {
            Ok(Some(dgv)) => HwStatus::try_from(dgv.value.status).unwrap_or(HwStatus::Up),
            Ok(None) => {
                warn!(dg_id, "sync: disk-group value absent; assuming Up");
                HwStatus::Up
            }
            Err(e) => {
                warn!(error = %e, dg_id, "sync: get_disk_group failed; assuming Up");
                HwStatus::Up
            }
        };

        // A.1: apply group status to the in-memory DdbDiskGroup.
        let current_group_status = *dg.status.read().unwrap();
        if current_group_status != group_status {
            match self.status_machine.transition_disk_group(dg, group_status) {
                Ok(_) => {
                    dg.rebuild_allocating_disks();
                    outcome.status_changes += 1;
                    info!(dg_id, from = ?current_group_status, to = ?group_status, "disk-group status changed");
                }
                Err(e) => {
                    warn!(dg_id, from = ?current_group_status, to = ?group_status, error = %e, "illegal disk-group transition; keeping current");
                }
            }
        }

        let current_disk_ids: Vec<DiskId> = {
            let disks_guard = dg.disks.read().unwrap();
            disks_guard.iter().map(|d| d.disk_id).collect()
        };

        for (disk_id, disk_value) in disks {
            if current_disk_ids.contains(disk_id) {
                // Existing disk — reset miss count (present in sync).
                self.disk_miss_counts.write().unwrap().remove(disk_id);
                // A.3: disk present → clear Suspect timer.
                self.disk_suspect_since.write().unwrap().remove(disk_id);
                self.reconcile_existing_disk(dg, disk_id, disk_value, node_status, group_status, outcome)
                    .await;
            } else {
                // New disk — disk-add init flow (R81: Init-state).
                self.disk_add_init(dg, *disk_id, disk_value);
                outcome.disks_added += 1;
            }
        }

        // Detect removed disks (present in memory but absent from sync).
        for disk_id in &current_disk_ids {
            if !disks.iter().any(|(id, _)| id == disk_id) {
                self.reconcile_absent_disk(
                    dg,
                    rack_id,
                    node_id,
                    dg_id,
                    disk_id,
                    node_status,
                    group_status,
                    outcome,
                )
                .await;
            }
        }
    }

    /// Reconcile an existing disk present in the sync response: update
    /// status if changed, resume recovery scan if still Bad. A.1:
    /// computes the three-level effective status and stores it on the
    /// disk.
    async fn reconcile_existing_disk(
        &self,
        dg: &Arc<DdbDiskGroup>,
        disk_id: &DiskId,
        disk_value: &DiskValue,
        node_status: HwStatus,
        group_status: HwStatus,
        outcome: &mut KeepAliveOutcome,
    ) {
        let disk = {
            let disks_guard = dg.disks.read().unwrap();
            disks_guard.iter().find(|d| d.disk_id == *disk_id).cloned()
        };
        let Some(disk) = disk else { return };
        let old_status = *disk.effective_status.read().unwrap();
        // R81: an Init disk's status is owned by the background zone
        // load task — it transitions Init → disk_value.status only
        // after all zones are loaded. Skipping reconciliation here
        // prevents a sync tick from flipping Init → Up with zero or
        // partially-loaded zones (making the disk allocatable early).
        if old_status == HwStatus::Init {
            // Deferred zone load: the disk was added while the
            // disk-group was unbound (bind == (0,0)). If the bind is
            // now set, spawn the zone load task that was deferred in
            // disk_add_init.
            let bind = *dg.bind.read().unwrap();
            if bind != (0, 0) {
                if let Some(ref kv) = self.kv {
                    if disk.try_claim_zone_load() {
                        info!(
                            disk = ?disk_id,
                            dg_id = dg.disk_group_id,
                            "reconcile: Init disk now bound; spawning deferred zone load"
                        );
                        self.spawn_zone_load(dg, &disk, disk_value, bind, kv.clone());
                    }
                }
            }
            return;
        }
        let raw_disk_status = HwStatus::try_from(disk_value.status).unwrap_or(HwStatus::Up);
        // A.1: effective = max(node, group, disk).
        let new_effective = HwStateMachine::effective_status(node_status, group_status, raw_disk_status);
        if old_status != new_effective {
            // R76: unified recovery path for → Up transitions.
            if new_effective == HwStatus::Up
                && matches!(
                    old_status,
                    HwStatus::Missing | HwStatus::Bad | HwStatus::Offline | HwStatus::Suspect
                )
            {
                self.recover_disk_to_up(dg, &disk).await;
                outcome.status_changes += 1;
            } else {
                // A.1: directly set the effective status (the
                // transition may not be legal per the disk-only
                // transition table — e.g. node going Offline while
                // disk is Up → effective Offline. This is not a disk
                // status change, it's an effective-status derivation).
                disk.set_effective_status(new_effective);
                dg.rebuild_allocating_disks();
                outcome.status_changes += 1;
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

    /// Reconcile a disk absent from the sync response. A.3: implements
    /// the Suspect anti-flapping path — `Up → Suspect` on first
    /// absence, response-driven resolution after `miss_threshold`.
    /// A.2: write-back to group 0 before the local transition.
    #[allow(clippy::too_many_lines, clippy::too_many_arguments)]
    async fn reconcile_absent_disk(
        &self,
        dg: &Arc<DdbDiskGroup>,
        rack_id: RackId,
        node_id: NodeId,
        dg_id: DiskGroupId,
        disk_id: &DiskId,
        node_status: HwStatus,
        group_status: HwStatus,
        outcome: &mut KeepAliveOutcome,
    ) {
        let disk = {
            let disks_guard = dg.disks.read().unwrap();
            disks_guard.iter().find(|d| d.disk_id == *disk_id).cloned()
        };
        let Some(disk) = disk else { return };
        let old_status = *disk.effective_status.read().unwrap();

        // R81: Bad disks are kept in memory (recovery scan running).
        if old_status == HwStatus::Bad {
            return;
        }

        // R81: Offline, Maintenance, Init disks are removed from
        // memory directly. Their DiskKey was deleted from group 0
        // (moved or removed); absence means the disk is gone.
        if matches!(
            old_status,
            HwStatus::Offline | HwStatus::Maintenance | HwStatus::Init
        ) {
            dg.remove_disk_from_memory(disk_id);
            self.disk_miss_counts.write().unwrap().remove(disk_id);
            self.disk_suspect_since.write().unwrap().remove(disk_id);
            outcome.disks_removed += 1;
            info!(disk = ?disk_id, status = ?old_status, "disk absent from sync → removed from memory");
            return;
        }

        // Increment consecutive miss count.
        let miss_count = {
            let mut counts = self.disk_miss_counts.write().unwrap();
            let c = counts.entry(*disk_id).or_insert(0);
            *c += 1;
            *c
        };

        // A.3: Up → Suspect on first absence (anti-flapping buffer).
        // Suspect is not allocatable; free is allowed. Suspect is
        // local-only — NOT written back to group 0. Writing it back
        // would conflict with A.1c (effective = max over group-0 disk
        // status): on rediscovery the raw group-0 status would be
        // Suspect, making effective = Suspect with no path back to Up
        // (the Suspect timer is cleared on presence, so
        // check_suspect_timeout never fires). Keeping Suspect local
        // means the group-0 disk status stays Up, so rediscovery
        // yields effective = Up and recover_disk_to_up fires.
        if old_status == HwStatus::Up {
            disk.set_effective_status(HwStatus::Suspect);
            self.disk_suspect_since
                .write()
                .unwrap()
                .insert(*disk_id, std::time::Instant::now());
            dg.rebuild_allocating_disks();
            outcome.status_changes += 1;
            info!(disk = ?disk_id, "disk absent from sync → Suspect (anti-flapping)");
            return;
        }

        // A.3: Suspect state — resolve based on miss count.
        if old_status == HwStatus::Suspect {
            if miss_count >= self.config.miss_threshold {
                // Threshold reached — resolve based on the sync
                // response context. Since the disk is absent, the
                // response either didn't contain it (Missing) or the
                // whole sync failed (Offline). We can't distinguish
                // here (the caller already has the response), so we
                // use the node/group status: if the node or group is
                // not Up, the whole thing is down → Offline;
                // otherwise the disk was removed → Missing.
                let resolved = if node_status != HwStatus::Up || group_status != HwStatus::Up {
                    HwStatus::Offline
                } else {
                    HwStatus::Missing
                };
                self.write_back_disk_status(rack_id, node_id, dg_id, disk_id, resolved)
                    .await;
                match self.status_machine.transition_disk(&disk, resolved) {
                    Ok(_) => {
                        dg.rebuild_allocating_disks();
                        outcome.status_changes += 1;
                        self.disk_suspect_since.write().unwrap().remove(disk_id);
                        info!(disk = ?disk_id, miss_count, resolved = ?resolved, "Suspect → resolved after miss_threshold");
                        if resolved == HwStatus::Missing {
                            // Keep counting — Missing → Bad after
                            // further confirmation.
                        } else {
                            // Offline — stop counting.
                            self.disk_miss_counts.write().unwrap().remove(disk_id);
                        }
                    }
                    Err(e) => {
                        warn!(
                            disk = ?disk_id,
                            from = ?old_status,
                            to = ?resolved,
                            error = %e,
                            "illegal Suspect resolution; keeping current"
                        );
                    }
                }
            } else {
                // A.3: check_suspect_timeout — Suspect → Offline
                // after temp_failure_timeout (intermittent absences
                // that never reach miss_threshold).
                let suspect_timed_out = {
                    let since_guard = self.disk_suspect_since.read().unwrap();
                    since_guard.get(disk_id).is_some_and(|&s| {
                        self.status_machine
                            .check_suspect_timeout(s, std::time::Instant::now())
                    })
                };
                if suspect_timed_out {
                    self.write_back_disk_status(rack_id, node_id, dg_id, disk_id, HwStatus::Offline)
                        .await;
                    match self.status_machine.transition_disk(&disk, HwStatus::Offline) {
                        Ok(_) => {
                            dg.rebuild_allocating_disks();
                            outcome.status_changes += 1;
                            self.disk_suspect_since.write().unwrap().remove(disk_id);
                            self.disk_miss_counts.write().unwrap().remove(disk_id);
                            info!(disk = ?disk_id, "Suspect → Offline (temp_failure_timeout)");
                        }
                        Err(e) => {
                            warn!(
                                disk = ?disk_id,
                                from = ?old_status,
                                to = ?HwStatus::Offline,
                                error = %e,
                                "illegal Suspect → Offline; keeping current"
                            );
                        }
                    }
                }
            }
            return;
        }

        // old_status == Missing — continue the Missing → Bad path.
        if old_status == HwStatus::Missing && miss_count >= self.config.miss_threshold {
            self.write_back_disk_status(rack_id, node_id, dg_id, disk_id, HwStatus::Bad)
                .await;
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

    /// A.2: best-effort write-back of a disk's status to group 0
    /// before the local transition. Failures are logged and ignored —
    /// the local transition is the safety-critical action; the
    /// write-back is observability for the operator.
    async fn write_back_disk_status(
        &self,
        rack_id: RackId,
        node_id: NodeId,
        dg_id: DiskGroupId,
        disk_id: &DiskId,
        status: HwStatus,
    ) {
        if let Err(e) = self
            .hw
            .set_disk_status(rack_id, node_id, dg_id, disk_id, status)
            .await
        {
            warn!(
                disk = ?disk_id,
                status = ?status,
                error = %e,
                "write-back: set_disk_status failed (best-effort; local transition proceeds)"
            );
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
        let Some(ref impacted) = self.impacted_blocks else {
            warn!(disk = ?disk.disk_id, "recovery scan: no impacted-blocks gauge, skipping");
            return;
        };
        let bind = *dg.bind.read().unwrap();
        let cancel = Arc::new(AtomicBool::new(false));
        let task = RecoveryScanTask::new(
            Arc::clone(disk),
            bind,
            kv.clone(),
            Arc::clone(&cancel),
            Arc::clone(impacted),
        );
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
    /// → Up` (R76). Cancels the recovery scan (if running), drops the
    /// disk's impacted-blocks contribution, routes the status
    /// transition through the state machine (single source of truth),
    /// then runs compaction + rebuild + re-include.
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

        // Drop this disk's contribution to the cluster impacted-blocks
        // gauge.
        if let Some(ref ib) = self.impacted_blocks {
            ib.remove_disk(&disk.disk_id);
        }

        // Route the status transition through the state machine so it
        // is the single source of truth (validates legality, including
        // the operator-override `Bad → Up`, and sets `effective_status`).
        if let Err(e) = self.status_machine.transition_disk(disk, HwStatus::Up) {
            warn!(
                disk = ?disk.disk_id,
                error = %e,
                "recovery-to-Up: illegal transition, keeping current status"
            );
            return;
        }

        // Run the recovery-to-Up path: compaction + rebuild. The
        // status is already set by `transition_disk` above; the
        // `recover_disk_to_up` helper no longer sets it itself.
        if let Some(ref kv) = self.kv {
            let bind = *dg.bind.read().unwrap();
            if let Some(m) = &self.metrics {
                recover_disk_to_up(disk, bind, kv, self.config.zone_rotate_count, m).await;
            } else {
                let m = crate::metrics::DiskdbMetrics::disabled();
                recover_disk_to_up(disk, bind, kv, self.config.zone_rotate_count, &m).await;
            }
        } else {
            // No kv client (test mode) — status already set; just
            // rebuild active zones.
            disk.rebuild_active_zones(self.config.zone_rotate_count);
        }
        dg.rebuild_allocating_disks();
        info!(disk = ?disk.disk_id, "disk recovered → Up");
    }

    /// Disk-add init flow (R81): create `DdbDisk` with
    /// `effective_status = Init` and no zones, add to the disk-group,
    /// then spawn a background zone load task. The task loads zones
    /// via `load_zone_inner` (strategy 2 + strategy 1 fallback) and
    /// transitions `Init → disk_value.status` on success.
    ///
    /// If the disk-group is unbound (`bind == (0, 0)`), the zone load
    /// is deferred — the disk stays in Init state until a bind is set
    /// (detected by `reconcile_existing_disk` on a later sync tick).
    /// This prevents binary `ZoneValue` baseline writes from landing
    /// in group-0 (store 0, group 0), which is the system group and
    /// must only contain text-path keys + JSON values.
    fn disk_add_init(&self, dg: &Arc<DdbDiskGroup>, disk_id: DiskId, disk_value: &DiskValue) {
        let mut disk = DdbDisk::new(
            disk_id,
            dg.disk_group_id,
            dg.node_id,
            dg.rack_id,
            disk_value.clone(),
        );
        disk.metrics = Some(Arc::new(DiskMetrics::new()));
        let disk = Arc::new(disk);

        // Add the Init disk to the group (not allocatable until
        // transitioned to Up).
        dg.add_disk(disk.clone());
        dg.rebuild_allocating_disks();

        // Spawn background zone load if we have a kv client and the
        // disk-group is bound to a real data group.
        if let Some(ref kv) = self.kv {
            let bind = *dg.bind.read().unwrap();
            if bind == (0, 0) {
                info!(
                    disk = ?disk_id,
                    dg_id = dg.disk_group_id,
                    "disk-add init: disk-group unbound; deferring zone load until bind is set"
                );
                return;
            }
            if disk.try_claim_zone_load() {
                self.spawn_zone_load(dg, &disk, disk_value, bind, kv.clone());
            }
        } else {
            // No kv client (test mode) — transition Init → Up with
            // empty zones so the disk becomes allocatable.
            let target = HwStatus::try_from(disk_value.status).unwrap_or(HwStatus::Up);
            let final_status = if HwStateMachine::is_legal_transition(HwStatus::Init, target) {
                target
            } else {
                warn!(
                    disk = ?disk_id,
                    target = ?target,
                    "disk-add init: illegal Init → target; falling back to Offline"
                );
                HwStatus::Offline
            };
            if let Err(e) = self.status_machine.transition_disk(&disk, final_status) {
                warn!(disk = ?disk_id, error = %e, "disk-add init: Init → final_status failed");
            }
            disk.rebuild_active_zones(self.config.zone_rotate_count);
            dg.rebuild_allocating_disks();
        }
    }

    /// Spawn the background zone load task for a disk. Extracted from
    /// `disk_add_init` so it can also be called from
    /// `reconcile_existing_disk` when a deferred Init disk's bind
    /// becomes set.
    fn spawn_zone_load(
        &self,
        dg: &Arc<DdbDiskGroup>,
        disk: &Arc<DdbDisk>,
        disk_value: &DiskValue,
        bind: Bind,
        kv: DdbKvClient,
    ) {
        let disk_id = disk.disk_id;
        let zone_rotate_count = self.config.zone_rotate_count;
        let status_machine = self.status_machine.clone();
        let dg = Arc::clone(dg);
        let disk = Arc::clone(disk);
        let kv = Arc::new(kv);
        let cas_retry_metric = self.cas_retry_metric.clone();
        let disk_value_owned = disk_value.clone();
        let hw = self.hw.clone();
        tokio::spawn(async move {
            Self::background_zone_load(
                bind,
                disk_id,
                disk_value_owned,
                kv,
                cas_retry_metric,
                zone_rotate_count,
                status_machine,
                dg,
                disk,
                hw,
            )
            .await;
        });
    }

    /// Background zone load task for a new Init-state disk. Loads all
    /// zones via strategy 2 (journal replay) with strategy 1 (full
    /// scan) fallback, then transitions `Init → disk_value.status`.
    /// B.2: writes baseline `ZoneValue` records for fresh-disk zones
    /// that had no snapshot.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    async fn background_zone_load(
        bind: Bind,
        disk_id: DiskId,
        disk_value: DiskValue,
        kv: Arc<DdbKvClient>,
        cas_retry_metric: Option<Arc<Counter>>,
        zone_rotate_count: u32,
        status_machine: HwStateMachine,
        dg: Arc<DdbDiskGroup>,
        disk: Arc<DdbDisk>,
        hw: HardwareClient,
    ) {
        let zone_count = disk_value.zone_count;
        let zone_size_units = disk_value.zone_size_units;
        let mut all_ok = true;
        // B.2: track zones without snapshots (fresh disk) for baseline
        // ZoneValue writes.
        let mut zones_needing_baseline: Vec<(u32, Arc<crate::model::zone::DdbZone>)> = Vec::new();

        for zi in 0..zone_count {
            let unit_capacity = unit_capacity_for_zone(&disk_value, zi, zone_count, zone_size_units);
            let (zone, _max_freed_ts, snapshot_found) = match journal_replay::load_zone_inner(
                &kv,
                bind,
                disk_id,
                zi,
                dg.disk_group_id,
                unit_capacity,
            )
            .await
            {
                Ok((zone, max_freed_ts, found)) => (zone, max_freed_ts, found),
                Err(e) => {
                    warn!(
                        disk = ?disk_id,
                        zone = zi,
                        error = %e,
                        "init-state load: strategy 2 failed; falling back to full scan"
                    );
                    match full_scan::rebuild_zone_bitmap_full_scan(
                        &kv,
                        bind,
                        disk_id,
                        zi,
                        dg.disk_group_id,
                        unit_capacity,
                    )
                    .await
                    {
                        // Strategy 1 fallback — not a fresh disk
                        // (it has records); no baseline needed.
                        Ok((zone, _)) => (zone, 0, true),
                        Err(e2) => {
                            tracing::error!(
                                disk = ?disk_id,
                                zone = zi,
                                error = %e2,
                                "init-state load: strategy 1 also failed; using empty zone"
                            );
                            all_ok = false;
                            (
                                crate::model::zone::DdbZone::new(
                                    disk_id,
                                    zi,
                                    dg.disk_group_id,
                                    unit_capacity,
                                ),
                                0,
                                false,
                            )
                        }
                    }
                }
            };

            // Attach CAS retry metric if configured.
            let zone = if let Some(ref counter) = cas_retry_metric {
                Arc::new(zone.with_cas_retry_metric(Arc::clone(counter)))
            } else {
                Arc::new(zone)
            };

            // B.2: if no snapshot was found (fresh disk zone), queue
            // for baseline write. Strategy 1 fallback zones have
            // records (snapshot_found = true) — no baseline needed.
            if !snapshot_found {
                zones_needing_baseline.push((zi, Arc::clone(&zone)));
            }

            disk.add_zone(zone);
        }

        // B.2: write baseline ZoneValue records for fresh-disk zones
        // that had no snapshot. Chunked batch_write — the batch size
        // depends on the operation size. Non-atomicity is harmless
        // because background_zone_load retries missing baselines on
        // next restart (strategy 2 finds no snapshot for the missing
        // zones).
        if !zones_needing_baseline.is_empty() {
            let baseline_count = zones_needing_baseline.len();
            let mut write_ok = true;
            for (zi, zone) in &zones_needing_baseline {
                let zv = zone.to_zone_value();
                if let Err(e) = kv.put_zone(bind, &disk_id, *zi, &zv).await {
                    warn!(
                        disk = ?disk_id,
                        zone = zi,
                        error = %e,
                        "baseline ZoneValue write failed; will retry on next restart"
                    );
                    write_ok = false;
                }
            }
            if !write_ok {
                // B.2: baseline write failed → transition to Offline
                // (not Up). The disk has no/partial snapshots; on
                // next restart background_zone_load retries the
                // baseline write for the missing zones.
                all_ok = false;
            }
            info!(disk = ?disk_id, baseline_count, "baseline ZoneValue records written for fresh-disk zones");
        }

        // Transition Init → final status.
        let target = if all_ok {
            HwStatus::try_from(disk_value.status).unwrap_or(HwStatus::Up)
        } else {
            HwStatus::Offline
        };
        let final_status = if HwStateMachine::is_legal_transition(HwStatus::Init, target) {
            target
        } else {
            warn!(
                disk = ?disk_id,
                target = ?target,
                "init-state load: illegal Init → target; falling back to Offline"
            );
            HwStatus::Offline
        };
        // R77: write back Offline to group 0 before the local
        // transition so the next sync tick sees Offline (not Up) —
        // prevents the recover_disk_to_up loop on zone-load failure.
        if final_status == HwStatus::Offline {
            if let Err(e) = hw
                .set_disk_status(dg.rack_id, dg.node_id, dg.disk_group_id, &disk_id, final_status)
                .await
            {
                warn!(
                    disk = ?disk_id,
                    status = ?final_status,
                    error = %e,
                    "init-state load: write-back Offline failed (best-effort; local transition proceeds)"
                );
            }
        }
        if let Err(e) = status_machine.transition_disk(&disk, final_status) {
            warn!(disk = ?disk_id, error = %e, "init-state load: Init → final_status failed");
        }
        disk.rebuild_active_zones(zone_rotate_count);
        dg.rebuild_allocating_disks();
        info!(disk = ?disk_id, status = ?final_status, "init-state zone load complete");
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
