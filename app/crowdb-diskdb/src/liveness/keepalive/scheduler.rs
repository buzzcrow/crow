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

struct ObservedDiskGroup {
    owner: crowdb_protocol::DiskdbOwnerEntry,
    bind: (u64, u64),
    node_status: HwStatus,
    group_status: HwStatus,
    disks: Vec<(DiskId, DiskValue)>,
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

        // Observe a complete group-0 snapshot before publishing changes.
        let read_start = std::time::Instant::now();
        let Some(observed) = self.observe_group0().await else {
            if let Some(m) = &self.metrics {
                m.sync_failure_total.inc();
            }
            return KeepAliveOutcome {
                sync_duration_ms: elapsed_ms(start),
                ..Default::default()
            };
        };

        let (groups_added, groups_removed) = self.reconcile_ownership(&observed);
        let mut outcome = self.reconcile_observed_disks(&observed).await;
        if let Some(m) = &self.metrics {
            m.sync_read_group0_latency.observe(elapsed_ns(read_start));
        }
        outcome.groups_added = groups_added;
        outcome.groups_removed = groups_removed;

        // Publish the reconciled ownership and usage in the heartbeat.
        if !self.heartbeat().await {
            if let Some(m) = &self.metrics {
                m.sync_failure_total.inc();
            }
            outcome.sync_duration_ms = elapsed_ms(start);
            return outcome;
        }

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
}

#[path = "heartbeat.rs"]
mod heartbeat;
#[path = "loading.rs"]
mod loading;
#[path = "observation.rs"]
mod observation;
#[path = "reconciliation.rs"]
mod reconciliation;

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
