// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Background-task framework — shared context, trigger types, and a
//! runner that spawns tasks with a common stop signal.
//!
//! Each background task (compaction, keep-alive, future health probing,
//! scanner) implements `BackgroundTask` and is registered with
//! `BgRunner`. The runner spawns each task, which loops: wait on its
//! trigger → `run_cycle(&ctx)` → log on err, continue on Ok. On
//! shutdown, the stop signal is notified and all tasks are awaited.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use tracing::{error, info};

use crate::ddb_config::DdbConfig;
use crate::ddb_kv_client::DdbKvClient;
use crate::metrics::DiskdbMetrics;
use crate::model::disk_group_container::DdbDiskGroupContainer;

/// Shared context passed to every background task.
pub struct BgCtx {
    pub container: Arc<DdbDiskGroupContainer>,
    pub kv: Arc<DdbKvClient>,
    pub metrics: DiskdbMetrics,
    /// Shared config handle — bg tasks read dynamic fields (timer
    /// intervals, thresholds) from this on each tick so config
    /// reloads take effect without restart.
    pub config: Arc<ArcSwap<DdbConfig>>,
}

/// What causes a background task to wake and run a cycle.
pub enum Trigger {
    /// Sleep for a fixed `Duration` between cycles.
    Timer(Duration),
    /// Sleep for a dynamically-computed `Duration` between cycles.
    /// The closure is called each tick to read the current interval
    /// from the shared config handle, so config reloads take effect
    /// on the next tick with no stop/restart.
    TimerFn(Box<dyn Fn() -> Duration + Send + Sync>),
    /// Wait on a `Notify` (woken externally) between cycles.
    Event(Arc<tokio::sync::Notify>),
}

/// Error from a background task cycle.
#[derive(Debug)]
pub struct BgError(pub String);

impl std::fmt::Display for BgError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for BgError {}

/// A boxed future returned by `run_cycle` (avoids `async_trait` dep).
pub type CycleFut<'a> = Pin<Box<dyn Future<Output = Result<(), BgError>> + Send + 'a>>;

/// One background task.
pub trait BackgroundTask: Send + Sync + 'static {
    /// One cycle of work. Called repeatedly per the trigger.
    /// Err on fatal; Ok on cycle-complete (loop continues).
    fn run_cycle<'a>(&'a self, ctx: &'a BgCtx) -> CycleFut<'a>;
    /// What wakes this task between cycles.
    fn trigger(&self) -> Trigger;
    /// Human-readable name for logging.
    fn name(&self) -> &'static str;
}

/// Spawns + manages background tasks with a shared stop signal.
pub struct BgRunner {
    tasks: Vec<Arc<dyn BackgroundTask>>,
    stop: Arc<tokio::sync::Notify>,
    stopped: Arc<AtomicBool>,
}

/// Handle used to signal shutdown to all `BgRunner` tasks. Replaces the
/// bare `Arc<Notify>` so the stop signal is race-free: `notify_waiters()`
/// sets an `AtomicBool` (the source of truth) before waking sleepers, so a
/// notification that arrives while a task is mid-cycle (not polling the
/// `Notify`) is not lost — the task sees the flag on the next loop.
#[derive(Clone)]
pub struct StopHandle {
    notify: Arc<tokio::sync::Notify>,
    flag: Arc<AtomicBool>,
}

impl StopHandle {
    /// Signal all tasks to stop. Sets the stop flag (source of truth)
    /// then wakes any task currently parked on the `Notify`.
    pub fn notify_waiters(&self) {
        self.flag.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }
}

impl BgRunner {
    #[must_use]
    pub fn new() -> Self {
        Self {
            tasks: Vec::new(),
            stop: Arc::new(tokio::sync::Notify::new()),
            stopped: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Register a background task.
    #[must_use]
    pub fn register(mut self, task: Arc<dyn BackgroundTask>) -> Self {
        self.tasks.push(task);
        self
    }

    /// Stop signal handle — call `notify_waiters()` to shut down all tasks.
    #[must_use]
    pub fn stop_handle(&self) -> StopHandle {
        StopHandle {
            notify: Arc::clone(&self.stop),
            flag: Arc::clone(&self.stopped),
        }
    }

    /// Spawn all registered tasks. Returns join handles.
    pub fn spawn(self, ctx: &Arc<BgCtx>) -> Vec<tokio::task::JoinHandle<()>> {
        let stop = Arc::clone(&self.stop);
        let stopped = Arc::clone(&self.stopped);
        self.tasks
            .into_iter()
            .map(|task| {
                let ctx = Arc::clone(ctx);
                let stop = Arc::clone(&stop);
                let stopped = Arc::clone(&stopped);
                spawn_task(task, ctx, stop, stopped)
            })
            .collect()
    }
}

impl Default for BgRunner {
    fn default() -> Self {
        Self::new()
    }
}

fn spawn_task(
    task: Arc<dyn BackgroundTask>,
    ctx: Arc<BgCtx>,
    stop: Arc<tokio::sync::Notify>,
    stopped: Arc<AtomicBool>,
) -> tokio::task::JoinHandle<()> {
    let name = task.name();
    tokio::spawn(async move {
        info!(task = name, "background task started");
        loop {
            // Check the stop flag first — covers the race where
            // notify_waiters() fired during the previous run_cycle
            // (when this task was not polling stop.notified()).
            if stopped.load(Ordering::Acquire) {
                info!(task = name, "background task stopped");
                return;
            }
            // Wait for trigger or stop.
            let stopped_now = tokio::select! {
                biased;
                () = stop.notified() => true,
                () = wait_trigger(&task) => false,
            };
            if stopped_now || stopped.load(Ordering::Acquire) {
                info!(task = name, "background task stopped");
                return;
            }
            // Run one cycle.
            let result = task.run_cycle(&ctx).await;
            match result {
                Ok(()) => {}
                Err(e) => {
                    error!(task = name, error = %e, "background task cycle failed");
                }
            }
        }
    })
}

async fn wait_trigger(task: &Arc<dyn BackgroundTask>) {
    match task.trigger() {
        Trigger::Timer(dur) => {
            tokio::time::sleep(dur).await;
        }
        Trigger::TimerFn(f) => {
            tokio::time::sleep(f()).await;
        }
        Trigger::Event(notify) => {
            notify.notified().await;
        }
    }
}
