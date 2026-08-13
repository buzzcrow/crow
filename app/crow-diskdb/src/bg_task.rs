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
use std::sync::Arc;
use std::time::Duration;

use tracing::{error, info};

use crate::data_group_client::DataGroupClient;
use crate::metrics::DiskdbMetrics;
use crate::model::disk_group_container::DdbDiskGroupContainer;

/// Shared context passed to every background task.
pub struct BgCtx {
    pub container: Arc<DdbDiskGroupContainer>,
    pub kv: Arc<DataGroupClient>,
    pub metrics: DiskdbMetrics,
}

/// What causes a background task to wake and run a cycle.
pub enum Trigger {
    /// Sleep for `Duration` between cycles.
    Timer(Duration),
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
}

impl BgRunner {
    #[must_use]
    pub fn new() -> Self {
        Self {
            tasks: Vec::new(),
            stop: Arc::new(tokio::sync::Notify::new()),
        }
    }

    /// Register a background task.
    #[must_use]
    pub fn register(mut self, task: Arc<dyn BackgroundTask>) -> Self {
        self.tasks.push(task);
        self
    }

    /// Stop signal handle — pass to `Notify` to shut down all tasks.
    #[must_use]
    pub fn stop_handle(&self) -> Arc<tokio::sync::Notify> {
        Arc::clone(&self.stop)
    }

    /// Spawn all registered tasks. Returns join handles.
    pub fn spawn(self, ctx: &Arc<BgCtx>) -> Vec<tokio::task::JoinHandle<()>> {
        let stop = Arc::clone(&self.stop);
        self.tasks
            .into_iter()
            .map(|task| {
                let ctx = Arc::clone(ctx);
                let stop = Arc::clone(&stop);
                spawn_task(task, ctx, stop)
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
) -> tokio::task::JoinHandle<()> {
    let name = task.name();
    tokio::spawn(async move {
        info!(task = name, "background task started");
        loop {
            // Wait for trigger or stop.
            let stopped = tokio::select! {
                biased;
                () = stop.notified() => true,
                () = wait_trigger(&task) => false,
            };
            if stopped {
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
        Trigger::Event(notify) => {
            notify.notified().await;
        }
    }
}
