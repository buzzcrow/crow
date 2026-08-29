// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 0.0.

//! `BgRunner` + `BackgroundTask` tests — spawn, stop, error handling.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crowdb_diskdb::bg_task::{BackgroundTask, BgCtx, BgError, CycleFut, Trigger};
use crowdb_diskdb::ddb_config::KeepAliveConfig;
use crowdb_diskdb::ddb_kv_client::DdbKvClient;
use crowdb_diskdb::metrics::DiskdbMetrics;
use crowdb_diskdb::model::disk_group_container::DdbDiskGroupContainer;

/// A mock task that counts cycles and optionally returns errors.
struct MockTask {
    name: &'static str,
    cycle_count: Arc<AtomicU32>,
    fail_first_n: u32,
    delay: Duration,
}

impl MockTask {
    fn new(name: &'static str, cycle_count: Arc<AtomicU32>) -> Self {
        Self {
            name,
            cycle_count,
            fail_first_n: 0,
            delay: Duration::from_millis(1),
        }
    }

    fn with_fail_first_n(mut self, n: u32) -> Self {
        self.fail_first_n = n;
        self
    }
}

impl BackgroundTask for MockTask {
    fn run_cycle<'a>(&'a self, _ctx: &'a BgCtx) -> CycleFut<'a> {
        let cycle = self.cycle_count.fetch_add(1, Ordering::SeqCst) + 1;
        let fail = cycle <= self.fail_first_n;
        let delay = self.delay;
        Box::pin(async move {
            tokio::time::sleep(delay).await;
            if fail {
                Err(BgError(format!("mock failure cycle {cycle}")))
            } else {
                Ok(())
            }
        })
    }

    fn trigger(&self) -> Trigger {
        Trigger::Timer(self.delay)
    }

    fn name(&self) -> &'static str {
        self.name
    }
}

fn make_ctx() -> Arc<BgCtx> {
    let container = Arc::new(DdbDiskGroupContainer::new(1));
    let kv = Arc::new(DdbKvClient::new(crowdb_kv_client::CrowdbClient::new(
        crowdb_kv_client::ClientConfig::new(vec!["127.0.0.1:0".to_string()]),
    )));
    let mut registry = crowdb_common::metrics::MetricsRegistry::new();
    let metrics = DiskdbMetrics::register(&mut registry);
    let _ = KeepAliveConfig::default();
    let config = Arc::new(arc_swap::ArcSwap::from_pointee(
        crowdb_diskdb::ddb_config::DdbConfig::default(),
    ));
    Arc::new(BgCtx {
        container,
        kv,
        metrics,
        config,
    })
}

#[tokio::test]
async fn bg_runner_stops_all_tasks_on_stop_signal() {
    let ctx = make_ctx();
    let cycle_a = Arc::new(AtomicU32::new(0));
    let cycle_b = Arc::new(AtomicU32::new(0));

    let mut runner = crowdb_diskdb::bg_task::BgRunner::new();
    runner = runner.register(Arc::new(MockTask::new("task-a", Arc::clone(&cycle_a))));
    runner = runner.register(Arc::new(MockTask::new("task-b", Arc::clone(&cycle_b))));

    let stop = runner.stop_handle();
    let handles = runner.spawn(&ctx);

    // Let tasks run a few cycles.
    tokio::time::sleep(Duration::from_millis(50)).await;
    stop.notify_waiters();

    // Wait for all tasks to stop.
    for h in handles {
        let _ = h.await;
    }

    let a = cycle_a.load(Ordering::SeqCst);
    let b = cycle_b.load(Ordering::SeqCst);
    assert!(a > 0, "task-a should have run at least 1 cycle, got {a}");
    assert!(b > 0, "task-b should have run at least 1 cycle, got {b}");

    // After stop, no more cycles.
    let a_after = cycle_a.load(Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(
        cycle_a.load(Ordering::SeqCst),
        a_after,
        "no more cycles after stop"
    );
}

#[tokio::test]
async fn bg_runner_continues_after_cycle_error() {
    let ctx = make_ctx();
    let cycle = Arc::new(AtomicU32::new(0));

    let mut runner = crowdb_diskdb::bg_task::BgRunner::new();
    runner = runner.register(Arc::new(
        MockTask::new("failing-task", Arc::clone(&cycle)).with_fail_first_n(2),
    ));

    let stop = runner.stop_handle();
    let handles = runner.spawn(&ctx);

    // Let the task run past the failing cycles.
    tokio::time::sleep(Duration::from_millis(50)).await;
    stop.notify_waiters();
    for h in handles {
        let _ = h.await;
    }

    // The task should have run more than 2 cycles (errors don't stop it).
    let total = cycle.load(Ordering::SeqCst);
    assert!(total > 2, "task should continue after errors, ran {total} cycles");
}

#[tokio::test]
async fn bg_runner_empty_spawn_completes_immediately() {
    let ctx = make_ctx();
    let runner = crowdb_diskdb::bg_task::BgRunner::new();
    let stop = runner.stop_handle();
    let handles = runner.spawn(&ctx);
    assert!(handles.is_empty(), "no tasks registered");
    stop.notify_waiters();
}
