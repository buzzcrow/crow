// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Per-worker counters and the closed-loop worker task.
//!
//! Each worker is a strict closed loop: issue one op via
//! `BenchClient::issue_op`, await completion, record latency, repeat.
//! Concurrency comes from having multiple workers (threads) each in
//! closed-loop mode.

use std::collections::BTreeMap;
use std::time::Instant;

use crowdb_common::metrics::{Counter, MetricName};
use tracing::debug;

use super::report::{OpOutcome, OpStats};
use super::target::BenchClient;
use super::workload::{OpGen, OpKind, WorkloadKind};
use crate::bench::runner::BenchConfig;

/// Lock-free per-worker counters used by the optional progress
/// snapshotter and the metrics flusher. Workers bump these on every op
/// via `Counter::inc` — there is no contention because each worker owns
/// its `Arc<WorkerCounters>` exclusively. Per-op-kind ok/err counts let
/// the metrics log distinguish successful from failed operations.
#[derive(Debug)]
pub(crate) struct WorkerCounters {
    pub(crate) put_ok: Counter,
    pub(crate) put_err: Counter,
    pub(crate) get_ok: Counter,
    pub(crate) get_err: Counter,
    pub(crate) delete_ok: Counter,
    pub(crate) delete_err: Counter,
    pub(crate) scan_ok: Counter,
    pub(crate) scan_err: Counter,
}

impl WorkerCounters {
    #[must_use]
    pub(crate) fn new() -> Self {
        Self {
            put_ok: Counter::new(MetricName::Static("bench.put.ok")),
            put_err: Counter::new(MetricName::Static("bench.put.err")),
            get_ok: Counter::new(MetricName::Static("bench.get.ok")),
            get_err: Counter::new(MetricName::Static("bench.get.err")),
            delete_ok: Counter::new(MetricName::Static("bench.delete.ok")),
            delete_err: Counter::new(MetricName::Static("bench.delete.err")),
            scan_ok: Counter::new(MetricName::Static("bench.scan.ok")),
            scan_err: Counter::new(MetricName::Static("bench.scan.err")),
        }
    }

    pub(crate) fn total_ops(&self) -> u64 {
        self.put_ok.snapshot().total
            + self.get_ok.snapshot().total
            + self.delete_ok.snapshot().total
            + self.scan_ok.snapshot().total
    }

    pub(crate) fn total_errors(&self) -> u64 {
        self.put_err.snapshot().total
            + self.get_err.snapshot().total
            + self.delete_err.snapshot().total
            + self.scan_err.snapshot().total
    }

    pub(crate) fn record(&self, kind: OpKind, ok: bool) {
        match (kind, ok) {
            (OpKind::Write, true) => self.put_ok.inc(),
            (OpKind::Write, false) => self.put_err.inc(),
            (OpKind::Read, true) => self.get_ok.inc(),
            (OpKind::Read, false) => self.get_err.inc(),
            (OpKind::Delete, true) => self.delete_ok.inc(),
            (OpKind::Delete, false) => self.delete_err.inc(),
            (OpKind::List, true) => self.scan_ok.inc(),
            (OpKind::List, false) => self.scan_err.inc(),
        }
    }
}

/// Run a single worker until `deadline`, returning its local per-op
/// stats. Errors during ops are recorded in the histogram (with `ok=false`)
/// rather than aborting the worker.
///
/// Closed loop: at most one in-flight RPC per worker at any instant.
#[allow(
    clippy::too_many_arguments,
    reason = "per-op dispatch loop; splitting per-kind reduces readability"
)]
pub(crate) async fn run_worker<C: BenchClient>(
    client: &C,
    gen: &mut OpGen,
    cfg: &BenchConfig,
    measure_start: Instant,
    deadline: Instant,
    worker_id: u32,
    counters: &WorkerCounters,
) -> BTreeMap<OpKind, OpStats> {
    let mut stats: BTreeMap<OpKind, OpStats> = BTreeMap::new();
    let mut iter: u64 = 0;

    loop {
        let now_pre = Instant::now();
        if now_pre >= deadline {
            break;
        }
        let recording = now_pre >= measure_start;
        iter = iter.wrapping_add(1);

        let kind = pick_kind(cfg, gen);
        let t0 = Instant::now();
        let outcome = client.issue_op(kind, gen, cfg, worker_id, iter).await;
        if recording {
            record_op(&mut stats, counters, kind, outcome, t0);
        }
        if iter % 64 == 0 {
            tokio::task::yield_now().await;
        }
    }

    debug!(worker_id, total = iter, "bench: worker stop");
    stats
}

/// Pick the op kind for this iteration based on the workload config.
fn pick_kind(cfg: &BenchConfig, gen: &mut OpGen) -> OpKind {
    match cfg.workload {
        WorkloadKind::Read => OpKind::Read,
        WorkloadKind::Write => OpKind::Write,
        WorkloadKind::List => OpKind::List,
        WorkloadKind::Mix => gen.pick_mix_kind(),
    }
}

/// Record one op's latency + outcome into stats + counters.
fn record_op(
    stats: &mut BTreeMap<OpKind, OpStats>,
    counters: &WorkerCounters,
    kind: OpKind,
    outcome: OpOutcome,
    t0: Instant,
) {
    let lat_us = u64::try_from(t0.elapsed().as_micros()).unwrap_or(u64::MAX);
    stats.entry(kind).or_default().record(lat_us, outcome);
    counters.record(kind, outcome.ok);
}
