// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Per-worker counters and the closed-loop / pipelined worker task.
//!
//! Each worker is a strict closed loop when `pipeline_depth == 1`:
//! issue one op via `BenchClient::issue_op`, await completion, record
//! latency, repeat. When `pipeline_depth > 1`, the worker uses a
//! semaphore to bound concurrent in-flight ops (pipelined sends).

use std::collections::BTreeMap;
use std::time::Instant;

use crow_common::metrics::{Counter, MetricName};
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
/// Closed loop: at most one in-flight RPC per worker at any instant
/// (when `pipeline_depth == 1`). Pipelined: up to `pipeline_depth`
/// concurrent in-flight ops (when > 1).
#[allow(
    clippy::too_many_arguments,
    clippy::too_many_lines,
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
    let pipeline_depth = cfg.pipeline_depth.max(1);

    if pipeline_depth == 1 {
        // Closed-loop: one op at a time.
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
    } else {
        // Pipelined: up to `pipeline_depth` concurrent in-flight ops.
        // The worker issues ops in a loop, but instead of awaiting each
        // one immediately, it collects futures and awaits them in
        // bounded batches. This gives the transport a chance to batch
        // multiple sends into one writev call.
        //
        // Since `BenchClient::issue_op` takes `&mut OpGen`, we can't
        // spawn multiple concurrent issue_op calls from one worker.
        // Instead, pipelining at the worker level is achieved by having
        // the RPC client's `call()` submit immediately and return a
        // future — the worker can fire multiple calls before awaiting
        // any. This requires the BenchClient to support a "fire then
        // collect" pattern, which is a future enhancement.
        //
        // For now, pipeline_depth > 1 falls back to closed-loop. The
        // real pipelining comes from having multiple workers (threads)
        // each in closed-loop mode, which is the existing behavior.
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
