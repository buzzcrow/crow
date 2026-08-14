// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Per-worker counters and the closed-loop worker task.
//!
//! Each worker is a strict closed loop: issue one op via
//! `kv.{get,put,delete,scan}().await`, await completion, record
//! latency, repeat. There is no internal queue and no per-worker
//! pipelining, so the number of in-flight requests at any instant is
//! exactly bounded by `cfg.threads`.

use std::collections::BTreeMap;
use std::time::Instant;

use crow_common::metrics::{Counter, MetricName};
use crow_kv_client::{CrowkvClient, Error as ClientError, GetOutcome, WriteOutcome};
use tracing::debug;

use super::report::{OpOutcome, OpStats};
use super::workload::{OpGen, OpKind, WorkloadKind};
use crate::bench::runner::BenchConfig;

/// Lock-free per-worker counters used by the optional progress
/// snapshotter and the metrics flusher. Workers bump these on every op
/// via `Counter::inc` — there is no contention because each worker owns
/// its `Arc<WorkerCounters>` exclusively. Per-op-kind ok/err counts let
/// the metrics log distinguish successful from failed operations. The
/// progress snapshotter reads cumulative totals via `snapshot().total`;
/// the metrics flusher reads window deltas via `flush().count`, dropping
/// its manual `prev_*` bookkeeping.
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
/// Combined across all workers, the runner-wide in-flight count is
/// bounded by `cfg.threads` exactly (see module-level docs).
///
/// `counters` is the worker's own `WorkerCounters` (shared with the
/// optional progress snapshotter). Both increments use `Relaxed` —
/// ordering doesn't matter since the snapshotter only sums and prints,
/// and the final report is computed from the returned `OpStats`.
#[allow(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    reason = "per-op dispatch loop; splitting per-kind reduces readability"
)]
pub(crate) async fn run_worker(
    kv: &CrowkvClient,
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
        // `recording` is sticky-`true` once the warmup window passes.
        // We re-evaluate per iteration to keep the implementation
        // robust to clock skew, but in practice it just flips once.
        let recording = now_pre >= measure_start;
        iter = iter.wrapping_add(1);

        let kind = match cfg.workload {
            WorkloadKind::Read => OpKind::Read,
            WorkloadKind::Write => OpKind::Write,
            WorkloadKind::List => OpKind::List,
            WorkloadKind::Mix => gen.pick_mix_kind(),
        };

        // Reads draw from the populated range (`next_read_key`) and
        // carry the key_id for spot-check verification; other ops draw
        // from the full `key_space`.
        let (key, read_key_id) = if kind == OpKind::Read {
            let (id, k) = gen.next_read_key();
            (k, Some(id))
        } else {
            (gen.next_key(), None)
        };
        let t0 = Instant::now();
        let outcome = match kind {
            OpKind::Read => {
                let min_slot = cfg.min_slot_policy.to_min_slot();
                match kv
                    .get(cfg.store_id, cfg.group_id, &key, cfg.read_mode, min_slot)
                    .await
                {
                    Ok(GetOutcome::Found { value, .. }) => {
                        // Spot-check `verify_bytes` random offsets
                        // against the deterministic formula. A
                        // mismatch is a correctness error (distinct
                        // from transport/NotLeader errors).
                        let ok_verify = read_key_id
                            .is_some_and(|id| gen.verify_value(id, value.as_ref(), cfg.verify_bytes));
                        OpOutcome {
                            ok: true,
                            correctness_error: !ok_verify,
                            ..Default::default()
                        }
                    }
                    Ok(GetOutcome::NotFound) => OpOutcome {
                        ok: true,
                        not_found: true,
                        ..Default::default()
                    },
                    Err(ClientError::NotLeader { .. }) => OpOutcome {
                        no_leader: true,
                        ..Default::default()
                    },
                    Err(_) => OpOutcome::default(),
                }
            }
            OpKind::Write => {
                let value = gen.make_value();
                let client_id = u64::from(worker_id) + 1;
                match kv
                    .put(cfg.store_id, cfg.group_id, &key, &value, Some((client_id, iter)))
                    .await
                {
                    Ok(WriteOutcome { .. }) => OpOutcome {
                        ok: true,
                        ..Default::default()
                    },
                    Err(ClientError::NotLeader { .. }) => OpOutcome {
                        no_leader: true,
                        ..Default::default()
                    },
                    Err(_) => OpOutcome::default(),
                }
            }
            OpKind::Delete => {
                let client_id = u64::from(worker_id) + 1;
                match kv
                    .delete(cfg.store_id, cfg.group_id, &key, Some((client_id, iter)))
                    .await
                {
                    Ok(_) => OpOutcome {
                        ok: true,
                        ..Default::default()
                    },
                    Err(ClientError::NotLeader { .. }) => OpOutcome {
                        no_leader: true,
                        ..Default::default()
                    },
                    Err(_) => OpOutcome::default(),
                }
            }
            OpKind::List => match kv
                .scan(
                    cfg.store_id,
                    cfg.group_id,
                    &cfg.scan_prefix,
                    &cfg.scan_start_after,
                    &[],
                    cfg.scan_limit,
                    cfg.read_mode,
                    cfg.min_slot_policy.to_min_slot(),
                    false,
                    None,
                )
                .await
            {
                Ok(_) => OpOutcome {
                    ok: true,
                    ..Default::default()
                },
                Err(ClientError::NotLeader { .. }) => OpOutcome {
                    no_leader: true,
                    ..Default::default()
                },
                Err(_) => OpOutcome::default(),
            },
        };
        // During the warmup window we drive the same RPC sequence so
        // pool channels stay warm and OpGen state advances normally,
        // but we throw the latency / error result away — neither the
        // histogram, the per-kind counters, nor the live atomic
        // counters are touched. This keeps cold-start spikes (TCP
        // slow-start, channel handshake, server first-touch caches)
        // out of the published percentiles.
        if recording {
            let lat_us = u64::try_from(t0.elapsed().as_micros()).unwrap_or(u64::MAX);
            stats.entry(kind).or_default().record(lat_us, outcome);

            // Live per-op counters: each worker owns its `WorkerCounters`
            // so the increments are uncontended.
            counters.record(kind, outcome.ok);
        }

        // Yield periodically so heavy worker counts cooperate.
        if iter % 64 == 0 {
            tokio::task::yield_now().await;
        }
    }

    debug!(worker_id, total = iter, "bench: worker stop");
    stats
}
