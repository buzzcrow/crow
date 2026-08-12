// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Background tasks for periodic progress output and metrics logging.
//!
//! `spawn_progress_snapshotter` emits a one-line stderr summary every
//! `progress_interval`. `spawn_metrics_flusher` writes per-op-kind
//! latency percentiles and client counters to a metrics log file every
//! 5 seconds.

use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;
use crow_common::metrics::PreciseHistogram;
use crow_kv_client::{CrowkvClient, WindowLatencySnapshot};

use super::report::percentiles_from_histogram;
use super::worker::WorkerCounters;

/// Spawn the optional progress snapshotter. Wakes every `interval`,
/// sums each worker's `Arc<WorkerCounters>` atomics, and emits one
/// human-readable line to stderr. The task self-terminates when wall
/// time crosses `deadline`. The snapshotter never blocks workers; it
/// only reads `Relaxed` atomics.
///
/// Output format (chosen to be greppable and copy-pastable):
///   `[+12s] ops=124000 qps=10333 err=0 nl_hint=0 xport_err=0`
///
/// Where `qps` is the **delta** ops since the previous tick divided by
/// the actual elapsed time between ticks (so it self-corrects if the
/// runtime can't quite hit the requested cadence). `nl_hint` and
/// `xport_err` are cumulative client-side counters for leader-redirect
/// and transport-error events.
pub(crate) fn spawn_progress_snapshotter(
    interval: Duration,
    started: Instant,
    deadline: Instant,
    counters: Vec<Arc<WorkerCounters>>,
    client: Arc<CrowkvClient>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut last_ops: u64 = 0;
        let mut last_tick = started;
        loop {
            tokio::time::sleep(interval).await;
            let now = Instant::now();
            // Clamp the final partial tick to the deadline so the last
            // progress line is still emitted with accurate QPS.
            let past_deadline = now >= deadline;
            let effective_now = if past_deadline { deadline } else { now };

            let total_ops: u64 = counters.iter().map(|c| c.total_ops()).sum();
            let total_err: u64 = counters.iter().map(|c| c.total_errors()).sum();
            let delta_ops = total_ops.saturating_sub(last_ops);
            let dt = effective_now.duration_since(last_tick).as_secs_f64().max(1e-9);
            #[allow(
                clippy::cast_precision_loss,
                clippy::cast_possible_truncation,
                clippy::cast_sign_loss
            )]
            let qps = (delta_ops as f64 / dt).round() as u64;
            let elapsed_s = effective_now.duration_since(started).as_secs();

            let cm = client.metrics();
            eprintln!(
                "[+{elapsed_s}s] ops={total_ops} qps={qps} err={total_err} nl_hint={} leader_query={} xport_err={}",
                cm.not_leader_hint_followed, cm.leader_query, cm.transport_error_retry,
            );

            last_ops = total_ops;
            last_tick = effective_now;

            if past_deadline {
                break;
            }
        }
    })
}

/// One row of cumulative latency output in the periodic metrics log.
struct CumLine {
    name: String,
    count: u64,
    tps: u64,
    avg: u64,
    p50: u64,
    p90: u64,
    p99: u64,
    p999: u64,
    max: u64,
}

/// Cumulative latency histograms maintained by the metrics flusher.
/// Each tick, the drained `WindowLatencySnapshot` from the client is
/// merged into these for run-wide `bench.*.lh` percentiles.
#[derive(Debug)]
struct CumulativeLatency {
    put: PreciseHistogram,
    get: PreciseHistogram,
    delete: PreciseHistogram,
    scan: PreciseHistogram,
    batch_write: PreciseHistogram,
}

impl Default for CumulativeLatency {
    fn default() -> Self {
        let mk = || PreciseHistogram::new(3);
        Self {
            put: mk(),
            get: mk(),
            delete: mk(),
            scan: mk(),
            batch_write: mk(),
        }
    }
}

impl CumulativeLatency {
    fn merge(&mut self, snap: &WindowLatencySnapshot) {
        self.put.add(&snap.put);
        self.get.add(&snap.get);
        self.delete.add(&snap.delete);
        self.scan.add(&snap.scan);
        self.batch_write.add(&snap.batch_write);
    }
}

/// Spawn a background task that flushes per-op-kind latency percentiles
/// and client counters to a metrics log file every 5 seconds. Format
/// mirrors the server-side `[metrics]` log for consistency.
///
/// Latency histograms (`client.*.lh`) are produced by the crow-kv-client
/// library via `flush_latencies`. Cumulative latency (`bench.*.lh`) and
/// per-op gauges (`bench.*.ops.g`, `bench.*.errors.g`) are produced by
/// the bench runner from `WorkerCounters` and accumulated window data.
/// Client error/retry counters (`client.*_errors.c`, etc.) are read
/// from the client metrics snapshot.
#[allow(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    clippy::uninlined_format_args
)]
pub(crate) fn spawn_metrics_flusher(
    interval: Duration,
    started: Instant,
    deadline: Instant,
    counters: Vec<Arc<WorkerCounters>>,
    client: Arc<CrowkvClient>,
    path: std::path::PathBuf,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        use std::fmt::Write;
        let mut last_tick = started;
        let mut prev_cm = client.metrics();
        let mut cum_lat = CumulativeLatency::default();

        loop {
            // Align each tick to a wall-clock interval boundary (e.g. :05,
            // :10, :15) so timestamps land on round multiples. dt is
            // computed from actual elapsed time so TPS stays accurate.
            let now_epoch = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default();
            let interval_ns = interval.as_nanos();
            let rem_ns = now_epoch.as_nanos() % interval_ns;
            let delay = interval
                .checked_sub(Duration::from_nanos(rem_ns.try_into().unwrap_or(u64::MAX)))
                .unwrap_or(interval);
            tokio::time::sleep(delay).await;
            let now = Instant::now();
            // If the aligned tick overshoots the deadline, clamp reporting
            // to the deadline so the final partial window is still emitted
            // (TPS stays accurate) instead of being silently dropped.
            let past_deadline = now >= deadline;
            let effective_now = if past_deadline { deadline } else { now };

            let dt = effective_now.duration_since(last_tick).as_secs_f64().max(1e-9);
            let elapsed = effective_now.duration_since(started).as_secs_f64().max(1e-9);
            let timestamp = Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string();

            // Drain window latency from the client library once.
            // The snapshot is used both for per-window flush and for
            // cumulative accumulation.
            let window_snap = client.drain_window();
            cum_lat.merge(&window_snap);

            let cm = client.metrics();

            // Per-window deltas from cumulative client error counters.
            let delta = |curr: u64, prev: u64| curr.saturating_sub(prev);
            let d_put_err = delta(cm.put_errors, prev_cm.put_errors);
            let d_get_err = delta(cm.get_errors, prev_cm.get_errors);
            let d_del_err = delta(cm.delete_errors, prev_cm.delete_errors);
            let d_scan_err = delta(cm.scan_errors, prev_cm.scan_errors);
            let d_bw_err = delta(cm.batch_write_errors, prev_cm.batch_write_errors);
            let d_nl_hint = delta(cm.not_leader_hint_followed, prev_cm.not_leader_hint_followed);
            let d_leader_q = delta(cm.leader_query, prev_cm.leader_query);
            let d_unknown = delta(cm.unknown_leader_wait, prev_cm.unknown_leader_wait);
            let d_xport = delta(cm.transport_error_retry, prev_cm.transport_error_retry);
            let d_retries = delta(cm.retries_exhausted, prev_cm.retries_exhausted);
            let d_no_leader = delta(cm.no_leader, prev_cm.no_leader);
            let d_topo = delta(cm.topology_refresh, prev_cm.topology_refresh);

            let cm_snap = cm.clone();
            prev_cm = cm;

            // Per-op bench counter window deltas from WorkerCounters.
            // `Counter::flush()` returns the window delta since the last
            // flush and resets the window, so no manual `prev_*`
            // bookkeeping is needed.
            let d_put_ok: u64 = counters.iter().map(|c| c.put_ok.flush().count).sum();
            let d_put_err_b: u64 = counters.iter().map(|c| c.put_err.flush().count).sum();
            let d_get_ok: u64 = counters.iter().map(|c| c.get_ok.flush().count).sum();
            let d_get_err_b: u64 = counters.iter().map(|c| c.get_err.flush().count).sum();
            let d_del_ok: u64 = counters.iter().map(|c| c.delete_ok.flush().count).sum();
            let d_del_err_b: u64 = counters.iter().map(|c| c.delete_err.flush().count).sum();
            let d_scan_ok: u64 = counters.iter().map(|c| c.scan_ok.flush().count).sum();
            let d_scan_err_b: u64 = counters.iter().map(|c| c.scan_err.flush().count).sum();

            // Column widths — match server defaults.
            let width = 24usize;
            let count_w = 5usize;
            let tps_w = 7usize;

            let mut out = String::new();
            let _ = writeln!(out, "[bench-metrics {timestamp} window={dt:.3}s]");

            // 1. Per-window latency histograms from the client library.
            client.flush_latencies(&mut out, &window_snap, dt);

            // 2. Cumulative latency histograms (bench.*.lh) with p90/p999.
            let mut cum_lines: Vec<CumLine> = Vec::new();
            let entries: [(&str, &PreciseHistogram); 5] = [
                ("bench.put.lh", &cum_lat.put),
                ("bench.get.lh", &cum_lat.get),
                ("bench.delete.lh", &cum_lat.delete),
                ("bench.scan.lh", &cum_lat.scan),
                ("bench.batch_write.lh", &cum_lat.batch_write),
            ];
            for (name, h) in &entries {
                if !h.is_empty() {
                    let p = percentiles_from_histogram(h);
                    cum_lines.push(CumLine {
                        name: (*name).to_string(),
                        count: h.len(),
                        tps: tps_calc(h.len(), elapsed),
                        avg: p.avg_us,
                        p50: p.p50_us,
                        p90: p.p90_us,
                        p99: p.p99_us,
                        p999: p.p999_us,
                        max: p.max_us,
                    });
                }
            }
            if !cum_lines.is_empty() {
                let _ = writeln!(
                    out,
                    "{:<width$}  {:>count_w$}  {:>tps_w$}  {:>8}  {:>8}  {:>8}  {:>8}  {:>8}  {:>8}",
                    "",
                    "count",
                    "tps(/s)",
                    "avg(us)",
                    "p50(us)",
                    "p90(us)",
                    "p99(us)",
                    "p999(us)",
                    "max(us)",
                    width = width,
                    count_w = count_w,
                    tps_w = tps_w,
                );
                for c in &cum_lines {
                    let name_w = c.name.len().max(width);
                    let _ = writeln!(
                        out,
                        "{:<name_w$}  {:>count_w$}  {:>tps_w$}  {:>8}  {:>8}  {:>8}  {:>8}  {:>8}  {:>8}",
                        c.name,
                        c.count,
                        c.tps,
                        c.avg,
                        c.p50,
                        c.p90,
                        c.p99,
                        c.p999,
                        c.max,
                        name_w = name_w,
                        count_w = count_w,
                        tps_w = tps_w,
                    );
                }
            }

            // 3. Bench per-op gauges (ok/err per window).
            let gauge_lines: Vec<(&str, u64)> = vec![
                ("bench.put.ops.g", d_put_ok),
                ("bench.put.errors.g", d_put_err_b),
                ("bench.get.ops.g", d_get_ok),
                ("bench.get.errors.g", d_get_err_b),
                ("bench.delete.ops.g", d_del_ok),
                ("bench.delete.errors.g", d_del_err_b),
                ("bench.scan.ops.g", d_scan_ok),
                ("bench.scan.errors.g", d_scan_err_b),
            ];
            let active_gauges: Vec<(&str, u64)> =
                gauge_lines.iter().filter(|(_, v)| *v > 0).copied().collect();
            if !active_gauges.is_empty() {
                let _ = writeln!(out, "{:<width$}  {:>8}", "", "value", width = width);
                for (name, val) in &active_gauges {
                    let name_w = name.len().max(width);
                    let _ = writeln!(out, "{:<name_w$}  {:>8}", name, val, name_w = name_w);
                }
            }

            // 4. Client error counters (from crow-kv-client library).
            let err_lines: Vec<(&str, u64, u64)> = vec![
                ("client.put_errors.c", d_put_err, cm_snap.put_errors),
                ("client.get_errors.c", d_get_err, cm_snap.get_errors),
                ("client.delete_errors.c", d_del_err, cm_snap.delete_errors),
                ("client.scan_errors.c", d_scan_err, cm_snap.scan_errors),
                (
                    "client.batch_write_errors.c",
                    d_bw_err,
                    cm_snap.batch_write_errors,
                ),
            ];
            let active_err: Vec<(&str, u64, u64)> =
                err_lines.iter().filter(|(_, d, _)| *d > 0).copied().collect();
            if !active_err.is_empty() {
                let _ = writeln!(
                    out,
                    "{:<width$}  {:>count_w$}  {:>tps_w$}  {:>8}",
                    "",
                    "count",
                    "tps(/s)",
                    "total",
                    width = width,
                    count_w = count_w,
                    tps_w = tps_w,
                );
                for (name, d, total) in &active_err {
                    let name_w = name.len().max(width);
                    let _ = writeln!(
                        out,
                        "{:<name_w$}  {:>count_w$}  {:>tps_w$}  {:>8}",
                        name,
                        d,
                        tps_calc(*d, dt),
                        total,
                        name_w = name_w,
                        count_w = count_w,
                        tps_w = tps_w,
                    );
                }
            }

            // 5. Client retry/leader counters.
            let retry_lines: Vec<(&str, u64, u64)> = vec![
                (
                    "client.not_leader_hint.c",
                    d_nl_hint,
                    cm_snap.not_leader_hint_followed,
                ),
                ("client.leader_query.c", d_leader_q, cm_snap.leader_query),
                (
                    "client.unknown_leader_wait.c",
                    d_unknown,
                    cm_snap.unknown_leader_wait,
                ),
                ("client.transport_error.c", d_xport, cm_snap.transport_error_retry),
                ("client.retries_exhausted.c", d_retries, cm_snap.retries_exhausted),
                ("client.no_leader.c", d_no_leader, cm_snap.no_leader),
                ("client.topology_refresh.c", d_topo, cm_snap.topology_refresh),
            ];
            let active_retry: Vec<(&str, u64, u64)> =
                retry_lines.iter().filter(|(_, d, _)| *d > 0).copied().collect();
            if !active_retry.is_empty() {
                let _ = writeln!(
                    out,
                    "{:<width$}  {:>count_w$}  {:>tps_w$}  {:>8}",
                    "",
                    "count",
                    "tps(/s)",
                    "total",
                    width = width,
                    count_w = count_w,
                    tps_w = tps_w,
                );
                for (name, d, total) in &active_retry {
                    let name_w = name.len().max(width);
                    let _ = writeln!(
                        out,
                        "{:<name_w$}  {:>count_w$}  {:>tps_w$}  {:>8}",
                        name,
                        d,
                        tps_calc(*d, dt),
                        total,
                        name_w = name_w,
                        count_w = count_w,
                        tps_w = tps_w,
                    );
                }
            }

            let _ = writeln!(out);

            if let Ok(mut f) = std::fs::OpenOptions::new().create(true).append(true).open(&path) {
                use std::io::Write;
                let _ = f.write_all(out.as_bytes());
            }

            last_tick = effective_now;

            if past_deadline {
                break;
            }
        }
    })
}

#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss
)]
fn tps_calc(count: u64, window_secs: f64) -> u64 {
    (count as f64 / window_secs) as u64
}
