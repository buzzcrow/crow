// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Parsing and aggregation of server-side `log/metrics.log` files.
//!
//! Split from `report.rs` so the core serialization structs stay
//! small and the metrics-log scanning logic is isolated.

use super::report::ServerMetrics;

/// Section a metrics-log line currently belongs to, tracked while
/// scanning `parse_metrics_log`'s input line by line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LogSection {
    None,
    Counter,
    Histogram,
    Summary,
    Bandwidth,
    Gauge,
    Misc,
}

/// Parse an `.rpc.*.c` counter line into the transport stats snapshot.
/// Legacy raw counters (`read_calls`, etc.) are no longer emitted — the
/// branches are kept for historical metrics logs but will not match
/// new logs.
fn parse_rpc_counter(name: &str, count: u64, rpc: &mut super::report::TransportStatsSnapshot) {
    if name.contains(".read_calls.") {
        rpc.read_calls += count;
    } else if name.contains(".writev_calls.") {
        rpc.writev_calls += count;
    } else if name.contains(".frames_sent.") {
        rpc.frames_sent += count;
    } else if name.contains(".frames_parsed.") {
        rpc.frames_parsed += count;
    } else if name.contains(".read_bytes.") {
        rpc.read_bytes += count;
    } else if name.contains(".writev_bytes.") {
        rpc.writev_bytes += count;
    }
}

/// Parse an `.rpc.*.g` gauge line into the transport stats snapshot.
fn parse_rpc_gauge(name: &str, value: u64, rpc: &mut super::report::TransportStatsSnapshot) {
    if name.contains(".submit_to_writev.") {
        if name.contains(".avg_us.") {
            rpc.submit_to_writev_avg_us = value;
        } else if name.contains(".count.") {
            rpc.submit_to_writev_count = value;
        }
    }
}

/// Update g.1 (active group) summary metrics: inflight wait avg and
/// inter-replica RPC latency/tps. Takes the max avg across flush
/// windows (steady state has the highest count). Summary format:
/// name count tps avg(us) max(us). Field indices: 2=tps, 3=avg(us).
fn update_g1_summary(metrics: &mut ServerMetrics, name: &str, fields: &[&str]) {
    if name.contains(".inflight_wait.") {
        let avg: u64 = fields.get(3).and_then(|f| f.parse().ok()).unwrap_or(0);
        if avg > metrics.inflight_wait_avg_us {
            metrics.inflight_wait_avg_us = avg;
        }
    }
    update_replica_metrics(&mut metrics.replica, name, fields);
}

/// Update inter-replica metrics from a summary line. Takes the max
/// across flush windows (steady state has the highest count/tps).
/// Summary format: name count tps avg(us) max(us).
/// Field indices: 2=tps, 3=avg(us).
fn update_replica_metrics(replica: &mut super::report::ReplicaMetrics, name: &str, fields: &[&str]) {
    let tps: u64 = fields.get(2).and_then(|f| f.parse().ok()).unwrap_or(0);
    let avg: u64 = fields.get(3).and_then(|f| f.parse().ok()).unwrap_or(0);
    if name.ends_with(".rpc.l@2") {
        if tps > replica.r2_tps {
            replica.r2_tps = tps;
        }
        if avg > replica.r2 {
            replica.r2 = avg;
        }
    } else if name.ends_with(".rpc.l@3") {
        if tps > replica.r3_tps {
            replica.r3_tps = tps;
        }
        if avg > replica.r3 {
            replica.r3 = avg;
        }
    }
}

/// Parse a `crow-kv-server` `log/metrics.log` file's full contents into a
/// [`ServerMetrics`] summary spanning every flush block in the file.
///
/// Counters/histograms/summaries print only the *window delta* count per
/// flush (see `crow_kv::metrics::mod::flush_histograms`/`flush_summaries`),
/// so per-run totals for KV ops and WAL appends are the sum of the count
/// column across every block. `sys.*` CPU/TCP lines are deltas since the
/// previous flush (summed); `sys.rss_kb` is an absolute point-in-time
/// snapshot (tracked as the run's peak).
#[must_use]
pub(crate) fn parse_metrics_log(content: &str) -> ServerMetrics {
    let mut metrics = ServerMetrics::default();
    let mut section = LogSection::None;
    let mut peak_rss_kb = 0u64;

    for raw_line in content.lines() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with("[metrics ") {
            section = LogSection::None;
            continue;
        }
        if line == "misc" {
            section = LogSection::Misc;
            continue;
        }
        if line.starts_with("count") {
            section = if line.contains("p50") {
                LogSection::Histogram
            } else if line.contains("avg_size") {
                LogSection::Bandwidth
            } else if line.contains("avg(us)") && !line.contains("p50") {
                LogSection::Summary
            } else if line.contains("total") {
                LogSection::Counter
            } else if line.contains("value") {
                LogSection::Gauge
            } else {
                LogSection::None
            };
            continue;
        }
        if line == "value" {
            section = LogSection::Gauge;
            continue;
        }

        let fields: Vec<&str> = line.split_whitespace().collect();
        if fields.len() < 2 {
            continue;
        }
        match section {
            LogSection::Histogram => {
                let name = fields[0];
                let count: u64 = fields[1].parse().unwrap_or(0);
                if name.contains(".kv.put.") {
                    metrics.kv_put_count += count;
                } else if name.contains(".kv.get.") {
                    metrics.kv_get_count += count;
                }
            }
            LogSection::Summary => {
                let name = fields[0];
                let count: u64 = fields[1].parse().unwrap_or(0);
                if name.contains(".wal.") && name.contains(".append.") {
                    metrics.wal_append_count += count;
                }
                // g.1 summaries: inflight wait + inter-replica RPC.
                if name.contains(".g.1.") {
                    update_g1_summary(&mut metrics, name, &fields);
                }
            }
            LogSection::Misc => {
                let value: u64 = fields[1].parse().unwrap_or(0);
                match fields[0] {
                    "sys.cpu_user_us" => metrics.system.cpu_user_us += value,
                    "sys.cpu_sys_us" => metrics.system.cpu_sys_us += value,
                    "sys.rss_kb" => peak_rss_kb = peak_rss_kb.max(value),
                    "sys.tcp_retrans" => metrics.system.tcp_retransmits += value,
                    "sys.tcp_lost" => metrics.system.tcp_lost += value,
                    _ => {}
                }
            }
            LogSection::Counter => {
                let name = fields[0];
                let count: u64 = fields[1].parse().unwrap_or(0);
                if name.contains(".wal.") && name.contains(".logical_bytes.") {
                    metrics.wal_logical_bytes += count;
                } else if name.contains(".wal.") && name.contains(".physical_bytes.") {
                    metrics.wal_physical_bytes += count;
                } else if name.contains(".wal.") && name.contains(".rmw.") {
                    metrics.wal_rmw_count += count;
                } else if name.contains(".inflight_enqueued.") {
                    metrics.inflight_enqueued += count;
                } else if name.contains(".rpc.") {
                    parse_rpc_counter(name, count, &mut metrics.rpc);
                }
            }
            LogSection::Gauge => {
                let name = fields[0];
                let value: u64 = fields[1].parse().unwrap_or(0);
                if name.contains(".rpc.") {
                    parse_rpc_gauge(name, value, &mut metrics.rpc);
                }
            }
            LogSection::Bandwidth | LogSection::None => {}
        }
    }

    metrics.system.rss_kb = peak_rss_kb;
    metrics
}

/// Aggregate per-node [`ServerMetrics`] into a single cluster-wide summary:
/// counters (WAL append, KV put/get) and CPU time are summed across nodes;
/// RSS and TCP retransmit/lost counters take the max across nodes.
#[must_use]
pub(crate) fn aggregate_server_metrics(per_node: &[ServerMetrics]) -> ServerMetrics {
    let mut agg = ServerMetrics::default();
    for m in per_node {
        agg.wal_append_count += m.wal_append_count;
        agg.kv_put_count += m.kv_put_count;
        agg.kv_get_count += m.kv_get_count;
        agg.wal_logical_bytes += m.wal_logical_bytes;
        agg.wal_physical_bytes += m.wal_physical_bytes;
        agg.wal_rmw_count += m.wal_rmw_count;
        agg.system.cpu_user_us += m.system.cpu_user_us;
        agg.system.cpu_sys_us += m.system.cpu_sys_us;
        agg.system.rss_kb = agg.system.rss_kb.max(m.system.rss_kb);
        agg.system.tcp_retransmits = agg.system.tcp_retransmits.max(m.system.tcp_retransmits);
        agg.system.tcp_lost = agg.system.tcp_lost.max(m.system.tcp_lost);
        agg.rpc.read_calls += m.rpc.read_calls;
        agg.rpc.writev_calls += m.rpc.writev_calls;
        agg.rpc.frames_sent += m.rpc.frames_sent;
        agg.rpc.frames_parsed += m.rpc.frames_parsed;
        agg.rpc.read_bytes += m.rpc.read_bytes;
        agg.rpc.writev_bytes += m.rpc.writev_bytes;
        agg.rpc.submit_to_writev_count += m.rpc.submit_to_writev_count;
        agg.rpc.submit_to_writev_avg_us = agg.rpc.submit_to_writev_avg_us.max(m.rpc.submit_to_writev_avg_us);
        // Inter-replica: leader-side metrics (rpc.l@2/3) take the max
        // across nodes (only the leader has non-zero values).
        agg.replica.r2 = agg.replica.r2.max(m.replica.r2);
        agg.replica.r2_tps = agg.replica.r2_tps.max(m.replica.r2_tps);
        agg.replica.r3 = agg.replica.r3.max(m.replica.r3);
        agg.replica.r3_tps = agg.replica.r3_tps.max(m.replica.r3_tps);
        // Inflight: enqueued is summed (total window-full hits across
        // nodes), wait avg takes max (only the leader enqueues).
        agg.inflight_enqueued += m.inflight_enqueued;
        agg.inflight_wait_avg_us = agg.inflight_wait_avg_us.max(m.inflight_wait_avg_us);
    }
    agg
}

#[cfg(test)]
mod tests {
    use super::*;
    use crow_common::metrics::SystemMetrics;

    /// `parse_metrics_log` sums histogram/summary window-delta counts
    /// across multiple flush blocks, sums CPU/TCP misc deltas, and tracks
    /// peak RSS — mirroring two ticks of `MetricsRunner::flush`.
    #[test]
    fn parse_metrics_log_sums_across_flush_blocks() {
        let log = "\
[metrics 2026-07-17T00:00:00.000Z window=1s]
                          count  tps(/s)  avg(us)  p50(us)  p99(us)  max(us)
s.1.kv.put.lh               10       10       50       48       90       95
s.1.kv.get.lh                5        5       20       19       30       31
                          count  tps(/s)  avg(us)  max(us)
s.1.g.1.wal.file.append.l   10       10       12       20
                          count  tps(/s)  total
s.1.g.1.wal.file.logical_bytes.c    10240     10   10240
s.1.g.1.wal.file.physical_bytes.c   12288     10   12288
s.1.g.1.wal.file.rmw.c                  5      5       5
misc
sys.cpu_user_us  1000
sys.cpu_sys_us   200
sys.rss_kb       51200
sys.tcp_retrans  0
sys.tcp_lost     0

[metrics 2026-07-17T00:00:01.000Z window=1s]
                          count  tps(/s)  avg(us)  p50(us)  p99(us)  max(us)
s.1.kv.put.lh               20       20       55       50       99      101
                          count  tps(/s)  avg(us)  max(us)
s.1.g.1.wal.file.append.l   20       20       13       22
                          count  tps(/s)  total
s.1.g.1.wal.file.logical_bytes.c    20480     20   30720
s.1.g.1.wal.file.physical_bytes.c   24576     20   36864
s.1.g.1.wal.file.rmw.c                 10     10      15
misc
sys.cpu_user_us  1100
sys.cpu_sys_us   210
sys.rss_kb       61440
sys.tcp_retrans  2
sys.tcp_lost     1
";
        let m = parse_metrics_log(log);
        assert_eq!(m.kv_put_count, 30); // 10 + 20, kv.get absent in 2nd block
        assert_eq!(m.kv_get_count, 5);
        assert_eq!(m.wal_append_count, 30); // 10 + 20
        assert_eq!(m.wal_logical_bytes, 30720); // 10240 + 20480
        assert_eq!(m.wal_physical_bytes, 36864); // 12288 + 24576
        assert_eq!(m.wal_rmw_count, 15); // 5 + 10
        assert_eq!(m.system.cpu_user_us, 2100); // 1000 + 1100
        assert_eq!(m.system.cpu_sys_us, 410); // 200 + 210
        assert_eq!(m.system.rss_kb, 61440); // peak across blocks
        assert_eq!(m.system.tcp_retransmits, 2);
        assert_eq!(m.system.tcp_lost, 1);
    }

    #[test]
    fn parse_metrics_log_empty_content_is_default() {
        assert_eq!(parse_metrics_log(""), ServerMetrics::default());
    }

    #[test]
    fn aggregate_server_metrics_sums_counters_and_maxes_system() {
        let node_a = ServerMetrics {
            wal_append_count: 10,
            kv_put_count: 5,
            kv_get_count: 3,
            wal_logical_bytes: 10240,
            wal_physical_bytes: 12288,
            wal_rmw_count: 5,
            system: SystemMetrics {
                cpu_user_us: 100,
                cpu_sys_us: 20,
                rss_kb: 50_000,
                tcp_retransmits: 1,
                tcp_lost: 0,
            },
            ..Default::default()
        };
        let node_b = ServerMetrics {
            wal_append_count: 15,
            kv_put_count: 7,
            kv_get_count: 4,
            wal_logical_bytes: 20480,
            wal_physical_bytes: 24576,
            wal_rmw_count: 10,
            system: SystemMetrics {
                cpu_user_us: 90,
                cpu_sys_us: 30,
                rss_kb: 60_000,
                tcp_retransmits: 3,
                tcp_lost: 2,
            },
            ..Default::default()
        };
        let agg = aggregate_server_metrics(&[node_a, node_b]);
        assert_eq!(agg.wal_append_count, 25);
        assert_eq!(agg.kv_put_count, 12);
        assert_eq!(agg.kv_get_count, 7);
        assert_eq!(agg.wal_logical_bytes, 30720);
        assert_eq!(agg.wal_physical_bytes, 36864);
        assert_eq!(agg.wal_rmw_count, 15);
        assert_eq!(agg.system.cpu_user_us, 190);
        assert_eq!(agg.system.cpu_sys_us, 50);
        assert_eq!(agg.system.rss_kb, 60_000);
        assert_eq!(agg.system.tcp_retransmits, 3);
        assert_eq!(agg.system.tcp_lost, 2);
    }
}
