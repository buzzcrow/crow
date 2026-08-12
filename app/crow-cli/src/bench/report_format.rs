// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Markdown and human-readable formatting for `BenchReport`.
//!
//! Split from `report.rs` so the core serialization structs stay
//! small and the display logic is isolated.

use std::collections::HashMap;
use std::fmt::Write;
use std::path::Path;

use super::report::BenchReport;

impl BenchReport {
    /// Generate a comprehensive Markdown report covering test
    /// configuration, cluster topology, per-op results, and server-side
    /// metrics. Uses bullet lists with bold labels for readability.
    #[allow(
        clippy::too_many_lines,
        reason = "display formatter, splitting reduces readability"
    )]
    #[must_use]
    pub(crate) fn markdown_report(
        &self,
        node_ids: &[u64],
        workspace_dir: &Path,
        endpoint_map: &HashMap<String, String>,
    ) -> String {
        let mut out = String::new();

        let _ = writeln!(out, "# CrowKV Benchmark Report");
        let _ = writeln!(out);

        // ── Run Info ──
        let _ = writeln!(out, "## Run Info");
        let _ = writeln!(out);
        let _ = writeln!(out, "- **run_id:** {}", self.run_id);
        let _ = writeln!(
            out,
            "- **started_at:** {}",
            self.started_at.format("%Y-%m-%d %H:%M:%S UTC")
        );
        let _ = writeln!(
            out,
            "- **finished_at:** {}",
            self.finished_at.format("%Y-%m-%d %H:%M:%S UTC")
        );
        let _ = writeln!(
            out,
            "- **duration:** {} ms (measurement window)",
            self.duration_ms
        );
        if self.warmup_ms > 0 {
            let _ = writeln!(out, "- **warmup:** {} ms (discarded)", self.warmup_ms);
        }
        if self.pre_pop_ms > 0 {
            let _ = writeln!(
                out,
                "- **pre_populate:** {} ms, {} errors",
                self.pre_pop_ms, self.pre_pop_errors
            );
        }
        if self.correctness_errors > 0 {
            let _ = writeln!(
                out,
                "- **correctness_errors:** {} (spot-check mismatches)",
                self.correctness_errors
            );
        }
        let _ = writeln!(out);

        // ── Test Configuration ──
        let _ = writeln!(out, "## Test Configuration");
        let _ = writeln!(out);
        let _ = writeln!(out, "- **workload:** {:?}", self.workload);
        let _ = writeln!(out, "- **storage_mode:** {}", self.mode);
        let _ = writeln!(out, "- **connections:** {} (gRPC channels)", self.connections);
        let _ = writeln!(out, "- **threads:** {} (worker tasks, closed-loop)", self.threads);
        let _ = writeln!(out, "- **key_space:** {} keys", self.key_space);
        let _ = writeln!(out, "- **value_size:** {} bytes", self.value_size);
        let _ = writeln!(out, "- **target_endpoint:** {}", self.target_endpoint);
        let _ = writeln!(
            out,
            "- **store_id / group_id:** {} / {}",
            self.store_id, self.group_id
        );
        let _ = writeln!(out);

        // ── Cluster Topology ──
        let _ = writeln!(out, "## Cluster Topology");
        let _ = writeln!(out);
        let _ = writeln!(out, "- **nodes:** {} (3-replica Paxos group)", node_ids.len());
        for (i, nid) in node_ids.iter().enumerate() {
            let _ = writeln!(out, "  - node[{i}]: {nid}");
        }
        let _ = writeln!(out, "- **workspace:** {}", workspace_dir.display());
        let (wal_desc, kv_desc) = match self.mode.as_str() {
            "mem" => (
                "mem-block (in-memory, no disk I/O)",
                "crow-tree + mem-block page store (in-memory, no disk I/O)",
            ),
            "file" => (
                "file (file-backed, no O_DIRECT)",
                "crow-tree + file page store (file-backed, no O_DIRECT)",
            ),
            "block-device" => (
                "block-device (O_DIRECT, 4K aligned)",
                "crow-tree + block page store (O_DIRECT, 4K aligned)",
            ),
            other => (other, other),
        };
        let _ = writeln!(out, "- **WAL backend:** {wal_desc}");
        let _ = writeln!(out, "- **KV engine backend:** {kv_desc}");
        let _ = writeln!(out);

        // ── Client-side Results ──
        let _ = writeln!(out, "## Client-side Results");
        let _ = writeln!(out);
        #[allow(
            clippy::cast_precision_loss,
            reason = "display-only QPS, precision loss irrelevant"
        )]
        let secs = self.duration_ms as f64 / 1000.0;
        #[allow(
            clippy::cast_precision_loss,
            reason = "display-only QPS, precision loss irrelevant"
        )]
        let qps = if secs > 0.0 {
            self.total_ops as f64 / secs
        } else {
            0.0
        };
        let _ = writeln!(out, "- **total_ops (success):** {}", self.total_ops);
        let _ = writeln!(out, "- **total_attempts:** {}", self.total_attempts);
        let _ = writeln!(out, "- **total_errors:** {}", self.total_errors);
        let _ = writeln!(out, "- **error_rate:** {:.4}", self.error_rate);
        let _ = writeln!(out, "- **throughput:** {qps:.1} ops/s (success only)");
        let _ = writeln!(out);

        // ── Per-Op Breakdown ──
        if !self.by_op.is_empty() {
            let _ = writeln!(out, "## Per-Op Breakdown");
            let _ = writeln!(out);
            for (kind, op) in &self.by_op {
                let p = &op.latency_us;
                #[allow(
                    clippy::cast_precision_loss,
                    reason = "display-only QPS, precision loss irrelevant"
                )]
                let op_qps = if secs > 0.0 { op.ops as f64 / secs } else { 0.0 };
                let _ = writeln!(out, "### {kind}");
                let _ = writeln!(out);
                let _ = writeln!(out, "- **ops (success):** {} ({op_qps:.1} ops/s)", op.ops);
                let _ = writeln!(out, "- **attempts:** {}", op.attempts);
                let _ = writeln!(out, "- **errors:** {}", op.errors);
                if op.no_leader > 0 {
                    let _ = writeln!(out, "- **no_leader:** {}", op.no_leader);
                }
                if op.not_found > 0 {
                    let _ = writeln!(out, "- **not_found:** {}", op.not_found);
                }
                if op.correctness_errors > 0 {
                    let _ = writeln!(out, "- **correctness_errors:** {}", op.correctness_errors);
                }
                let _ = writeln!(
                    out,
                    "- **latency (us):** min={} avg={} p50={} p90={} p99={} p999={} max={}",
                    p.min_us, p.avg_us, p.p50_us, p.p90_us, p.p99_us, p.p999_us, p.max_us
                );
                let _ = writeln!(out);
            }
        }

        // ── Client-side Metrics ──
        let cm = &self.client_metrics;
        let _ = writeln!(out, "## Client-side Metrics");
        let _ = writeln!(out);
        let _ = writeln!(out, "- **put_errors:** {}", cm.put_errors);
        let _ = writeln!(out, "- **get_errors:** {}", cm.get_errors);
        let _ = writeln!(out, "- **delete_errors:** {}", cm.delete_errors);
        let _ = writeln!(out, "- **scan_errors:** {}", cm.scan_errors);
        let _ = writeln!(out, "- **batch_write_errors:** {}", cm.batch_write_errors);
        let _ = writeln!(
            out,
            "- **not_leader_hint_followed:** {}",
            cm.not_leader_hint_followed
        );
        let _ = writeln!(out, "- **leader_query:** {}", cm.leader_query);
        let _ = writeln!(out, "- **unknown_leader_wait:** {}", cm.unknown_leader_wait);
        let _ = writeln!(out, "- **transport_error_retry:** {}", cm.transport_error_retry);
        let _ = writeln!(out, "- **retries_exhausted:** {}", cm.retries_exhausted);
        let _ = writeln!(out, "- **no_leader:** {}", cm.no_leader);
        let _ = writeln!(out, "- **topology_refresh:** {}", cm.topology_refresh);
        let _ = writeln!(out);

        // ── Leader Change Analysis ──
        let changes = &cm.leader_changes;
        if changes.is_empty() {
            let _ = writeln!(out, "## Leader Change Analysis");
            let _ = writeln!(out);
            let _ = writeln!(out, "- no leader changes detected");
            let _ = writeln!(out);
        } else {
            let total_recovery_ms: u64 = changes.iter().map(|c| c.recovery_ms).sum();
            let max_recovery_ms = changes.iter().map(|c| c.recovery_ms).max().unwrap_or(0);
            let min_recovery_ms = changes.iter().map(|c| c.recovery_ms).min().unwrap_or(0);
            let avg_recovery_ms = total_recovery_ms / changes.len() as u64;
            let _ = writeln!(out, "## Leader Change Analysis");
            let _ = writeln!(out);
            let _ = writeln!(out, "- **count:** {}", changes.len());
            let _ = writeln!(out, "- **total_recovery:** {total_recovery_ms} ms");
            let _ = writeln!(out, "- **avg_recovery:** {avg_recovery_ms} ms");
            let _ = writeln!(out, "- **min_recovery:** {min_recovery_ms} ms");
            let _ = writeln!(out, "- **max_recovery:** {max_recovery_ms} ms");
            let _ = writeln!(out);
            let _ = writeln!(out, "### Episodes");
            let _ = writeln!(out);
            let resolve = |ep: &str| -> String {
                let normalized = ep.strip_prefix("http://").unwrap_or(ep);
                endpoint_map
                    .get(ep)
                    .or_else(|| endpoint_map.get(normalized))
                    .cloned()
                    .unwrap_or_else(|| ep.to_string())
            };
            for (i, c) in changes.iter().enumerate() {
                let detected_at = chrono::DateTime::from_timestamp_millis(
                    i64::try_from(c.detected_at_ms).unwrap_or(i64::MAX),
                )
                .map_or_else(
                    || format!("{}ms", c.detected_at_ms),
                    |dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string(),
                );
                let _ = writeln!(
                    out,
                    "- **[{}]** `{} -> {}` trigger={} recovery={}ms detected_at={}",
                    i,
                    resolve(&c.old_endpoint),
                    resolve(&c.new_endpoint),
                    c.trigger,
                    c.recovery_ms,
                    detected_at,
                );
            }
            let _ = writeln!(out);
        }

        // ── Server-side Metrics ──
        let sm = &self.server_metrics;
        let _ = writeln!(out, "## Server-side Metrics (aggregated across nodes)");
        let _ = writeln!(out);
        let _ = writeln!(out, "- **wal_append_count:** {}", sm.wal_append_count);
        let _ = writeln!(out, "- **kv_put_count:** {}", sm.kv_put_count);
        let _ = writeln!(out, "- **kv_get_count:** {}", sm.kv_get_count);
        if sm.wal_logical_bytes > 0 || sm.wal_physical_bytes > 0 || sm.wal_rmw_count > 0 {
            let _ = writeln!(out);
            let _ = writeln!(out, "### WAL Block Device Metrics");
            let _ = writeln!(out);
            let _ = writeln!(out, "- **wal_logical_bytes:** {}", sm.wal_logical_bytes);
            let _ = writeln!(out, "- **wal_physical_bytes:** {}", sm.wal_physical_bytes);
            #[allow(clippy::cast_precision_loss, reason = "display-only ratio")]
            let amplification = if sm.wal_logical_bytes > 0 {
                sm.wal_physical_bytes as f64 / sm.wal_logical_bytes as f64
            } else {
                0.0
            };
            let _ = writeln!(out, "- **write_amplification:** {amplification:.2}x");
            let _ = writeln!(out, "- **wal_rmw_count:** {}", sm.wal_rmw_count);
        }
        let _ = writeln!(out);
        let _ = writeln!(out, "### System Resources");
        let _ = writeln!(out);
        let sys = &sm.system;
        let _ = writeln!(out, "- **cpu_user:** {} us", sys.cpu_user_us);
        let _ = writeln!(out, "- **cpu_sys:** {} us", sys.cpu_sys_us);
        let _ = writeln!(out, "- **peak_rss:** {} KB", sys.rss_kb);
        let _ = writeln!(out, "- **tcp_retransmits:** {}", sys.tcp_retransmits);
        let _ = writeln!(out, "- **tcp_lost:** {}", sys.tcp_lost);
        let _ = writeln!(out);

        // ── Anomalies ──
        let mut anomalies: Vec<String> = Vec::new();
        if self.error_rate > 0.0 {
            anomalies.push(format!("non-zero client error rate: {:.4}", self.error_rate));
        }
        if sys.tcp_retransmits > 0 {
            anomalies.push(format!("TCP retransmits: {}", sys.tcp_retransmits));
        }
        if sys.tcp_lost > 0 {
            anomalies.push(format!("TCP lost segments: {}", sys.tcp_lost));
        }
        if !cm.leader_changes.is_empty() {
            let total: u64 = cm.leader_changes.iter().map(|c| c.recovery_ms).sum();
            anomalies.push(format!(
                "leader changes: {} (total recovery {}ms)",
                cm.leader_changes.len(),
                total,
            ));
        }
        let _ = writeln!(out, "## Anomalies");
        let _ = writeln!(out);
        if anomalies.is_empty() {
            let _ = writeln!(out, "- none");
        } else {
            for a in &anomalies {
                let _ = writeln!(out, "- {a}");
            }
        }
        let _ = writeln!(out);
        let _ = writeln!(out, "---");
        out
    }

    /// Pretty multi-line summary suitable for `bench compare` output.
    #[must_use]
    pub(crate) fn human_summary(&self) -> String {
        use std::fmt::Write;
        let mut out = String::new();
        let _ = writeln!(
            out,
            "run_id          : {}\nmode            : {}\nworkload        : {:?}\nduration        : {} ms (measurement)\nwarmup          : {} ms (discarded)\nconnections     : {}\nthreads         : {}\nkey_space       : {}\nvalue_size      : {} B\ntarget          : {} (store={} group={})\ntotal_ops       : {}\ntotal_errors    : {}\nerror_rate      : {:.4}\ncorrectness_err : {}\npre_populate    : {} ms, {} errors",
            self.run_id,
            self.mode,
            self.workload,
            self.duration_ms,
            self.warmup_ms,
            self.connections,
            self.threads,
            self.key_space,
            self.value_size,
            self.target_endpoint,
            self.store_id,
            self.group_id,
            self.total_ops,
            self.total_errors,
            self.error_rate,
            self.correctness_errors,
            self.pre_pop_ms,
            self.pre_pop_errors,
        );
        for (kind, op) in &self.by_op {
            let p = &op.latency_us;
            let _ = writeln!(
                out,
                "{kind:>8}: ops={ops} err={err} nl={nl} nf={nf} ce={ce}  avg={avg}us p50={p50}us p99={p99}us p999={p999}us max={max}us",
                kind = kind,
                ops = op.ops,
                err = op.errors,
                nl = op.no_leader,
                nf = op.not_found,
                ce = op.correctness_errors,
                avg = p.avg_us,
                p50 = p.p50_us,
                p99 = p.p99_us,
                p999 = p.p999_us,
                max = p.max_us,
            );
        }
        let sm = &self.server_metrics;
        let cm = &self.client_metrics;
        let _ = writeln!(
            out,
            "server_metrics  : wal_append={wal_append} kv_put={kv_put} kv_get={kv_get}\nsystem          : cpu_user={cpu_user}us cpu_sys={cpu_sys}us rss={rss}KB tcp_retrans={tcp_retrans} tcp_lost={tcp_lost}\nclient_metrics  : nl_hint={nl_hint} leader_query={leader_query} xport_err={xport_err} retries_exhausted={retries_exhausted} no_leader={no_leader} topo_refresh={topo_refresh}\nblock_device    : logical_bytes={logical} physical_bytes={physical} rmw={rmw}",
            wal_append = sm.wal_append_count,
            kv_put = sm.kv_put_count,
            kv_get = sm.kv_get_count,
            cpu_user = sm.system.cpu_user_us,
            cpu_sys = sm.system.cpu_sys_us,
            rss = sm.system.rss_kb,
            tcp_retrans = sm.system.tcp_retransmits,
            tcp_lost = sm.system.tcp_lost,
            nl_hint = cm.not_leader_hint_followed,
            leader_query = cm.leader_query,
            xport_err = cm.transport_error_retry,
            retries_exhausted = cm.retries_exhausted,
            no_leader = cm.no_leader,
            topo_refresh = cm.topology_refresh,
            logical = sm.wal_logical_bytes,
            physical = sm.wal_physical_bytes,
            rmw = sm.wal_rmw_count,
        );
        let lc = &cm.leader_changes;
        if lc.is_empty() {
            let _ = writeln!(out, "leader_changes   : none");
        } else {
            let total: u64 = lc.iter().map(|c| c.recovery_ms).sum();
            let max: u64 = lc.iter().map(|c| c.recovery_ms).max().unwrap_or(0);
            let _ = writeln!(
                out,
                "leader_changes   : count={} total_recovery={}ms max_recovery={}ms",
                lc.len(),
                total,
                max,
            );
        }
        out
    }
}
