// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `bench kv write` — put workload against store 0 / group 0.
//!
//! Spawns `--loader-num` tokio tasks, each looping random `put` calls
//! for `--duration-secs`. After the workload, fetches server-side
//! metrics from every node's `/metrics` endpoint and aggregates them
//! into the `server_metrics` JSON section.

use std::process::ExitCode;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crowdb_console_shared::clients::http::ServerClient;
use crowdb_console_shared::snapshot::MetricFieldView;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};

use super::kv_client::{build_kv_client, KvClientTunables};
use super::loader::{run_workload, BenchRecorder};
use super::result::{BenchOps, BenchResult, ReplicaStats, ServerMetrics, TransportStats};
use super::verb::WriteArgs;
use crate::commands::load_config;
use crate::Cli;

const STORE_ID: u64 = 0;
const GROUP_ID: u64 = 0;

pub async fn run(cli: &Cli, args: WriteArgs) -> ExitCode {
    let client = match build_kv_client(
        cli,
        crowdb_kv_client::ReadEndpointPolicy::Leader,
        &KvClientTunables {
            event_write: args.event_write,
            rpc_workers: args.rpc_workers,
            send_queue_capacity: if args.send_queue_capacity > 0 {
                args.send_queue_capacity
            } else {
                4096
            },
            pool_size: args.connections,
            ..Default::default()
        },
    ) {
        Ok(c) => Arc::new(c),
        Err(c) => return c,
    };

    let key_space = args.key_space.max(1);
    let value_size = args.value_size;

    let duration = Duration::from_secs(args.duration_secs);
    let start = Instant::now();
    let run = run_workload(args.loader_num, duration, move |rec: Arc<BenchRecorder>| {
        let client = Arc::clone(&client);
        let key_space = key_space;
        let value_size = value_size;
        async move {
            let mut rng = SmallRng::from_entropy();
            let id = rng.gen_range(0..key_space);
            let key = format!("k{id:020}");
            let value = build_value(id, value_size);
            let t0 = Instant::now();
            match client.put(STORE_ID, GROUP_ID, key.as_bytes(), &value, None).await {
                Ok(_) => rec.record_ok(t0.elapsed().as_micros().try_into().unwrap_or(u64::MAX)),
                Err(_) => rec.record_err(),
            }
        }
    })
    .await;

    let duration_ms: u64 = start.elapsed().as_millis().try_into().unwrap_or(u64::MAX);

    // Fetch server-side metrics from every node.
    let server_metrics = fetch_server_metrics(cli, STORE_ID, GROUP_ID).await;

    let snap = run.recorder.hist_snapshot();
    eprintln!(
        "bench kv write: {} ops, {} errors, {}ms",
        run.recorder.ops(),
        run.recorder.errors(),
        duration_ms,
    );

    // Client transport stats — not yet wired (requires FFI transport
    // stats export from crowdb-rpc). Reported as zeros.
    let client_transport_stats = Some(TransportStats::default());

    let result = BenchResult {
        total_ops: run.recorder.ops(),
        duration_ms,
        total_errors: run.recorder.errors(),
        correctness_errors: Some(run.recorder.correctness_errors()),
        by_op: BenchOps::default().write(snap),
        client_transport_stats,
        server_metrics,
    };
    crate::commands::print_json(cli, &result)
}

/// Build a deterministic value for key `id`: byte `i` = `(id + i) % 256`.
fn build_value(id: u64, size: usize) -> Vec<u8> {
    (0..size)
        .map(|i| u8::try_from((id + i as u64) % 256).unwrap_or(0))
        .collect()
}

/// Fetch `/metrics` from every server in the config and aggregate into
/// `ServerMetrics`. Metrics are summed across nodes except for averages
/// (which are averaged across nodes that report them).
async fn fetch_server_metrics(cli: &Cli, store_id: u64, group_id: u64) -> Option<ServerMetrics> {
    let config = load_config(cli).ok()?;
    let group_prefix = format!("s.{store_id}.g.{group_id}.");
    let store_prefix = format!("s.{store_id}.rpc.");

    let mut mgmt_urls: Vec<String> = config.servers.iter().map(|s| s.url.clone()).collect();
    mgmt_urls.sort();
    mgmt_urls.dedup();
    if mgmt_urls.is_empty() {
        return None;
    }

    let mut wal_append_count = 0u64;
    let mut inflight_enqueued = 0u64;
    let mut inflight_wait_sum = 0u64;
    let mut inflight_wait_count = 0u64;
    let mut rpc_s2w_sum = 0u64;
    let mut rpc_s2w_count = 0u64;
    // Replica RPC: collect all `rpc.l@<peer>` summaries across nodes.
    // The leader sends accept rounds to peers; followers only send
    // fetchgap/chosen_notice. We aggregate by peer id and report the
    // two peers with the highest `total` (round-trips) as r2/r3.
    let mut peer_totals: Vec<(u64, u64, u64)> = Vec::new(); // (peer_id, avg_us, total)

    for url in &mgmt_urls {
        let Ok(sc) = ServerClient::new(url.clone()) else {
            continue;
        };
        // Group-level metrics: WAL, inflight, replica RPC.
        if let Ok(resp) = sc.metrics(&group_prefix).await {
            for point in &resp.metrics {
                let fields = field_map(&point.fields);
                match point.name.as_str() {
                    // WAL append summary: `total` = cumulative count.
                    n if n.ends_with(".wal.mem.append.l") || n.ends_with(".wal.file.append.l") => {
                        wal_append_count += get_u64(&fields, "total");
                    }
                    n if n.ends_with(".write.inflight_enqueued.c") => {
                        inflight_enqueued += get_u64(&fields, "total");
                    }
                    n if n.ends_with(".write.inflight_wait.l") => {
                        if get_u64(&fields, "count") > 0 {
                            inflight_wait_sum += get_u64(&fields, "avg_ns") / 1000;
                            inflight_wait_count += 1;
                        }
                    }
                    // Replica RPC latency: s.0.g.0.rpc.l@<peer_id>
                    n if n.contains(".rpc.l@") => {
                        let peer_id = n
                            .rsplit('@')
                            .next()
                            .and_then(|s| s.parse::<u64>().ok())
                            .unwrap_or(0);
                        let avg_us = get_u64(&fields, "avg_ns") / 1000;
                        let total = get_u64(&fields, "total");
                        peer_totals.push((peer_id, avg_us, total));
                    }
                    _ => {}
                }
            }
        }
        // Store-level RPC transport stats.
        if let Ok(resp) = sc.metrics(&store_prefix).await {
            for point in &resp.metrics {
                let fields = field_map(&point.fields);
                if point.name.ends_with(".rpc.submit_to_writev.avg_us.g") {
                    rpc_s2w_sum += get_u64(&fields, "value");
                    rpc_s2w_count += 1;
                }
            }
        }
    }

    // Aggregate peer totals by peer_id (sum across nodes), then pick
    // the top 2 by total round-trips as r2/r3.
    peer_totals.sort_by_key(|&(_, _, total)| std::cmp::Reverse(total));
    let (r2_avg, r2_tps) = peer_totals
        .first()
        .map_or((0, 0), |(_, avg, total)| (*avg, *total));
    let (r3_avg, r3_tps) = peer_totals
        .get(1)
        .map_or((0, 0), |(_, avg, total)| (*avg, *total));

    let inflight_wait_avg_us = inflight_wait_sum.checked_div(inflight_wait_count).unwrap_or(0);
    let submit_to_writev_avg_us = rpc_s2w_sum.checked_div(rpc_s2w_count).unwrap_or(0);

    // RPC transport stats (writev_calls, frames_sent, etc.) are not
    // exposed via the metrics registry — they live in the crowdb-rpc
    // FFI transport and are not yet surfaced to the management API.
    // Reported as 0; the regression script tolerates this via jq `// 0`.
    Some(ServerMetrics {
        wal_append_count,
        rpc: TransportStats {
            writev_calls: 0,
            frames_sent: 0,
            read_calls: 0,
            frames_parsed: 0,
            submit_to_writev_avg_us,
        },
        replica: ReplicaStats {
            r2: r2_avg,
            r2_tps,
            r3: r3_avg,
            r3_tps,
        },
        inflight_enqueued,
        inflight_wait_avg_us,
    })
}

fn field_map(fields: &[MetricFieldView]) -> std::collections::HashMap<&str, f64> {
    fields.iter().map(|f| (f.key.as_str(), f.value)).collect()
}

fn get_u64(map: &std::collections::HashMap<&str, f64>, key: &str) -> u64 {
    // Metric values are non-negative counts/latencies; the filter
    // guards against NaN/negative before the truncating cast.
    map.get(key)
        .filter(|v| v.is_finite() && **v >= 0.0)
        .map_or(0, |v| {
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            {
                *v as u64
            }
        })
}
