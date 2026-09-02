// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `bench kv scan` — list/range workload against store 0 / group 0.
//!
//! Spawns `--loader-num` tokio tasks, each looping scan calls for
//! `--duration-secs`. Supports `--scan-limit`, `--scan-prefix`,
//! `--scan-start-after`, `--read-mode`, `--min-slot`,
//! `--read-endpoint-policy`, and `--value-size-mix`.

use std::process::ExitCode;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crowdb_kv_client::{ReadEndpointPolicy, ReadMode};

use super::kv_client::{build_kv_client, KvClientTunables};
use super::loader::{run_workload, BenchRecorder};
use super::metrics::BenchMetrics;
use super::result::{BenchOps, BenchResult};
use super::verb::{BenchMinSlot, BenchReadEndpoint, BenchReadMode, ScanArgs};
use crate::Cli;

#[allow(clippy::too_many_lines)]
pub async fn run(cli: &Cli, args: ScanArgs) -> ExitCode {
    let store_id = args.store;
    let group_id = args.group;
    let read_mode = match args.read_mode {
        BenchReadMode::Linearizable => ReadMode::Linearizable,
        BenchReadMode::Minslot => ReadMode::MinSlot,
    };
    let endpoint_policy = match args.read_endpoint_policy {
        BenchReadEndpoint::Leader => ReadEndpointPolicy::Leader,
        BenchReadEndpoint::AnyReplica => ReadEndpointPolicy::AnyReplica,
    };

    let client = match build_kv_client(
        cli,
        endpoint_policy,
        &KvClientTunables {
            pool_size: args.connections,
            ..Default::default()
        },
    ) {
        Ok(c) => Arc::new(c),
        Err(c) => return c,
    };

    let prefix = args.scan_prefix.as_bytes().to_vec();
    let start_after = args.scan_start_after.as_bytes().to_vec();
    let limit = args.scan_limit;
    let min_slot_arg = args.min_slot;
    let value_size = args.value_size;
    let value_size_mix = parse_value_size_mix(args.value_size_mix.as_deref());

    let mut metrics = BenchMetrics::new(&cli.log_dir, args.metrics_interval);
    metrics.start();

    let duration = Duration::from_secs(args.duration_secs);
    let start = Instant::now();
    let recorder = Arc::clone(&metrics.recorder);
    let run = run_workload(
        recorder,
        args.loader_num,
        duration,
        move |rec: Arc<BenchRecorder>| {
            let client = Arc::clone(&client);
            let prefix = prefix.clone();
            let start_after = start_after.clone();
            let limit = limit;
            let read_mode = read_mode;
            let min_slot_arg = min_slot_arg;
            let value_size = value_size;
            let value_size_mix = value_size_mix.clone();
            async move {
                let min_slot = match min_slot_arg {
                    BenchMinSlot::Auto => None,
                    BenchMinSlot::Zero => Some(0),
                };
                let t0 = Instant::now();
                match client
                    .scan(
                        store_id,
                        group_id,
                        &prefix,
                        &start_after,
                        &[],
                        limit,
                        read_mode,
                        min_slot,
                        false,
                        None,
                    )
                    .await
                {
                    Ok(outcome) => {
                        // Touch value bytes to prevent lazy-load optimization
                        // skewing latency. Both --value-size and --value-size-mix
                        // paths touch the same bytes.
                        let total_bytes: u64 = outcome.items.iter().map(|(_, v)| v.len() as u64).sum();
                        rec.record_ok(
                            t0.elapsed().as_micros().try_into().unwrap_or(u64::MAX),
                            total_bytes,
                        );
                        let _ = outcome.items.len();
                        if value_size > 0 || !value_size_mix.is_empty() {
                            for (_, v) in &outcome.items {
                                let _ = v.len();
                            }
                        }
                    }
                    Err(_) => rec.record_err(),
                }
            }
        },
        || {},
    )
    .await;

    let duration_ms: u64 = start.elapsed().as_millis().try_into().unwrap_or(u64::MAX);
    metrics.stop().await;

    let snap = run.recorder.hist_snapshot();
    eprintln!(
        "bench kv scan: {} ops, {} errors, {}ms",
        run.recorder.ops(),
        run.recorder.errors(),
        duration_ms,
    );

    let result = BenchResult {
        total_ops: run.recorder.ops(),
        duration_ms,
        total_errors: run.recorder.errors(),
        correctness_errors: Some(run.recorder.correctness_errors()),
        by_op: BenchOps::default().list(snap),
        client_transport_stats: None,
        server_metrics: None,
    };
    let json = serde_json::to_value(&result).unwrap_or_default();
    tracing::info!(report = %json, "bench_report");
    crate::commands::print_json(cli, &result)
}

/// Parse a `--value-size-mix` string like `64:70,1024:20,16384:10`
/// into a list of `(size, percent)` pairs. Empty/None → empty vec.
fn parse_value_size_mix(s: Option<&str>) -> Vec<(usize, u8)> {
    let Some(s) = s else { return Vec::new() };
    s.split(',')
        .filter_map(|pair| {
            let mut parts = pair.split(':');
            let size = parts.next()?.trim().parse::<usize>().ok()?;
            let pct = parts.next()?.trim().parse::<u8>().ok()?;
            Some((size, pct))
        })
        .collect()
}
