// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `bench kv read` — point-get workload against store 0 / group 0.
//!
//! Spawns `--loader-num` tokio tasks, each looping random `get` calls
//! for `--duration-secs`. Supports `--read-mode` (linearizable|minslot),
//! `--min-slot` (auto|zero), `--read-endpoint-policy` (leader|any-replica),
//! and `--verify-bytes` correctness checking.

use std::process::ExitCode;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crowdb_kv_client::{GetOutcome, ReadEndpointPolicy, ReadMode};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};

use super::kv_client::{build_kv_client, KvClientTunables};
use super::loader::{run_workload, BenchRecorder};
use super::metrics::BenchMetrics;
use super::result::{BenchOps, BenchResult};
use super::verb::{BenchMinSlot, BenchReadEndpoint, BenchReadMode, ReadArgs};
use crate::Cli;

const STORE_ID: u64 = 0;
const GROUP_ID: u64 = 0;

pub async fn run(cli: &Cli, args: ReadArgs) -> ExitCode {
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

    let key_space = args.key_space.max(1);
    let verify_bytes = args.verify_bytes;
    let value_size = args.value_size;
    let min_slot_arg = args.min_slot;

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
            let key_space = key_space;
            let verify_bytes = verify_bytes;
            let value_size = value_size;
            let read_mode = read_mode;
            let min_slot_arg = min_slot_arg;
            async move {
                let mut rng = SmallRng::from_entropy();
                let id = rng.gen_range(0..key_space);
                let key = format!("k{id:020}");
                let min_slot = match min_slot_arg {
                    BenchMinSlot::Auto => None,
                    BenchMinSlot::Zero => Some(0),
                };
                let t0 = Instant::now();
                match client
                    .get(STORE_ID, GROUP_ID, key.as_bytes(), read_mode, min_slot)
                    .await
                {
                    Ok(GetOutcome::Found { value, .. }) => {
                        rec.record_ok(
                            t0.elapsed().as_micros().try_into().unwrap_or(u64::MAX),
                            value.len() as u64,
                        );
                        if verify_bytes > 0 {
                            let expected = expected_value(id, value_size, verify_bytes);
                            if value.len() < verify_bytes || value[..verify_bytes] != expected[..] {
                                rec.record_correctness_err();
                            }
                        }
                    }
                    Ok(GetOutcome::NotFound) => {
                        rec.record_err();
                        if verify_bytes > 0 {
                            rec.record_correctness_err();
                        }
                    }
                    Err(_) => rec.record_err(),
                }
            }
        },
    )
    .await;

    let duration_ms: u64 = start.elapsed().as_millis().try_into().unwrap_or(u64::MAX);
    metrics.stop().await;

    let snap = run.recorder.hist_snapshot();
    eprintln!(
        "bench kv read: {} ops, {} errors, {} corr errors, {}ms",
        run.recorder.ops(),
        run.recorder.errors(),
        run.recorder.correctness_errors(),
        duration_ms,
    );

    let result = BenchResult {
        total_ops: run.recorder.ops(),
        duration_ms,
        total_errors: run.recorder.errors(),
        correctness_errors: Some(run.recorder.correctness_errors()),
        by_op: BenchOps::default().read(snap),
        client_transport_stats: None,
        server_metrics: None,
    };
    let json = serde_json::to_value(&result).unwrap_or_default();
    tracing::info!(report = %json, "bench_report");
    crate::commands::print_json(cli, &result)
}

/// Build the expected first `verify` bytes for key `id`: byte `i` =
/// `(id + i) % 256` (same pattern as `kv_prepare::build_value`).
fn expected_value(id: u64, size: usize, verify: usize) -> Vec<u8> {
    let len = verify.min(size);
    (0..len)
        .map(|i| u8::try_from((id + i as u64) % 256).unwrap_or(0))
        .collect()
}
