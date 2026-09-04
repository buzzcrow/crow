// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Diskdb allocate and mixed allocate/free benchmarks.

use std::collections::HashSet;
use std::process::ExitCode;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crowdb_diskdb_client::{DiskdbClient, DiskdbClientError, DiskdbRpcTransport};
use crowdb_kv_client::{ReadEndpointPolicy, ServiceRegistryClient};
use crowdb_protocol::common::ChunkId;
use crowdb_protocol::diskdb::rpc::{AllocateBlocksRequest, CompactZoneRequest, FreeBlocksRequest, Segment};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};

use super::kv_client::{build_kv_client, KvClientTunables};
use super::loader::BenchRecorder;
use super::metrics::BenchMetrics;
use super::verb::{DiskdbArgs, DiskdbBenchVerb};
use crate::Cli;

#[derive(Default)]
struct TaskResult {
    allocated: u64,
    freed: u64,
    errors: u64,
    exhausted: bool,
    latency_ns: Vec<u64>,
}

struct TaskContext {
    mixed: bool,
    client: Arc<DiskdbClient>,
    groups: Arc<Vec<u64>>,
    args: DiskdbArgs,
    sender: crossbeam_channel::Sender<Segment>,
    receiver: crossbeam_channel::Receiver<Segment>,
    stop: Arc<AtomicBool>,
    compacting: Arc<AtomicBool>,
    in_flight: Arc<AtomicU64>,
    sequence: Arc<AtomicU64>,
    recorder: Arc<BenchRecorder>,
    deadline: Instant,
}

struct ReportInput<'a> {
    args: &'a DiskdbArgs,
    mixed: bool,
    elapsed: Duration,
    baseline: u64,
    final_busy: u64,
    live: &'a [Segment],
}

pub async fn run(cli: &Cli, verb: DiskdbBenchVerb) -> ExitCode {
    let args = match &verb {
        DiskdbBenchVerb::Allocate(args) | DiskdbBenchVerb::Mix(args) => args.clone(),
    };
    if !valid_args(&args) {
        return ExitCode::from(2);
    }
    let kv = match build_kv_client(cli, ReadEndpointPolicy::Leader, &KvClientTunables::default()) {
        Ok(kv) => Arc::new(kv),
        Err(code) => return code,
    };
    let client = Arc::new(DiskdbClient::new(
        ServiceRegistryClient::from_shared(Arc::clone(&kv)),
        Arc::new(DiskdbRpcTransport::new()),
    ));
    if let Err(error) = client.refresh_endpoints().await {
        eprintln!("diskdb discovery failed: {error}");
        return ExitCode::FAILURE;
    }
    let groups = Arc::new(client.disk_group_ids());
    if groups.is_empty() {
        eprintln!("no owned disk-groups discovered");
        return ExitCode::FAILURE;
    }
    if let Err(error) = validate_topology(&client, &groups).await {
        eprintln!("invalid diskdb benchmark topology: {error}");
        return ExitCode::FAILURE;
    }
    let baseline = match busy_bytes(&client, &groups).await {
        Ok(value) => value,
        Err(error) => {
            eprintln!("baseline capacity query failed: {error}");
            return ExitCode::FAILURE;
        }
    };
    let mixed = matches!(verb, DiskdbBenchVerb::Mix(_));
    let mut metrics = BenchMetrics::new(&cli.log_dir, args.metrics_interval);
    metrics.start();
    let (sender, receiver) = crossbeam_channel::unbounded::<Segment>();
    let stop = Arc::new(AtomicBool::new(false));
    let compacting = Arc::new(AtomicBool::new(false));
    let in_flight = Arc::new(AtomicU64::new(0));
    let sequence = Arc::new(AtomicU64::new(1));
    let deadline = Instant::now() + Duration::from_secs(args.duration_secs);
    let started = Instant::now();
    let mut handles = Vec::with_capacity(args.concurrency);
    for task_id in 0..args.concurrency {
        let client = Arc::clone(&client);
        let groups = Arc::clone(&groups);
        let args = args.clone();
        let sender = sender.clone();
        let receiver = receiver.clone();
        let stop = Arc::clone(&stop);
        let sequence = Arc::clone(&sequence);
        let compacting = Arc::clone(&compacting);
        let in_flight = Arc::clone(&in_flight);
        let recorder = Arc::clone(&metrics.recorder);
        handles.push(tokio::spawn(run_task(
            task_id,
            TaskContext {
                mixed,
                client,
                groups,
                args,
                sender,
                receiver,
                stop,
                compacting,
                in_flight,
                sequence,
                recorder,
                deadline,
            },
        )));
    }
    drop(sender);
    let mut total = collect_results(handles).await;
    let elapsed = started.elapsed();
    metrics.stop().await;
    let live: Vec<_> = receiver.try_iter().collect();
    if mixed && !compact_for_verification(&client, &groups).await {
        total.errors += 1;
    }
    let final_busy = busy_bytes(&client, &groups).await.unwrap_or(u64::MAX);
    report(
        &mut total,
        &ReportInput {
            args: &args,
            mixed,
            elapsed,
            baseline,
            final_busy,
            live: &live,
        },
    )
}

async fn collect_results(handles: Vec<tokio::task::JoinHandle<TaskResult>>) -> TaskResult {
    let mut total = TaskResult::default();
    for handle in handles {
        match handle.await {
            Ok(result) => merge_result(&mut total, result),
            Err(error) => {
                eprintln!("benchmark task failed: {error}");
                total.errors += 1;
            }
        }
    }
    total
}

fn valid_args(args: &DiskdbArgs) -> bool {
    let valid = args.concurrency > 0 && args.unit_count > 0 && args.blocks_per_request > 0;
    if !valid {
        eprintln!("concurrency, unit-count, and blocks-per-request must be non-zero");
    }
    valid
}

async fn compact_for_verification(client: &DiskdbClient, groups: &[u64]) -> bool {
    for _ in 0..3 {
        if let Err(error) = compact_all(client, groups).await {
            eprintln!("diskdb compaction failed: {error}");
            return false;
        }
    }
    true
}

fn report(total: &mut TaskResult, input: &ReportInput<'_>) -> ExitCode {
    let expected_delta = input
        .live
        .iter()
        .map(|segment| u64::from(segment.unit_count) * input.args.unit_size_bytes)
        .sum::<u64>();
    let actual_delta = input.final_busy.saturating_sub(input.baseline);
    let unique = input
        .live
        .iter()
        .filter_map(|segment| {
            segment
                .disk_id
                .map(|disk| (disk, segment.zone_index, segment.unit_offset))
        })
        .collect::<HashSet<_>>()
        .len();
    total.latency_ns.sort_unstable();
    let elapsed = input.elapsed;
    let operations = total.allocated + total.freed;
    let throughput = u128::from(operations)
        .saturating_mul(1_000_000_000)
        .checked_div(elapsed.as_nanos().max(1))
        .unwrap_or(0);
    let stop_reason = if total.exhausted { "exhausted" } else { "deadline" };
    println!(
        "diskdb bench mode={:?} workload={} stop={} elapsed={:.3}s ops={} ops_per_sec={} allocated={} freed={} live={} errors={} p50_us={} p99_us={} busy_delta={} expected_delta={}",
        input.args.mode,
        if input.mixed { "mix-70-30" } else { "allocate" },
        stop_reason,
        elapsed.as_secs_f64(),
        operations,
        throughput,
        total.allocated,
        total.freed,
        input.live.len(),
        total.errors,
        percentile(&total.latency_ns, 50) / 1_000,
        percentile(&total.latency_ns, 99) / 1_000,
        actual_delta,
        expected_delta,
    );
    if total.errors > 0 || unique != input.live.len() || actual_delta != expected_delta {
        eprintln!("diskdb benchmark correctness verification failed");
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}

async fn run_task(task_id: usize, context: TaskContext) -> TaskResult {
    let mut result = TaskResult::default();
    let mut rng = SmallRng::seed_from_u64(context.args.seed ^ u64::try_from(task_id).unwrap_or(u64::MAX));
    while Instant::now() < context.deadline && !context.stop.load(Ordering::Acquire) {
        if context.compacting.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
            continue;
        }
        context.in_flight.fetch_add(1, Ordering::AcqRel);
        if context.compacting.load(Ordering::Acquire) {
            context.in_flight.fetch_sub(1, Ordering::AcqRel);
            tokio::task::yield_now().await;
            continue;
        }
        let mut mutation_in_flight = true;
        let do_free = context.mixed && rng.gen_range(0..100) >= 70;
        let started = Instant::now();
        if do_free {
            if let Ok(segment) = context.receiver.try_recv() {
                match context
                    .client
                    .free_blocks(FreeBlocksRequest {
                        segments: vec![segment],
                    })
                    .await
                {
                    Ok(response) if response.freed_count == 1 => {
                        result.freed += 1;
                        context
                            .recorder
                            .record_ok(started.elapsed().as_micros().try_into().unwrap_or(u64::MAX), 0);
                    }
                    _ => {
                        let _ = context.sender.send(segment);
                        result.errors += 1;
                        context.recorder.record_err();
                    }
                }
            }
        } else {
            let seq = context.sequence.fetch_add(1, Ordering::Relaxed);
            match allocate_any_group(&context, task_id, seq).await {
                Ok(Some(response)) => {
                    result.allocated += response.segments.len() as u64;
                    for _ in &response.segments {
                        context
                            .recorder
                            .record_ok(started.elapsed().as_micros().try_into().unwrap_or(u64::MAX), 0);
                    }
                    for segment in response.segments {
                        let _ = context.sender.send(segment);
                    }
                }
                Ok(None) if !context.mixed => {
                    result.exhausted = true;
                    context.stop.store(true, Ordering::Release);
                }
                Ok(None) => {
                    if context
                        .compacting
                        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                        .is_ok()
                    {
                        context.in_flight.fetch_sub(1, Ordering::AcqRel);
                        mutation_in_flight = false;
                        while context.in_flight.load(Ordering::Acquire) != 0 {
                            tokio::task::yield_now().await;
                        }
                        if compact_all(&context.client, &context.groups).await.is_err() {
                            result.errors += 1;
                            context.recorder.record_err();
                        }
                        context.compacting.store(false, Ordering::Release);
                    } else {
                        tokio::task::yield_now().await;
                    }
                }
                Err(_) => {
                    result.errors += 1;
                    context.recorder.record_err();
                }
            }
        }
        result
            .latency_ns
            .push(started.elapsed().as_nanos().try_into().unwrap_or(u64::MAX));
        if mutation_in_flight {
            context.in_flight.fetch_sub(1, Ordering::AcqRel);
        }
    }
    result
}

async fn allocate_any_group(
    context: &TaskContext,
    task_id: usize,
    sequence: u64,
) -> Result<Option<crowdb_protocol::diskdb::rpc::AllocateResponse>, DiskdbClientError> {
    let group_count = u64::try_from(context.groups.len()).unwrap_or(u64::MAX);
    let start = usize::try_from(sequence % group_count).unwrap_or(0);
    for offset in 0..context.groups.len() {
        let request = AllocateBlocksRequest {
            disk_group_id: context.groups[(start + offset) % context.groups.len()],
            unit_count: context.args.unit_count,
            count: context.args.blocks_per_request,
            exclude_disk_ids: Vec::new(),
            owner_chunk: Some(ChunkId {
                high: u64::try_from(task_id).unwrap_or(u64::MAX),
                low: sequence,
            }),
        };
        match context.client.allocate_blocks(request).await {
            Ok(response) => return Ok(Some(response)),
            Err(DiskdbClientError::NoSpace(_)) => {}
            Err(error) => return Err(error),
        }
    }
    Ok(None)
}

fn merge_result(total: &mut TaskResult, result: TaskResult) {
    total.allocated += result.allocated;
    total.freed += result.freed;
    total.errors += result.errors;
    total.exhausted |= result.exhausted;
    total.latency_ns.extend(result.latency_ns);
}

async fn busy_bytes(client: &DiskdbClient, groups: &[u64]) -> Result<u64, DiskdbClientError> {
    let mut total = 0u64;
    for group in groups {
        let response = client.query_disk_group(*group).await?;
        total += response
            .disk_groups
            .iter()
            .map(|entry| entry.busy_bytes)
            .sum::<u64>();
    }
    Ok(total)
}

async fn validate_topology(client: &DiskdbClient, groups: &[u64]) -> Result<(), DiskdbClientError> {
    if groups.len() != 3 {
        return Err(DiskdbClientError::Rpc(format!(
            "expected 3 disk-groups, found {}",
            groups.len()
        )));
    }
    for group in groups {
        let response = client.query_disk_group(*group).await?;
        let disks = response
            .disk_groups
            .iter()
            .map(|entry| entry.disks.len())
            .sum::<usize>();
        if disks != 4 {
            return Err(DiskdbClientError::Rpc(format!(
                "disk-group {group} must contain 4 disks, found {disks}"
            )));
        }
    }
    Ok(())
}

async fn compact_all(client: &DiskdbClient, groups: &[u64]) -> Result<(), DiskdbClientError> {
    for group in groups {
        let response = client.query_disk_group(*group).await?;
        for disk in response.disk_groups.iter().flat_map(|entry| &entry.disks) {
            if let Some(disk_id) = disk.disk_id {
                client
                    .compact_zone(CompactZoneRequest {
                        disk_id: Some(disk_id),
                        zone_indices: Vec::new(),
                    })
                    .await?;
            }
        }
    }
    Ok(())
}

fn percentile(sorted: &[u64], percentile: usize) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    sorted[(sorted.len() - 1) * percentile / 100]
}
