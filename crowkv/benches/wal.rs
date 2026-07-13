// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! WAL pipeline benchmark — multi-threaded load through a single WAL pipeline.
//!
//! Measures append throughput with N concurrent loader tasks, each writing KV
//! records sequentially (wait-for-ack before next append). Tests both real
//! file I/O and in-memory `BlockDevice` backends to isolate bottlenecks.
//!
//! Loaders run for a fixed duration (default 5 s) per case. TPS, batch count,
//! and average batch size are reported in a summary table.
//!
//! Run all cases:
//!   cargo bench --bench wal
//! Run a specific case (exact name match):
//!   cargo bench --bench wal -- `mem_1`
//!   cargo bench --bench wal -- `file_32`

#![allow(clippy::cast_precision_loss, clippy::map_unwrap_or)]

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;

use crowkv::paxos::roles::{PxBallot, PxLogEntry};
use crowkv::wal::record::WALRecord;
use crowkv::wal::wal_engine::WalEngine;
use crowkv::wal::{BlockDevice, IoBackend, WalConfig};

// ---------- helpers ----------

fn make_record(group: u64, slot: u64, payload_size: usize) -> WALRecord {
    let entry = PxLogEntry {
        slot,
        ballot: PxBallot::new(0, 1),
        term: 1,
        payload: Bytes::from(vec![0u8; payload_size]),
    };
    WALRecord::from_accepted(group, &entry)
}

/// Spawn `threads` loader tasks that keep appending until `stop` is set.
/// Returns the total number of records appended.
async fn run_loaders_timed(
    wal: Arc<WalEngine>,
    threads: usize,
    payload_size: usize,
    stop: Arc<AtomicBool>,
) -> u64 {
    let slot_counter = Arc::new(AtomicU64::new(1));
    let record_counts: Vec<Arc<AtomicU64>> = (0..threads).map(|_| Arc::new(AtomicU64::new(0))).collect();
    let mut handles = Vec::with_capacity(threads);

    for record_count in record_counts.iter().take(threads) {
        let wal = wal.clone();
        let slot_counter = slot_counter.clone();
        let stop = stop.clone();
        let count = record_count.clone();
        handles.push(tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                let slot = slot_counter.fetch_add(1, Ordering::Relaxed);
                let record = make_record(1, slot, payload_size);
                wal.append(&record).await.unwrap();
                count.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }

    record_counts.iter().map(|c| c.load(Ordering::Relaxed)).sum()
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
}

// ---------- test case definition ----------

#[derive(Clone, Copy)]
enum Backend {
    Mem,
    File,
}

struct Case {
    name: &'static str,
    backend: Backend,
    threads: usize,
    payload_size: usize,
}

const RUN_DURATION: Duration = Duration::from_secs(5);

const CASES: &[Case] = &[
    Case {
        name: "mem_1",
        backend: Backend::Mem,
        threads: 1,
        payload_size: 1024,
    },
    Case {
        name: "mem_32",
        backend: Backend::Mem,
        threads: 32,
        payload_size: 1024,
    },
    Case {
        name: "mem_128",
        backend: Backend::Mem,
        threads: 128,
        payload_size: 1024,
    },
    Case {
        name: "file_1",
        backend: Backend::File,
        threads: 1,
        payload_size: 1024,
    },
    Case {
        name: "file_32",
        backend: Backend::File,
        threads: 32,
        payload_size: 1024,
    },
    Case {
        name: "file_128",
        backend: Backend::File,
        threads: 128,
        payload_size: 1024,
    },
];

// ---------- runner ----------

struct CaseResult {
    name: String,
    records: u64,
    tps: f64,
    avg_latency_us: f64,
    avg_batch_latency_us: f64,
    avg_batch: f64,
}

fn run_case(rt: &tokio::runtime::Runtime, case: &Case) -> CaseResult {
    let (wal, _tmp) = match case.backend {
        Backend::Mem => {
            let backend = Arc::new(IoBackend::BlockDevice(BlockDevice::new()));
            let config = WalConfig {
                wal_disks: vec![PathBuf::from("/bench-wal")],
                wal_segment_size: 64 * 1024 * 1024,
                ..Default::default()
            };
            let wal = rt.block_on(async { WalEngine::create(backend, config, 1).await.unwrap() });
            (wal, None)
        }
        Backend::File => {
            let tmp = tempfile::tempdir().unwrap();
            let backend = Arc::new(IoBackend::File);
            let config = WalConfig {
                wal_disks: vec![tmp.path().to_path_buf()],
                wal_segment_size: 64 * 1024 * 1024,
                ..Default::default()
            };
            let wal = rt.block_on(async { WalEngine::create(backend, config, 1).await.unwrap() });
            (wal, Some(tmp))
        }
    };

    let stop = Arc::new(AtomicBool::new(false));
    let stop_signal = stop.clone();
    let wal_clone = wal.clone();
    let threads = case.threads;
    let payload_size = case.payload_size;
    let handle =
        rt.spawn(async move { run_loaders_timed(wal_clone, threads, payload_size, stop_signal).await });

    let start = Instant::now();
    std::thread::sleep(RUN_DURATION);
    stop.store(true, Ordering::Relaxed);

    let records = rt.block_on(handle).unwrap();
    let elapsed = start.elapsed();
    let tps = records as f64 / elapsed.as_secs_f64();
    let avg_latency_us = if records > 0 {
        elapsed.as_micros() as f64 / records as f64
    } else {
        0.0
    };

    let stats = wal.batch_stats();
    let avg_batch = stats.avg_batch_size();
    let avg_batch_latency_us = if stats.flush_count > 0 {
        elapsed.as_micros() as f64 / stats.flush_count as f64
    } else {
        0.0
    };

    eprintln!(
        "  [{}] records={}, tps={:.0}, latency={:.1}us, batch_latency={:.1}us, avg_batch={:.1}",
        case.name, records, tps, avg_latency_us, avg_batch_latency_us, avg_batch,
    );

    CaseResult {
        name: case.name.to_string(),
        records,
        tps,
        avg_latency_us,
        avg_batch_latency_us,
        avg_batch,
    }
}

fn main() {
    // When this bench is run via `cargo test --all-targets`, skip the heavy
    // timed workload. Criterion-based benches do this automatically, but this
    // custom bench needs an explicit guard to avoid blocking commits.
    let force_run = std::env::var("CROWKV_RUN_WAL_BENCH")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    if cfg!(test) && !force_run {
        eprintln!("Skipping wal bench in cargo test mode; set CROWKV_RUN_WAL_BENCH=1 to run real results.");
        return;
    }

    let args: Vec<String> = std::env::args().collect();
    let filter = args
        .iter()
        .skip(1)
        .find(|a| !a.starts_with('-'))
        .map_or("", std::string::String::as_str);

    let cases: Vec<&Case> = CASES
        .iter()
        .filter(|c| filter.is_empty() || c.name == filter)
        .collect();

    if cases.is_empty() {
        eprintln!(
            "No cases matching '{filter}'. Available: {:?}",
            CASES.iter().map(|c| c.name).collect::<Vec<_>>()
        );
        return;
    }

    let rt = runtime();
    eprintln!(
        "Running {} case(s), {} each...\n",
        cases.len(),
        RUN_DURATION.as_secs()
    );

    // Warm up the runtime with a short no-op run.
    rt.block_on(async {
        tokio::time::sleep(Duration::from_millis(100)).await;
    });

    let mut results = Vec::new();
    for case in &cases {
        eprintln!("--- {} ---", case.name);
        results.push(run_case(&rt, case));
    }

    // Summary table
    eprintln!(
        "\n{:<12} {:>10} {:>10} {:>10} {:>12} {:>10}",
        "case", "records", "tps", "lat_us", "batch_lat_us", "avg_batch"
    );
    eprintln!("{}", "-".repeat(66));
    for r in &results {
        eprintln!(
            "{:<12} {:>10} {:>10.0} {:>10.1} {:>12.1} {:>10.1}",
            r.name, r.records, r.tps, r.avg_latency_us, r.avg_batch_latency_us, r.avg_batch,
        );
    }
}
