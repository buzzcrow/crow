// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Manual latency comparison for Phase 5: `AsyncCrowtree`'s
//! reactor-driven `get` (Phase 3) vs. the `spawn_blocking` bridge it
//! replaced, for both the fast (resident hit) and slow (demand-load miss)
//! paths.
//!
//! No `criterion`/nightly `#[bench]` dependency -- just wall-clock timing
//! per call, which is what actually matters here (the comparison is "does
//! this hop through the blocking thread pool", not sub-microsecond noise).
//!
//! Run: `cargo run --release --example async_get_bench -p crow-tree-ffi`

use std::sync::Arc;
use std::time::{Duration, Instant};

use crow_tree_ffi::{AsyncCrowtree, Crowtree, Options};

const N: usize = 2000;

fn key(i: usize) -> Vec<u8> {
    format!("key{i:08}").into_bytes()
}

fn report(label: &str, mut samples: Vec<Duration>) {
    samples.sort();
    let n = samples.len();
    let total: Duration = samples.iter().sum();
    let mean = total / n as u32;
    let p50 = samples[n / 2];
    let p99 = samples[(n * 99 / 100).min(n - 1)];
    let max = *samples.last().unwrap();
    println!(
        "{label:<32} n={n:<6} mean={mean:>10.2?} p50={p50:>10.2?} p99={p99:>10.2?} max={max:>10.2?} ops/sec={:>9.0}",
        n as f64 / total.as_secs_f64()
    );
}

/// The new path: drives the reactor directly.
async fn bench_new(tree: &AsyncCrowtree, keys: &[Vec<u8>], evict_each: bool) -> Vec<Duration> {
    let mut samples = Vec::with_capacity(keys.len());
    for k in keys {
        if evict_each {
            tree.handle().evict_clean_leaves(0);
        }
        let start = Instant::now();
        tree.get(k.clone()).await.unwrap();
        samples.push(start.elapsed());
    }
    samples
}

/// The old path this replaces: one `spawn_blocking` hop per call, exactly
/// what `AsyncCrowtree::get` used to do before Phase 3.
async fn bench_old(tree: &Arc<Crowtree>, keys: &[Vec<u8>], evict_each: bool) -> Vec<Duration> {
    let mut samples = Vec::with_capacity(keys.len());
    for k in keys {
        if evict_each {
            tree.evict_clean_leaves(0);
        }
        let t = tree.clone();
        let k = k.clone();
        let start = Instant::now();
        tokio::task::spawn_blocking(move || t.get(&k))
            .await
            .unwrap()
            .unwrap();
        samples.push(start.elapsed());
    }
    samples
}

fn main() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("build tokio runtime");
    rt.block_on(async_main());
}

/// `value_len` matters specifically for Phase 4 (zero-copy fast
/// path): a value small enough to Fit `buffer`'s SBO
/// (`kInlineCap` = 24 B) makes the copies Phase 4 removes cheap regardless
/// (a few-byte memcpy is noise either way), so the win only shows up
/// clearly for a value past that threshold -- run both sizes to see it.
async fn run_bench(value_len: usize, frame_bytes: u32) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("bench.ct");
    let opt = Options {
        path: Some(path.to_string_lossy().into_owned()),
        iu_size: 4096,
        frame_bytes,
        ..Default::default()
    };
    let tree = AsyncCrowtree::open(&opt).expect("open");
    let keys: Vec<Vec<u8>> = (0..N).map(key).collect();
    for (i, k) in keys.iter().enumerate() {
        let mut v = format!("v{i}-").into_bytes();
        v.resize(value_len, b'x');
        tree.apply_put((i + 1) as u64, k.clone(), v)
            .await
            .expect("apply_put");
    }
    tree.flush().await.expect("flush");
    tree.snapshot().await.expect("snapshot");

    println!("N={N} value_len={value_len}\n");
    println!("== fast path (resident hit, no reactor round trip needed) ==");
    let new_fast = bench_new(&tree, &keys, false).await;
    let old_fast = bench_old(&tree.handle(), &keys, false).await;
    report("new (reactor-driven)", new_fast);
    report("old (spawn_blocking)", old_fast);

    println!("\n== slow path (evict before every get -> genuine demand-load miss) ==");
    let new_slow = bench_new(&tree, &keys, true).await;
    let old_slow = bench_old(&tree.handle(), &keys, true).await;
    report("new (reactor-driven)", new_slow);
    report("old (spawn_blocking)", old_slow);
}

async fn async_main() {
    println!("### small value (fits buffer's inline SBO) ###\n");
    run_bench(16, 4096).await;
    println!("\n### larger value (heap-backed, past SBO) ###\n");
    run_bench(512, 4096).await;
    println!("\n### large value (near max_inline_value, still non-overflow) ###\n");
    run_bench(8192, 65536).await;
}
