// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#![allow(unsafe_code)]

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use crossbeam_skiplist::SkipMap;
use crowkv::paxos::roles::{PxBallot, PxLogEntry};
use crowkv::paxos::slot_list::PxSlotList;
use crowkv::paxos::slot_node::PxSlotNode;
use dashmap::DashMap;
use std::collections::BTreeMap;
use std::ptr::null_mut;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
// ---------- sequential tail insert ----------

// ---------- helpers ----------

/// Simple LCG for deterministic "randomness" without adding a dependency.
fn lcg(seed: &mut u64) -> u64 {
    // constants from Numerical Recipes
    *seed = seed.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
    *seed
}

macro_rules! define_slot_node_churn_bench {
    ($fn_name:ident, $group_name:literal) => {
        fn $fn_name(c: &mut Criterion) {
            let mut group = c.benchmark_group($group_name);
            for replacements in [1_000u64, 10_000u64].iter() {
                group.throughput(Throughput::Elements(*replacements * 2));
                group.bench_with_input(
                    BenchmarkId::from_parameter(replacements),
                    replacements,
                    |b, &replacements| {
                        b.iter(|| {
                            let list = PxSlotList::<PxSlotNode>::new();
                            let guard = list.insert_if_empty(7, PxSlotNode::default());
                            let node: &PxSlotNode = &guard;

                            let mut promised_ptr = null_mut();
                            let mut accepted_ptr = null_mut();
                            for i in 0..replacements {
                                let ballot = PxBallot::new(i + 1, 1);
                                promised_ptr = node.cas_promised(promised_ptr, ballot).unwrap();

                                let entry = PxLogEntry {
                                    slot: 7,
                                    ballot,
                                    term: ballot.round,
                                    payload: bytes::Bytes::from_static(&[1, 2, 3]),
                                };
                                accepted_ptr = node.cas_accepted(accepted_ptr, entry).unwrap();
                            }

                            std::hint::black_box(promised_ptr);
                            std::hint::black_box(accepted_ptr);
                        });
                    },
                );
            }
            group.finish();
        }
    };
}

macro_rules! define_tail_insert_bench {
    ($fn_name:ident, $group_name:literal, $setup:expr, $state:ident, $slot:ident, $body:block) => {
        fn $fn_name(c: &mut Criterion) {
            let mut group = c.benchmark_group($group_name);
            group.bench_function("sequential", |b| {
                #[allow(unused_mut)]
                let mut $state = $setup;
                let mut $slot = 0u64;
                b.iter(|| {
                    $body
                    $slot += 1;
                });
            });
            group.finish();
        }
    };
}

macro_rules! define_concurrent_insert_bench {
    (
        $fn_name:ident,
        $group_name:literal,
        $setup:expr,
        $state:ident,
        $slot:ident,
        $insert_body:block
    ) => {
        fn $fn_name(c: &mut Criterion) {
            let mut group = c.benchmark_group($group_name);
            for threads in [1, 8, 32].iter() {
                group.throughput(Throughput::Elements(*threads as u64 * 1000));
                group.bench_with_input(
                    BenchmarkId::from_parameter(threads),
                    threads,
                    |b, &threads| {
                        b.iter(|| {
                            let shared_state = Arc::new($setup);
                            let tail = Arc::new(AtomicU64::new(0));
                            let mut handles = Vec::with_capacity(threads);
                            for t in 0..threads {
                                let $state = Arc::clone(&shared_state);
                                let tail = Arc::clone(&tail);
                                handles.push(thread::spawn(move || {
                                    let mut rng = (t as u64 + 1).wrapping_mul(0x9e3779b97f4a7c15);
                                    for op in 0..1000 {
                                        let current = tail.load(Ordering::Relaxed);
                                        let variance = (current / 20).max(1);
                                        let offset = lcg(&mut rng) % variance;
                                        let $slot = current + offset;
                                        $insert_body

                                        if op % 200 == 0 {
                                            tail.fetch_add(100, Ordering::Relaxed);
                                        }
                                    }
                                }));
                            }
                            for h in handles {
                                h.join().unwrap();
                            }
                        });
                    },
                );
            }
            group.finish();
        }
    };
}

macro_rules! define_concurrent_get_bench {
    (
        $fn_name:ident,
        $group_name:literal,
        $setup:expr,
        $prefill_state:ident,
        $prefill_slot:ident,
        $prefill_body:block,
        $get_state:ident,
        $get_slot:ident,
        $get_body:block,
        $advance_state:ident,
        $window_start:ident,
        $advance_body:block
    ) => {
        fn $fn_name(c: &mut Criterion) {
            let mut group = c.benchmark_group($group_name);
            let prefill_count = 200_000u64;
            let window = 1_000u64;
            let shared_state = Arc::new($setup);
            for i in 0..prefill_count {
                let $prefill_state = Arc::as_ref(&shared_state);
                let $prefill_slot = i;
                $prefill_body
            }
            let window_start = Arc::new(AtomicU64::new(0));

            for threads in [1, 8, 32].iter() {
                group.throughput(Throughput::Elements(*threads as u64 * 1000));
                group.bench_with_input(
                    BenchmarkId::from_parameter(threads),
                    threads,
                    |b, &threads| {
                        b.iter(|| {
                            let mut handles = Vec::with_capacity(threads);
                            for t in 0..threads {
                                let thread_state = Arc::clone(&shared_state);
                                let thread_window_start = Arc::clone(&window_start);
                                handles.push(thread::spawn(move || {
                                    let mut rng = (t as u64 + 1).wrapping_mul(0x9e3779b97f4a7c15);
                                    for op in 0..1000 {
                                        let start = thread_window_start.load(Ordering::Relaxed);
                                        let slot = start + (lcg(&mut rng) % window);

                                        let $get_state = Arc::as_ref(&thread_state);
                                        let $get_slot = slot;
                                        $get_body

                                        if op % 200 == 0 {
                                            let $advance_state = Arc::as_ref(&thread_state);
                                            let $window_start = Arc::as_ref(&thread_window_start);
                                            $advance_body
                                        }
                                    }
                                }));
                            }
                            for h in handles {
                                h.join().unwrap();
                            }
                        });
                    },
                );
            }
            group.finish();
        }
    };
}

define_tail_insert_bench!(
    bench_insert_u64_tail,
    "slot_list_insert_u64_tail",
    PxSlotList::<u64>::new(),
    list,
    slot,
    {
        std::hint::black_box(list.insert_if_empty(slot, slot));
    }
);

define_tail_insert_bench!(
    bench_btreemap_insert_u64_tail,
    "btreemap_insert_u64_tail",
    BTreeMap::<u64, u64>::new(),
    map,
    slot,
    {
        std::hint::black_box(map.insert(slot, slot));
    }
);

define_concurrent_insert_bench!(
    bench_insert_u64_concurrent,
    "slot_list_insert_concurrent",
    PxSlotList::<u64>::new(),
    list,
    slot,
    {
        list.insert_if_empty(slot, slot);
    }
);

define_concurrent_insert_bench!(
    bench_dashmap_insert_concurrent,
    "dashmap_insert_concurrent",
    DashMap::<u64, u64>::new(),
    map,
    slot,
    {
        map.insert(slot, slot);
    }
);

define_concurrent_insert_bench!(
    bench_skipmap_insert_concurrent,
    "skipmap_insert_concurrent",
    SkipMap::<u64, u64>::new(),
    map,
    slot,
    {
        map.insert(slot, slot);
    }
);

define_concurrent_insert_bench!(
    bench_btreemap_insert_concurrent,
    "btreemap_insert_concurrent",
    Mutex::new(BTreeMap::<u64, u64>::new()),
    map,
    slot,
    {
        map.lock().unwrap().insert(slot, slot);
    }
);

define_concurrent_get_bench!(
    bench_get_u64_concurrent,
    "slot_list_get_concurrent",
    PxSlotList::<u64>::new(),
    list,
    slot,
    {
        list.insert_if_empty(slot, slot);
    },
    list,
    slot,
    {
        std::hint::black_box(list.get_tail(slot));
    },
    _list,
    window_start,
    {
        window_start.fetch_add(100, Ordering::Relaxed);
    }
);

define_concurrent_get_bench!(
    bench_dashmap_get_concurrent,
    "dashmap_get_concurrent",
    DashMap::<u64, u64>::new(),
    map,
    slot,
    {
        map.insert(slot, slot);
    },
    map,
    slot,
    {
        std::hint::black_box(map.get(&slot));
    },
    _map,
    window_start,
    {
        window_start.fetch_add(100, Ordering::Relaxed);
    }
);

define_concurrent_get_bench!(
    bench_skipmap_get_concurrent,
    "skipmap_get_concurrent",
    SkipMap::<u64, u64>::new(),
    map,
    slot,
    {
        map.insert(slot, slot);
    },
    map,
    slot,
    {
        std::hint::black_box(map.get(&slot));
    },
    _map,
    window_start,
    {
        window_start.fetch_add(100, Ordering::Relaxed);
    }
);

define_concurrent_get_bench!(
    bench_btreemap_get_concurrent,
    "btreemap_get_concurrent",
    RwLock::new(BTreeMap::<u64, u64>::new()),
    map,
    slot,
    {
        map.write().unwrap().insert(slot, slot);
    },
    map,
    slot,
    {
        let guard = map.read().unwrap();
        let r = guard.get(&slot);
        std::hint::black_box(r);
        drop(guard);
    },
    _map,
    window_start,
    {
        window_start.fetch_add(100, Ordering::Relaxed);
    }
);

define_slot_node_churn_bench!(bench_slot_node_reclaim_churn, "slot_node_reclaim_churn");

criterion_group!(
    benches,
    bench_insert_u64_tail,
    bench_btreemap_insert_u64_tail,
    bench_insert_u64_concurrent,
    bench_btreemap_insert_concurrent,
    bench_dashmap_insert_concurrent,
    bench_skipmap_insert_concurrent,
    bench_get_u64_concurrent,
    bench_btreemap_get_concurrent,
    bench_dashmap_get_concurrent,
    bench_skipmap_get_concurrent,
    bench_slot_node_reclaim_churn
);
criterion_main!(benches);
