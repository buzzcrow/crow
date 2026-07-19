// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Engine + paxos stats collector: bridges paxos slot watermarks and
//! snapshot.pages.c (a magnitude counter with no paired latency in the
//! C++ registry) into the Rust `MetricsRegistry` so they appear in the
//! periodic metrics log. C++ engine counters/gauges/summaries/bandwidths
//! are now flushed natively via the `[cpp-metrics]` section (FFI string
//! from `CrowtreeEngine::flush_metrics_str`).

use std::sync::{Arc, Mutex};

use crate::store_registry::KvStoreRegistry;
use crowkv::cluster::px_kv_store::PxKvStore;
use crowkv::kv::CrowtreeEngine;
use crowkv::metrics::{Counter, Gauge, MetricsRegistry, MetricsRunner};

/// Registered Rust handles for one (store, group): Paxos gauges +
/// snapshot.pages.c bridge counter.
struct EngineHandles {
    paxos_chosen: Arc<Gauge>,
    paxos_applied: Arc<Gauge>,
    paxos_last_chosen: Arc<Gauge>,
    paxos_highest_seen: Arc<Gauge>,
    paxos_term: Arc<Gauge>,
    snapshot_pages: Arc<Counter>,
}

impl EngineHandles {
    fn register(registry: &mut MetricsRegistry, store_id: u64, group_id: u64) -> Self {
        Self {
            paxos_chosen: registry.register_gauge(format!("s.{store_id}.g.{group_id}.paxos.chosen_slot.g")),
            paxos_applied: registry.register_gauge(format!("s.{store_id}.g.{group_id}.paxos.applied_slot.g")),
            paxos_last_chosen: registry
                .register_gauge(format!("s.{store_id}.g.{group_id}.paxos.last_chosen_slot.g")),
            paxos_highest_seen: registry
                .register_gauge(format!("s.{store_id}.g.{group_id}.paxos.highest_seen_slot.g")),
            paxos_term: registry.register_gauge(format!("s.{store_id}.g.{group_id}.paxos.current_term.g")),
            snapshot_pages: registry
                .register_counter(format!("s.{store_id}.g.{group_id}.tree.snapshot.pages.c")),
        }
    }
}

/// Paxos slot watermarks read per group for gauge export.
#[derive(Default, Clone, Copy)]
struct PaxosGauges {
    contiguous_chosen: u64,
    contiguous_applied: u64,
    last_chosen_slot: u64,
    highest_seen_slot: u64,
    current_term: u64,
}

/// Read paxos slot watermarks per group.
fn read_paxos_gauges_per_group(store: &Arc<PxKvStore>) -> std::collections::HashMap<u64, PaxosGauges> {
    let mut result: std::collections::HashMap<u64, PaxosGauges> = std::collections::HashMap::new();
    store.for_each_group(|group| {
        let replica = group.local_replica();
        result.insert(
            group.group_id(),
            PaxosGauges {
                contiguous_chosen: replica.contiguous_chosen(),
                contiguous_applied: replica.contiguous_applied(),
                last_chosen_slot: replica.last_chosen_slot(),
                highest_seen_slot: replica.highest_seen_slot(),
                current_term: replica.current_term_snapshot(),
            },
        );
    });
    result
}

/// Set up the engine stats collector and C++ metrics flush on the `MetricsRunner`.
///
/// Registers Paxos gauge handles + snapshot.pages.c bridge counter per
/// (store, group), installs a pre-flush collector that polls Paxos
/// watermarks and snapshot.pages deltas, and installs a post-flush C++
/// callback that calls `flush_metrics_str` per engine and writes the
/// `[cpp-metrics]` block.
///
/// # Panics
///
/// Panics if the metrics registry mutex is poisoned.
#[allow(clippy::too_many_lines)]
pub fn setup_engine_collector(
    registry: &Arc<Mutex<MetricsRegistry>>,
    store_registry: &Arc<KvStoreRegistry>,
    runner: &mut MetricsRunner,
) {
    type Key = (u64, u64);

    let mut handles: Vec<(Key, EngineHandles)> = Vec::new();
    {
        let mut reg = registry.lock().expect("metrics registry poisoned");
        for entry in &store_registry.stores {
            let store_id = *entry.key();
            let store = entry.value();
            store.for_each_group(|group| {
                handles.push((
                    (store_id, group.group_id()),
                    EngineHandles::register(&mut reg, store_id, group.group_id()),
                ));
            });
        }
    }

    let last_snapshot_pages: Arc<Mutex<std::collections::HashMap<Key, u64>>> =
        Arc::new(Mutex::new(std::collections::HashMap::new()));

    let handles: Arc<Mutex<Vec<(Key, EngineHandles)>>> = Arc::new(Mutex::new(handles));
    let known_keys: Arc<Mutex<std::collections::HashSet<Key>>> = Arc::new(Mutex::new(
        handles
            .lock()
            .expect("handles poisoned")
            .iter()
            .map(|(k, _)| *k)
            .collect(),
    ));

    let stores = Arc::clone(store_registry);
    let reg = Arc::clone(registry);

    // Pre-flush collector: poll Paxos gauges + snapshot.pages.c delta.
    runner.set_collector(move || {
        let new_keys: Vec<Key> = {
            let known = known_keys.lock().expect("known_keys poisoned");
            let mut scan: Vec<Key> = Vec::new();
            for entry in &stores.stores {
                let store_id = *entry.key();
                let store = entry.value();
                store.for_each_group(|group| {
                    let key = (store_id, group.group_id());
                    if !known.contains(&key) {
                        scan.push(key);
                    }
                });
            }
            scan
        };
        if !new_keys.is_empty() {
            let mut reg = reg.lock().expect("metrics registry poisoned");
            let mut h = handles.lock().expect("handles poisoned");
            let mut known = known_keys.lock().expect("known_keys poisoned");
            for key @ (sid, gid) in new_keys {
                h.push((key, EngineHandles::register(&mut reg, sid, gid)));
                known.insert(key);
            }
        }

        let h = handles.lock().expect("handles poisoned");
        for (key @ (store_id, _group_id), hd) in &*h {
            let Some(store) = stores.get_store(*store_id) else {
                continue;
            };

            // Paxos gauges: set directly from replica watermarks.
            let per_group_p = read_paxos_gauges_per_group(&store);
            if let Some(p) = per_group_p.get(&key.1) {
                hd.paxos_chosen.set(p.contiguous_chosen);
                hd.paxos_applied.set(p.contiguous_applied);
                hd.paxos_last_chosen.set(p.last_chosen_slot);
                hd.paxos_highest_seen.set(p.highest_seen_slot);
                hd.paxos_term.set(p.current_term);
            }

            // snapshot.pages.c: delta from cumulative snapshot_pages_total.
            store.for_each_group(|group| {
                if group.group_id() != key.1 {
                    return;
                }
                let replica = group.local_replica();
                let engine = replica.learner.engine();
                if let Some(e) = engine.as_any().downcast_ref::<CrowtreeEngine>() {
                    let s = e.stats();
                    let current = s.snapshot_pages_total;
                    let mut last = last_snapshot_pages.lock().expect("last_snapshot_pages poisoned");
                    let prev = last.entry(*key).or_default();
                    let delta = current.saturating_sub(*prev);
                    *prev = current;
                    drop(last);
                    if delta > 0 {
                        hd.snapshot_pages.inc_by(delta);
                    }
                }
            });
        }
    });

    // Post-flush C++ callback: call flush_metrics_str per engine.
    let stores2 = Arc::clone(store_registry);
    runner.set_cpp_flush(move |writer, window_secs, timestamp, rust_width| {
        for entry in &stores2.stores {
            let store = entry.value();
            store.for_each_group(|group| {
                let replica = group.local_replica();
                let engine = replica.learner.engine();
                if let Some(e) = engine.as_any().downcast_ref::<CrowtreeEngine>() {
                    let cpp_max = e.max_name_len();
                    let shared_width = rust_width.max(cpp_max);
                    let str = e.flush_metrics_str(window_secs, timestamp, shared_width);
                    if !str.is_empty() {
                        let _ = std::io::Write::write_all(writer, str.as_bytes());
                    }
                }
            });
        }
    });
}
