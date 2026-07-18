// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Engine stats collector: bridges C++ crowtree counters into the Rust
//! `MetricsRegistry` so they appear in the periodic metrics log.
//!
//! The C++ engine exposes cumulative counters via `ct_get_stats` (wrapped
//! by `CrowtreeEngine::stats()`). This module registers Rust `Counter`
//! handles in the `MetricsRegistry` per (store, group), then polls the
//! engine each metrics tick, computes deltas from the last poll, and
//! `inc_by(delta)`s so the metrics log shows per-window counts and
//! cumulative totals.

use std::sync::{Arc, Mutex};

use crate::store_registry::KvStoreRegistry;
use crowkv::cluster::px_kv_store::PxKvStore;
use crowkv::kv::CrowtreeEngine;
use crowkv::metrics::{Counter, Gauge, MetricsRegistry, MetricsRunner};

/// C++ engine counters that are bridged into the Rust metrics registry.
/// Each is a cumulative total read from `ct_get_stats`; we track the
/// last-seen value to compute per-window deltas.
#[derive(Default, Clone, Copy)]
struct EngineCounters {
    mt_upsert: u64,
    mt_get: u64,
    mt_get_hit: u64,
    flush_drain: u64,
    flush_entries: u64,
    l1_get: u64,
    l1_get_hit: u64,
    buf_hits: u64,
    buf_misses: u64,
    buf_evictions: u64,
    buf_writebacks: u64,
}

/// Point-in-time gauge values read from `ct_get_stats`.
/// Set directly each tick (no delta computation).
#[derive(Default, Clone, Copy)]
struct EngineGauges {
    resident: u32,
    dirty: u32,
    used: u32,
    num_frames: u32,
}

/// Registered Rust counter handles for one (store, group) engine metrics.
struct EngineHandles {
    mt_upsert: Arc<Counter>,
    mt_get: Arc<Counter>,
    mt_get_hit: Arc<Counter>,
    flush_drain: Arc<Counter>,
    flush_entries: Arc<Counter>,
    l1_get: Arc<Counter>,
    l1_get_hit: Arc<Counter>,
    buf_hits: Arc<Counter>,
    buf_misses: Arc<Counter>,
    buf_evictions: Arc<Counter>,
    buf_writebacks: Arc<Counter>,
    buf_resident: Arc<Gauge>,
    buf_dirty: Arc<Gauge>,
    buf_used: Arc<Gauge>,
    buf_num_frames: Arc<Gauge>,
}

impl EngineHandles {
    fn register(registry: &mut MetricsRegistry, store_id: u64, group_id: u64) -> Self {
        let p = format!("s.{store_id}.g.{group_id}.tree");
        Self {
            mt_upsert: registry.register_counter(format!("{p}.mt_upsert.c")),
            mt_get: registry.register_counter(format!("{p}.mt_get.c")),
            mt_get_hit: registry.register_counter(format!("{p}.mt_get_hit.c")),
            flush_drain: registry.register_counter(format!("{p}.flush_drain.c")),
            flush_entries: registry.register_counter(format!("{p}.flush_entries.c")),
            l1_get: registry.register_counter(format!("{p}.l1_get.c")),
            l1_get_hit: registry.register_counter(format!("{p}.l1_get_hit.c")),
            buf_hits: registry.register_counter(format!("{p}.buf.hits.c")),
            buf_misses: registry.register_counter(format!("{p}.buf.misses.c")),
            buf_evictions: registry.register_counter(format!("{p}.buf.evictions.c")),
            buf_writebacks: registry.register_counter(format!("{p}.buf.writebacks.c")),
            buf_resident: registry.register_gauge(format!("{p}.buf.resident.g")),
            buf_dirty: registry.register_gauge(format!("{p}.buf.dirty.g")),
            buf_used: registry.register_gauge(format!("{p}.buf.used.g")),
            buf_num_frames: registry.register_gauge(format!("{p}.buf.num_frames.g")),
        }
    }
}

/// Read cumulative counters from each group's local replica engine.
/// Returns a map of `group_id -> EngineCounters` for groups whose engine
/// is a `CrowtreeEngine`. Empty map if the store has no crowtree groups.
fn read_engine_counters_per_group(store: &Arc<PxKvStore>) -> std::collections::HashMap<u64, EngineCounters> {
    let mut result: std::collections::HashMap<u64, EngineCounters> = std::collections::HashMap::new();
    store.for_each_group(|group| {
        let replica = group.local_replica();
        let engine = replica.learner.engine();
        if let Some(e) = engine.as_any().downcast_ref::<CrowtreeEngine>() {
            let s = e.stats();
            result.insert(
                group.group_id(),
                EngineCounters {
                    mt_upsert: s.mt_upsert_total,
                    mt_get: s.mt_get_total,
                    mt_get_hit: s.mt_get_hit_total,
                    flush_drain: s.flush_drain_total,
                    flush_entries: s.flush_entries_total,
                    l1_get: s.l1_get_total,
                    l1_get_hit: s.l1_get_hit_total,
                    buf_hits: s.buffer_pool_hits,
                    buf_misses: s.buffer_pool_misses,
                    buf_evictions: s.buffer_pool_evictions,
                    buf_writebacks: s.buffer_pool_writebacks,
                },
            );
        }
    });
    result
}

/// Read point-in-time gauge values from each group's local replica engine.
/// Returns a map of `group_id -> EngineGauges` for groups whose engine
/// is a `CrowtreeEngine`. Empty map if the store has no crowtree groups.
fn read_engine_gauges_per_group(store: &Arc<PxKvStore>) -> std::collections::HashMap<u64, EngineGauges> {
    let mut result: std::collections::HashMap<u64, EngineGauges> = std::collections::HashMap::new();
    store.for_each_group(|group| {
        let replica = group.local_replica();
        let engine = replica.learner.engine();
        if let Some(e) = engine.as_any().downcast_ref::<CrowtreeEngine>() {
            let s = e.stats();
            result.insert(
                group.group_id(),
                EngineGauges {
                    resident: s.buffer_pool_resident,
                    dirty: s.buffer_pool_dirty,
                    used: s.buffer_pool_used,
                    num_frames: s.buffer_pool_num_frames,
                },
            );
        }
    });
    result
}

/// Compute per-window deltas from cumulative counters and increment handles.
fn apply_counter_deltas(hd: &EngineHandles, current: &EngineCounters, prev: &mut EngineCounters) {
    let delta = EngineCounters {
        mt_upsert: current.mt_upsert.saturating_sub(prev.mt_upsert),
        mt_get: current.mt_get.saturating_sub(prev.mt_get),
        mt_get_hit: current.mt_get_hit.saturating_sub(prev.mt_get_hit),
        flush_drain: current.flush_drain.saturating_sub(prev.flush_drain),
        flush_entries: current.flush_entries.saturating_sub(prev.flush_entries),
        l1_get: current.l1_get.saturating_sub(prev.l1_get),
        l1_get_hit: current.l1_get_hit.saturating_sub(prev.l1_get_hit),
        buf_hits: current.buf_hits.saturating_sub(prev.buf_hits),
        buf_misses: current.buf_misses.saturating_sub(prev.buf_misses),
        buf_evictions: current.buf_evictions.saturating_sub(prev.buf_evictions),
        buf_writebacks: current.buf_writebacks.saturating_sub(prev.buf_writebacks),
    };
    *prev = *current;

    if delta.mt_upsert > 0 {
        hd.mt_upsert.inc_by(delta.mt_upsert);
    }
    if delta.mt_get > 0 {
        hd.mt_get.inc_by(delta.mt_get);
    }
    if delta.mt_get_hit > 0 {
        hd.mt_get_hit.inc_by(delta.mt_get_hit);
    }
    if delta.flush_drain > 0 {
        hd.flush_drain.inc_by(delta.flush_drain);
    }
    if delta.flush_entries > 0 {
        hd.flush_entries.inc_by(delta.flush_entries);
    }
    if delta.l1_get > 0 {
        hd.l1_get.inc_by(delta.l1_get);
    }
    if delta.l1_get_hit > 0 {
        hd.l1_get_hit.inc_by(delta.l1_get_hit);
    }
    if delta.buf_hits > 0 {
        hd.buf_hits.inc_by(delta.buf_hits);
    }
    if delta.buf_misses > 0 {
        hd.buf_misses.inc_by(delta.buf_misses);
    }
    if delta.buf_evictions > 0 {
        hd.buf_evictions.inc_by(delta.buf_evictions);
    }
    if delta.buf_writebacks > 0 {
        hd.buf_writebacks.inc_by(delta.buf_writebacks);
    }
}

/// Set up the engine stats collector on the `MetricsRunner`.
///
/// Registers counter handles for each (store, group) present in the
/// registry at setup time, then installs a pre-flush collector callback
/// that polls engine stats, computes deltas, and increments the
/// registered counters.
///
/// # Panics
///
/// Panics if the metrics registry mutex is poisoned.
pub fn setup_engine_collector(
    registry: &Arc<Mutex<MetricsRegistry>>,
    store_registry: &Arc<KvStoreRegistry>,
    runner: &mut MetricsRunner,
) {
    // Key: (store_id, group_id).
    type Key = (u64, u64);

    // Register counter handles for each existing (store, group).
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

    // Track last-seen cumulative values per (store, group) for delta computation.
    let last_values: Arc<Mutex<std::collections::HashMap<Key, EngineCounters>>> =
        Arc::new(Mutex::new(std::collections::HashMap::new()));

    // Handles are behind a Mutex so dynamically-created stores/groups can be added.
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

    runner.set_collector(move || {
        // Register handles for stores/groups created dynamically via mgmt API.
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
            let per_group = read_engine_counters_per_group(&store);
            let Some(current) = per_group.get(&key.1) else {
                continue;
            };
            let mut last = last_values.lock().expect("last_values poisoned");
            let prev = last.entry(*key).or_default();
            apply_counter_deltas(hd, current, prev);
            drop(last);

            // Gauges: set directly from current engine state.
            let per_group_g = read_engine_gauges_per_group(&store);
            if let Some(g) = per_group_g.get(&key.1) {
                hd.buf_resident.set(u64::from(g.resident));
                hd.buf_dirty.set(u64::from(g.dirty));
                hd.buf_used.set(u64::from(g.used));
                hd.buf_num_frames.set(u64::from(g.num_frames));
            }
        }
    });
}
