// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Engine + paxos stats collector: bridges paxos slot watermarks and
//! snapshot.pages.c (a magnitude counter with no paired latency in the
//! C++ registry) into the Rust `MetricsRegistry` so they appear in the
//! periodic metrics log. C++ engine counters/gauges/summaries/bandwidths
//! are now flushed natively via the `[cpp-metrics]` section (FFI string
//! from `CrowTreeEngine::flush_metrics_str`).

use std::sync::{Arc, Mutex};

use crate::store_registry::KvStoreRegistry;
use crow_kv::cluster::px_kv_store::PxKvStore;
use crow_kv::kv::CrowTreeEngine;
use crow_kv::metrics::{Counter, Gauge, MetricsRegistry, MetricsRunner};
use crow_kv::wal::wal_engine::BlockDeviceSnapshot;
use crow_rpc_ffi::CrowRpcTransportStats;

/// Registered Rust handles for one (store, group): Paxos gauges +
/// snapshot.pages.c bridge counter.
struct EngineHandles {
    paxos_chosen: Arc<Gauge>,
    paxos_applied: Arc<Gauge>,
    paxos_last_chosen: Arc<Gauge>,
    paxos_highest_seen: Arc<Gauge>,
    paxos_term: Arc<Gauge>,
    paxos_inflight: Arc<Gauge>,
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
            paxos_inflight: registry
                .register_gauge(format!("s.{store_id}.g.{group_id}.paxos.inflight_slots.g")),
            snapshot_pages: registry
                .register_counter(format!("s.{store_id}.g.{group_id}.tree.snapshot.pages.c")),
        }
    }
}

/// Registered Rust handles for one store's crow-rpc transport stats:
/// syscall/frame aggregation counters (window-delta) + submit→writev
/// queue-wait gauges (cumulative snapshot).
struct RpcTransportHandles {
    read_calls: Arc<Counter>,
    writev_calls: Arc<Counter>,
    frames_sent: Arc<Counter>,
    frames_parsed: Arc<Counter>,
    read_bytes: Arc<Counter>,
    writev_bytes: Arc<Counter>,
    submit_to_writev_avg_us: Arc<Gauge>,
    submit_to_writev_count: Arc<Gauge>,
}

impl RpcTransportHandles {
    fn register(registry: &mut MetricsRegistry, store_id: u64) -> Self {
        Self {
            read_calls: registry.register_counter(format!("s.{store_id}.rpc.read_calls.c")),
            writev_calls: registry.register_counter(format!("s.{store_id}.rpc.writev_calls.c")),
            frames_sent: registry.register_counter(format!("s.{store_id}.rpc.frames_sent.c")),
            frames_parsed: registry.register_counter(format!("s.{store_id}.rpc.frames_parsed.c")),
            read_bytes: registry.register_counter(format!("s.{store_id}.rpc.read_bytes.c")),
            writev_bytes: registry.register_counter(format!("s.{store_id}.rpc.writev_bytes.c")),
            submit_to_writev_avg_us: registry
                .register_gauge(format!("s.{store_id}.rpc.submit_to_writev.avg_us.g")),
            submit_to_writev_count: registry
                .register_gauge(format!("s.{store_id}.rpc.submit_to_writev.count.g")),
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
    inflight_slots: u64,
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
                inflight_slots: group.inflight_slot_count(),
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
    let mut rpc_handles: Vec<(u64, RpcTransportHandles)> = Vec::new();
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
            rpc_handles.push((store_id, RpcTransportHandles::register(&mut reg, store_id)));
        }
    }

    let last_snapshot_pages: Arc<Mutex<std::collections::HashMap<Key, u64>>> =
        Arc::new(Mutex::new(std::collections::HashMap::new()));
    let last_block_device: Arc<Mutex<std::collections::HashMap<Key, BlockDeviceSnapshot>>> =
        Arc::new(Mutex::new(std::collections::HashMap::new()));
    let last_rpc_stats: Arc<Mutex<std::collections::HashMap<u64, CrowRpcTransportStats>>> =
        Arc::new(Mutex::new(std::collections::HashMap::new()));

    let handles: Arc<Mutex<Vec<(Key, EngineHandles)>>> = Arc::new(Mutex::new(handles));
    let rpc_handles: Arc<Mutex<Vec<(u64, RpcTransportHandles)>>> = Arc::new(Mutex::new(rpc_handles));
    let known_keys: Arc<Mutex<std::collections::HashSet<Key>>> = Arc::new(Mutex::new(
        handles
            .lock()
            .expect("handles poisoned")
            .iter()
            .map(|(k, _)| *k)
            .collect(),
    ));
    let known_rpc_stores: Arc<Mutex<std::collections::HashSet<u64>>> = Arc::new(Mutex::new(
        rpc_handles
            .lock()
            .expect("rpc_handles poisoned")
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

        // Dynamically register RPC transport handles for new stores.
        let new_rpc_stores: Vec<u64> = {
            let known = known_rpc_stores.lock().expect("known_rpc_stores poisoned");
            stores
                .stores
                .iter()
                .map(|e| *e.key())
                .filter(|sid| !known.contains(sid))
                .collect()
        };
        if !new_rpc_stores.is_empty() {
            let mut reg = reg.lock().expect("metrics registry poisoned");
            let mut rh = rpc_handles.lock().expect("rpc_handles poisoned");
            let mut known = known_rpc_stores.lock().expect("known_rpc_stores poisoned");
            for sid in new_rpc_stores {
                rh.push((sid, RpcTransportHandles::register(&mut reg, sid)));
                known.insert(sid);
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
                hd.paxos_inflight.set(p.inflight_slots);
            }

            // snapshot.pages.c: delta from cumulative snapshot_pages_total.
            store.for_each_group(|group| {
                if group.group_id() != key.1 {
                    return;
                }
                let replica = group.local_replica();
                let engine = replica.learner.engine();
                if let Some(e) = engine.as_any().downcast_ref::<CrowTreeEngine>() {
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

            // Block device counters: delta from cumulative WAL counters.
            store.for_each_group(|group| {
                if group.group_id() != key.1 {
                    return;
                }
                let replica = group.local_replica();
                if let Some(wal) = replica.wal() {
                    if let Some(snap) = wal.block_device_snapshot() {
                        let mut last = last_block_device.lock().expect("last_block_device poisoned");
                        let prev = last.entry(*key).or_default();
                        let d_logical = snap
                            .logical_bytes_written
                            .saturating_sub(prev.logical_bytes_written);
                        let d_physical = snap
                            .physical_bytes_written
                            .saturating_sub(prev.physical_bytes_written);
                        let d_rmw = snap.rmw_count.saturating_sub(prev.rmw_count);
                        *prev = snap;
                        drop(last);
                        if let Some(h) = wal.block_device_counter_handles() {
                            if d_logical > 0 {
                                h.logical_bytes.inc_by(d_logical);
                            }
                            if d_physical > 0 {
                                h.physical_bytes.inc_by(d_physical);
                            }
                            if d_rmw > 0 {
                                h.rmw.inc_by(d_rmw);
                            }
                        }
                    }
                }
            });
        }

        // RPC transport stats: delta from cumulative crow-rpc counters.
        let rh = rpc_handles.lock().expect("rpc_handles poisoned");
        for (store_id, hd) in &*rh {
            let Some(store) = stores.get_store(*store_id) else {
                continue;
            };
            let Some(cur) = store.rpc_transport_stats() else {
                continue;
            };
            let mut last = last_rpc_stats.lock().expect("last_rpc_stats poisoned");
            let prev = last.entry(*store_id).or_default();
            let d_read = cur.read_calls.saturating_sub(prev.read_calls);
            let d_writev = cur.writev_calls.saturating_sub(prev.writev_calls);
            let d_sent = cur.frames_sent.saturating_sub(prev.frames_sent);
            let d_parsed = cur.frames_parsed.saturating_sub(prev.frames_parsed);
            let d_rd_bytes = cur.read_bytes.saturating_sub(prev.read_bytes);
            let d_wr_bytes = cur.writev_bytes.saturating_sub(prev.writev_bytes);
            let sw = cur.submit_to_writev;
            *prev = cur;
            drop(last);
            if d_read > 0 {
                hd.read_calls.inc_by(d_read);
            }
            if d_writev > 0 {
                hd.writev_calls.inc_by(d_writev);
            }
            if d_sent > 0 {
                hd.frames_sent.inc_by(d_sent);
            }
            if d_parsed > 0 {
                hd.frames_parsed.inc_by(d_parsed);
            }
            if d_rd_bytes > 0 {
                hd.read_bytes.inc_by(d_rd_bytes);
            }
            if d_wr_bytes > 0 {
                hd.writev_bytes.inc_by(d_wr_bytes);
            }
            hd.submit_to_writev_avg_us
                .set(sw.sum_ns.checked_div(sw.count).unwrap_or(0) / 1000);
            hd.submit_to_writev_count.set(sw.count);
        }
    });

    // Post-flush C++ callback: call flush_metrics_str per engine, then
    // flush the C++ global registry (process-level metrics like
    // rpc.client.*).
    let stores2 = Arc::clone(store_registry);
    runner.set_cpp_flush(
        move |writer, window_secs, timestamp, rust_width, count_w, tps_w| {
            for entry in &stores2.stores {
                let store = entry.value();
                store.for_each_group(|group| {
                    let replica = group.local_replica();
                    let engine = replica.learner.engine();
                    if let Some(e) = engine.as_any().downcast_ref::<CrowTreeEngine>() {
                        let cpp_max = e.max_name_len();
                        let shared_width = rust_width.max(cpp_max);
                        let str =
                            e.flush_metrics_str_ext(window_secs, timestamp, shared_width, count_w, tps_w);
                        if !str.is_empty() {
                            let _ = std::io::Write::write_all(writer, str.as_bytes());
                        }
                    }
                });
            }
            // Flush C++ global registry (process-level metrics).
            let global_max = crow_tree_ffi::cpp_global_metrics_max_name_len();
            let shared_width = rust_width.max(global_max);
            if let Some(str) = crow_tree_ffi::flush_cpp_global_metrics(
                window_secs,
                timestamp,
                "cpp-metrics-global",
                shared_width,
                count_w,
                tps_w,
            ) {
                let _ = std::io::Write::write_all(writer, str.as_bytes());
            }
        },
    );

    // Pre-flush negotiate callback: query C++ for its preferred column
    // widths. Uses the first engine found (all engines share the same
    // C++ formatting defaults). The C++ global registry uses the same
    // defaults.
    let stores3 = Arc::clone(store_registry);
    runner.set_cpp_negotiate(move || {
        let mut result = (5, 7);
        let mut found = false;
        for entry in &stores3.stores {
            if found {
                break;
            }
            let store = entry.value();
            store.for_each_group(|group| {
                if found {
                    return;
                }
                let replica = group.local_replica();
                let engine = replica.learner.engine();
                if let Some(e) = engine.as_any().downcast_ref::<CrowTreeEngine>() {
                    result = e.negotiate_widths(5, 7);
                    found = true;
                }
            });
        }
        // C++ global registry uses the same column widths as per-engine.
        result
    });
}
