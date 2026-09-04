// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Engine + paxos stats collector: bridges paxos slot watermarks and
//! snapshot.pages.c (a magnitude counter with no paired latency in the
//! C++ registry) into the Rust `MetricsRegistry` so they appear in the
//! periodic metrics log. C++ engine counters/gauges/summaries/bandwidths
//! are now flushed natively via the `cpp-tree` section (FFI string
//! from `CrowdbTreeEngine::flush_metrics_str`).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::store_registry::KvStoreRegistry;
use arc_swap::ArcSwap;
use crowdb_kv::cluster::px_kv_store::PxKvStore;
use crowdb_kv::kv::CrowdbTreeEngine;
use crowdb_kv::metrics::{Counter, Gauge, MetricsRegistry, MetricsRunner};

/// Registered Rust handles for one (store, group): Paxos gauges +
/// snapshot.pages.c bridge counter.
struct EngineHandles {
    paxos_chosen: Arc<Gauge>,
    paxos_applied: Arc<Gauge>,
    paxos_highest_seen: Arc<Gauge>,
    paxos_term: Arc<Gauge>,
    paxos_leader: Arc<Gauge>,
    paxos_inflight: Arc<Gauge>,
    snapshot_pages: Arc<Counter>,
    last_snapshot_pages: AtomicU64,
    last_rmw_count: AtomicU64,
}

impl EngineHandles {
    fn register(registry: &mut MetricsRegistry, store_id: u64, group_id: u64) -> Self {
        Self {
            paxos_chosen: registry
                .register_gauge(format!("s.{store_id}.g.{group_id}.paxos.learn.slot.con.chosen.g")),
            paxos_applied: registry.register_gauge(format!(
                "s.{store_id}.g.{group_id}.paxos.learn.slot.con.applied.g"
            )),
            paxos_highest_seen: registry
                .register_gauge(format!("s.{store_id}.g.{group_id}.paxos.acp.slot.highest_seen.g")),
            paxos_term: registry.register_gauge(format!("s.{store_id}.g.{group_id}.paxos.current_term.g")),
            paxos_leader: registry.register_gauge(format!("s.{store_id}.g.{group_id}.paxos.leader_id.g")),
            paxos_inflight: registry
                .register_gauge(format!("s.{store_id}.g.{group_id}.paxos.inflight_slots.g")),
            snapshot_pages: registry
                .register_counter(format!("s.{store_id}.g.{group_id}.tree.snapshot.pages.c")),
            last_snapshot_pages: AtomicU64::new(0),
            last_rmw_count: AtomicU64::new(0),
        }
    }
}

/// Registered Rust handles for one store's crowdb-rpc transport stats:
/// submit→writev queue-wait gauge (cumulative snapshot).
struct RpcTransportHandles {
    submit_to_writev_avg_us: Arc<Gauge>,
}

impl RpcTransportHandles {
    fn register(registry: &mut MetricsRegistry, store_id: u64) -> Self {
        Self {
            submit_to_writev_avg_us: registry
                .register_gauge(format!("s.{store_id}.rpc.submit_to_writev.avg_us.g")),
        }
    }
}

/// Paxos slot watermarks read per group for gauge export.
#[derive(Default, Clone, Copy)]
struct PaxosGauges {
    contiguous_chosen: u64,
    contiguous_applied: u64,
    highest_seen_slot: u64,
    current_term: u64,
    leader_id: u64,
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
                highest_seen_slot: replica.highest_seen_slot(),
                current_term: replica.current_term_snapshot(),
                leader_id: group.leader_id(),
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
/// `cpp-tree` block.
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

    let mut handles: Vec<(Key, Arc<EngineHandles>)> = Vec::new();
    let mut rpc_handles: Vec<(u64, Arc<RpcTransportHandles>)> = Vec::new();
    {
        let mut reg = registry.lock().expect("metrics registry poisoned");
        for entry in &store_registry.stores {
            let store_id = *entry.key();
            let store = entry.value();
            store.for_each_group(|group| {
                handles.push((
                    (store_id, group.group_id()),
                    Arc::new(EngineHandles::register(&mut reg, store_id, group.group_id())),
                ));
            });
            rpc_handles.push((
                store_id,
                Arc::new(RpcTransportHandles::register(&mut reg, store_id)),
            ));
        }
    }

    let handles = Arc::new(ArcSwap::from_pointee(handles));
    let rpc_handles = Arc::new(ArcSwap::from_pointee(rpc_handles));

    let stores = Arc::clone(store_registry);
    let reg = Arc::clone(registry);

    // Pre-flush collector: poll Paxos gauges + snapshot.pages.c delta.
    runner.set_collector(move || {
        let current_handles = handles.load_full();
        let known_keys: std::collections::HashSet<Key> =
            current_handles.iter().map(|(key, _)| *key).collect();
        let new_keys: Vec<Key> = {
            let mut scan: Vec<Key> = Vec::new();
            for entry in &stores.stores {
                let store_id = *entry.key();
                let store = entry.value();
                store.for_each_group(|group| {
                    let key = (store_id, group.group_id());
                    if !known_keys.contains(&key) {
                        scan.push(key);
                    }
                });
            }
            scan
        };
        if !new_keys.is_empty() {
            let mut reg = reg.lock().expect("metrics registry poisoned");
            let mut next = (*current_handles).clone();
            for key @ (sid, gid) in new_keys {
                next.push((key, Arc::new(EngineHandles::register(&mut reg, sid, gid))));
            }
            handles.store(Arc::new(next));
        }

        // Dynamically register RPC transport handles for new stores.
        let current_rpc_handles = rpc_handles.load_full();
        let known_rpc_stores: std::collections::HashSet<u64> = current_rpc_handles
            .iter()
            .map(|(store_id, _)| *store_id)
            .collect();
        let new_rpc_stores: Vec<u64> = {
            stores
                .stores
                .iter()
                .map(|e| *e.key())
                .filter(|sid| !known_rpc_stores.contains(sid))
                .collect()
        };
        if !new_rpc_stores.is_empty() {
            let mut reg = reg.lock().expect("metrics registry poisoned");
            let mut next = (*current_rpc_handles).clone();
            for sid in new_rpc_stores {
                next.push((sid, Arc::new(RpcTransportHandles::register(&mut reg, sid))));
            }
            rpc_handles.store(Arc::new(next));
        }

        let current_handles = handles.load();
        for (key @ (store_id, _group_id), hd) in current_handles.iter() {
            let Some(store) = stores.get_store(*store_id) else {
                continue;
            };

            // Paxos gauges: set directly from replica watermarks.
            let per_group_p = read_paxos_gauges_per_group(&store);
            if let Some(p) = per_group_p.get(&key.1) {
                hd.paxos_chosen.set(p.contiguous_chosen);
                hd.paxos_applied.set(p.contiguous_applied);
                hd.paxos_highest_seen.set(p.highest_seen_slot);
                hd.paxos_term.set(p.current_term);
                hd.paxos_leader.set(p.leader_id);
                hd.paxos_inflight.set(p.inflight_slots);
            }

            // snapshot.pages.c: delta from cumulative snapshot_pages_total.
            store.for_each_group(|group| {
                if group.group_id() != key.1 {
                    return;
                }
                let replica = group.local_replica();
                let engine = replica.learner.engine();
                if let Some(e) = engine.as_any().downcast_ref::<CrowdbTreeEngine>() {
                    let s = e.stats();
                    let current = s.snapshot_pages_total;
                    let previous = hd.last_snapshot_pages.swap(current, Ordering::Relaxed);
                    let delta = current.saturating_sub(previous);
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
                        let previous = hd.last_rmw_count.swap(snap.rmw_count, Ordering::Relaxed);
                        let d_rmw = snap.rmw_count.saturating_sub(previous);
                        if let Some(h) = wal.block_device_counter_handles() {
                            if d_rmw > 0 {
                                h.rmw.inc_by(d_rmw);
                            }
                        }
                    }
                }
            });
        }

        // RPC transport stats: delta from cumulative crowdb-rpc counters.
        let current_rpc_handles = rpc_handles.load();
        for (store_id, hd) in current_rpc_handles.iter() {
            let Some(store) = stores.get_store(*store_id) else {
                continue;
            };
            let Some(cur) = store.rpc_transport_stats() else {
                continue;
            };
            let sw = cur.submit_to_writev;
            hd.submit_to_writev_avg_us
                .set(sw.sum_ns.checked_div(sw.count).unwrap_or(0) / 1000);
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
                    if let Some(e) = engine.as_any().downcast_ref::<CrowdbTreeEngine>() {
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
            let global_max = crowdb_tree_ffi::cpp_global_metrics_max_name_len();
            let shared_width = rust_width.max(global_max);
            if let Some(str) = crowdb_tree_ffi::flush_cpp_global_metrics(
                window_secs,
                timestamp,
                "cpp-rpc",
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
        let mut result = (7, 7);
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
                if let Some(e) = engine.as_any().downcast_ref::<CrowdbTreeEngine>() {
                    result = e.negotiate_widths(7, 7);
                    found = true;
                }
            });
        }
        // C++ global registry uses the same column widths as per-engine.
        result
    });
}
