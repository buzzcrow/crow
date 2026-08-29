// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Integration tests for R19 read-path metrics: barrier latency, lease vs
//! `ReadIndex` path counters, `engine_get` latency, MinSlot-fallback counter,
//! and read-state gauges.

use crowdb_kv::cluster::group::PxGroup;
use crowdb_kv::cluster::kv_store::KvStore;
use crowdb_kv::cluster::{PxKvStore, PxLocalReplica, PxLocalReplicaRole};
use crowdb_kv::metrics::MetricsRegistry;
use std::sync::{Arc, Mutex};

fn leader_group(group_id: u64, my_id: u64) -> PxGroup {
    let local = PxLocalReplica::new(my_id, PxLocalReplicaRole::Leader);
    PxGroup::new(group_id, local)
}

fn store_with_registry() -> (PxKvStore, Arc<Mutex<MetricsRegistry>>) {
    let registry = Arc::new(Mutex::new(MetricsRegistry::new()));
    let mut store = PxKvStore::new(0, "127.0.0.1:0".parse().unwrap());
    store.set_metrics_registry(Arc::clone(&registry));
    (store, registry)
}

/// Look up a metric value by name prefix from a registry snapshot.
fn snap(reg: &Arc<Mutex<MetricsRegistry>>, prefix: &str) -> Vec<(String, String)> {
    reg.lock().unwrap().snapshot(prefix)
}

#[tokio::test]
async fn linearizable_read_records_barrier_engine_and_path_counters() {
    let (store, registry) = store_with_registry();
    store.add_group(leader_group(1, 1));

    // Establish an applied frontier so reads have a non-zero slot.
    let put = store.kv_put(1, b"rk", b"rv", 11, 1, 1, 1).await;
    assert!(put.ok);
    let slot = put.revision;

    // First linearizable get: lease starts expired → ReadIndex path.
    // run_heartbeat_round on a single-voter leader trivially gets quorum
    // and renews the lease.
    let r1 = store.kv_get(1, b"rk", 0, 0, 2, 2).await;
    assert!(r1.ok && r1.value.as_ref() == b"rv");

    // Second linearizable get: lease is now valid → lease fast path.
    let r2 = store.kv_get(1, b"rk", 0, 0, 3, 3).await;
    assert!(r2.ok && r2.value.as_ref() == b"rv");

    let s = snap(&registry, "s.0.g.1.read.");
    let find = |suffix: &str| {
        s.iter()
            .find(|(n, _)| n.ends_with(suffix))
            .map_or("", |(_, v)| v.as_str())
    };

    // readindex_path counter: 1 (first get)
    let ri = find("read.readindex_path.c");
    assert!(ri.starts_with("c:1:"), "readindex_path count=1, got {ri}");

    // lease_path counter: 1 (second get)
    let lp = find("read.lease_path.c");
    assert!(lp.starts_with("c:1:"), "lease_path count=1, got {lp}");

    // barrier latency summary: 2 observations
    let bl = find("read.barrier.l");
    assert!(bl.starts_with("l:2:"), "barrier summary count=2, got {bl}");

    // engine_get latency summary: 2 observations
    let eg = find("read.engine_get.l");
    assert!(eg.starts_with("l:2:"), "engine_get summary count=2, got {eg}");

    // lease_valid gauge: 1 (lease was valid at the second barrier)
    let lv = find("read.lease_valid.g");
    assert!(lv == "g:1", "lease_valid gauge=1, got {lv}");

    // contiguous_applied gauge: reflects the applied frontier
    let ca = find("read.contiguous_applied.g");
    assert!(
        ca == format!("g:{slot}"),
        "contiguous_applied gauge={slot}, got {ca}"
    );

    // safe_slot gauge: 0 for a single-node group with no peer reports
    let ss = find("read.safe_slot.g");
    assert!(ss == "g:0", "safe_slot gauge=0, got {ss}");
}

#[tokio::test]
async fn minslot_fallback_counter_increments_on_stale_frontier() {
    let (store, registry) = store_with_registry();
    store.add_group(leader_group(1, 1));

    let put = store.kv_put(1, b"rk", b"rv", 11, 1, 1, 1).await;
    assert!(put.ok);
    let slot = put.revision;

    // MinSlot with a future min_slot → redirect (fallback).
    let r = store.kv_get(1, b"rk", 1, slot + 100, 2, 2).await;
    assert!(!r.ok, "min_slot past frontier must redirect");

    // MinSlot with caught-up min_slot → served (no fallback).
    let r2 = store.kv_get(1, b"rk", 1, slot, 3, 3).await;
    assert!(r2.ok, "min_slot at frontier should serve");

    let s = snap(&registry, "s.0.g.1.read.minslot_fallback.c");
    assert_eq!(s.len(), 1, "minslot_fallback counter should be registered");
    assert!(
        s[0].1.starts_with("c:1:"),
        "minslot_fallback count=1, got {}",
        s[0].1
    );
}

#[tokio::test]
async fn read_metrics_appear_in_flush_output() {
    let (store, registry) = store_with_registry();
    store.add_group(leader_group(1, 1));
    let put = store.kv_put(1, b"rk", b"rv", 11, 1, 1, 1).await;
    assert!(put.ok);
    let slot = put.revision;

    // Exercise all paths so no counter is zero-suppressed:
    // 1st linearizable → ReadIndex (lease expired), 2nd → lease path.
    let _ = store.kv_get(1, b"rk", 0, 0, 2, 2).await;
    let _ = store.kv_get(1, b"rk", 0, 0, 3, 3).await;
    // MinSlot fallback (future min_slot → redirect).
    let _ = store.kv_get(1, b"rk", 1, slot + 100, 4, 4).await;

    let mut buf = Vec::new();
    registry
        .lock()
        .unwrap()
        .flush(&mut buf, 5.0, "2026-07-22T00:00:00Z");
    let out = String::from_utf8(buf).unwrap();

    // Counters (all non-zero → not suppressed)
    assert!(out.contains("s.0.g.1.read.lease_path.c"));
    assert!(out.contains("s.0.g.1.read.readindex_path.c"));
    assert!(out.contains("s.0.g.1.read.minslot_fallback.c"));
    // Summaries
    assert!(out.contains("s.0.g.1.read.barrier.l"));
    assert!(out.contains("s.0.g.1.read.engine_get.l"));
    // Gauges (always printed, but zero-value gauges are suppressed)
    assert!(out.contains("s.0.g.1.read.lease_valid.g"));
    assert!(out.contains("s.0.g.1.read.contiguous_applied.g"));
    // safe_slot.g is 0 for a single-node group → zero-suppressed;
    // verified via snapshot in the path-counters test above.
}

#[tokio::test]
async fn lease_path_plus_readindex_path_equals_linearizable_get_count() {
    let (store, registry) = store_with_registry();
    store.add_group(leader_group(1, 1));
    store.kv_put(1, b"rk", b"rv", 11, 1, 1, 1).await;

    // Three linearizable gets: first is ReadIndex, rest are lease path.
    for i in 2..=4 {
        let r = store.kv_get(1, b"rk", 0, 0, i, i).await;
        assert!(r.ok);
    }

    let s = snap(&registry, "s.0.g.1.read.");
    let count = |suffix: &str| {
        s.iter()
            .find(|(n, _)| n.ends_with(suffix))
            .and_then(|(_, v)| v.strip_prefix("c:"))
            .and_then(|v| v.split(':').next())
            .and_then(|n| n.parse::<u64>().ok())
            .unwrap_or(0)
    };

    let lease = count("read.lease_path.c");
    let readindex = count("read.readindex_path.c");
    assert_eq!(
        lease + readindex,
        3,
        "lease_path({lease}) + readindex_path({readindex}) should equal 3 linearizable gets"
    );
}
