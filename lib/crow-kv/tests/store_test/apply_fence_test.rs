// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! R35 apply-fence integration test: with R17 (`async_engine_apply`) on,
//! a `put` followed by a linearizable `get` of the same key on the leader
//! returns the written value — the apply fence waits for the spawned
//! `learn_chosen` apply to land before serving the read, preserving
//! read-your-writes.
//!
//! Determinism: a test-only apply gate (`PxLearner::set_apply_gate_for_tests`)
//! parks the spawned R17 apply so the test can prove the fence actually
//! waits (rather than relying on scheduling to race the apply ahead of the
//! read). Without the fence, the read would return `not_found` while the
//! apply is parked; with the fence, the read blocks until the apply
//! completes and then returns the value.

use std::sync::Arc;
use std::time::Duration;

use crow_kv::cluster::group::PxGroup;
use crow_kv::cluster::kv_store::KvStore;
use crow_kv::cluster::{PxKvStore, PxLocalReplica, PxLocalReplicaRole};
use tokio::sync::Notify;

fn leader_group(group_id: u64, my_id: u64) -> PxGroup {
    let local = PxLocalReplica::new(my_id, PxLocalReplicaRole::Leader);
    let mut group = PxGroup::new(group_id, local);
    // R17 on: `learn_chosen` is spawned; the apply fence must gate the read.
    group.set_async_engine_apply(true);
    group
}

/// With R17 on and the spawned apply parked, a linearizable get of a
/// just-written key blocks at the apply fence until the apply completes,
/// then returns the written value (read-your-writes holds).
#[tokio::test]
async fn linearizable_read_waits_for_async_apply() {
    let store = Arc::new(PxKvStore::new(0, "127.0.0.1:0".parse().unwrap()));
    store.add_group(leader_group(1, 1));

    // Park the spawned R17 apply so the fence's slow path is exercised
    // deterministically (rather than racing the apply ahead of the read).
    let apply_gate = Arc::new(Notify::new());
    let group = store.get_group(1).expect("group exists");
    group
        .local_replica()
        .learner
        .set_apply_gate_for_tests(Arc::clone(&apply_gate));

    // Put: `contiguous_chosen` advances synchronously (write-side split),
    // but `contiguous_applied` does not — the spawned apply is parked.
    let put = store.kv_put(1, b"fence-key", b"fence-val", 7, 1, 1, 1).await;
    assert!(put.ok, "put should succeed");
    let slot = put.revision;
    assert!(slot > 0);
    let replica = group.local_replica();
    assert_eq!(
        replica.contiguous_chosen(),
        slot,
        "chosen frontier advances synchronously before propose returns"
    );
    assert!(
        replica.contiguous_applied() < slot,
        "applied frontier lags while the spawned apply is parked"
    );

    // Linearizable get: the barrier resolves `read_slot = contiguous_chosen
    // = slot`, then the apply fence waits for `contiguous_applied >= slot`.
    // The apply is parked, so the read blocks here.
    let store_for_get = Arc::clone(&store);
    let get_task = tokio::spawn(async move {
        tokio::time::timeout(
            Duration::from_secs(5),
            store_for_get.kv_get(1, b"fence-key", 0, 0, 2, 2),
        )
        .await
        .expect("fenced read did not complete within timeout")
    });

    // Give the fence a moment to park, then release the apply. The read
    // should then complete with the written value.
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(
        replica.contiguous_applied() < slot,
        "apply still parked before gate release"
    );
    apply_gate.notify_one();

    let get = get_task.await.expect("get task joined");
    assert!(get.ok, "fenced linearizable read should succeed");
    assert!(
        get.value.as_ref() == b"fence-val",
        "read-your-writes: get returns the just-written value"
    );
    assert_eq!(
        get.read_slot, slot,
        "read served at the chosen slot captured by the barrier"
    );
    assert_eq!(
        replica.contiguous_applied(),
        slot,
        "applied frontier caught up after the apply completed"
    );
}

/// With R17 on but the apply NOT parked, a linearizable get of a
/// just-written key returns the value via the fence fast path (the apply
/// has already completed by the time the read lands).
#[tokio::test]
async fn linearizable_read_fast_path_when_apply_done() {
    let store = Arc::new(PxKvStore::new(0, "127.0.0.1:0".parse().unwrap()));
    store.add_group(leader_group(1, 1));

    let put = store.kv_put(1, b"fast-key", b"fast-val", 8, 1, 1, 1).await;
    assert!(put.ok);
    let slot = put.revision;

    // Let the spawned apply complete (no gate set).
    let group = store.get_group(1).expect("group exists");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        if group.local_replica().contiguous_applied() >= slot {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "apply never completed");
        tokio::time::sleep(Duration::from_millis(2)).await;
    }

    let get = store.kv_get(1, b"fast-key", 0, 0, 2, 2).await;
    assert!(get.ok);
    assert_eq!(get.value.as_ref(), b"fast-val");
    assert_eq!(get.read_slot, slot);
}
