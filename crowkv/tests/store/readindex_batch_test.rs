// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Integration tests for R27 `ReadIndex` batching: concurrent
//! linearizable reads with an expired lease coalesce onto a single
//! heartbeat round instead of one round per read.
//!
//! Determinism: a test-only round gate
//! (`PxGroup::set_readindex_round_gate_for_tests`) holds the first
//! `ReadIndex` round open until the test has fired the full burst of
//! concurrent reads and confirmed they all queued onto the
//! pending-barrier batch. Releasing the gate then lets the single
//! round complete and resolve every waiter at once.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crowkv::cluster::group::PxGroup;
use crowkv::cluster::kv_store::KvStore;
use crowkv::cluster::{PxKvStore, PxLocalReplica, PxLocalReplicaRole, PxRemoteReplica};
use crowkv::metrics::MetricsRegistry;
use tokio::sync::oneshot;

fn leader_group(group_id: u64, my_id: u64) -> PxGroup {
    let local = PxLocalReplica::new(my_id, PxLocalReplicaRole::Leader);
    PxGroup::new(group_id, local)
}

fn store_with_registry() -> (Arc<PxKvStore>, Arc<Mutex<MetricsRegistry>>) {
    let registry = Arc::new(Mutex::new(MetricsRegistry::new()));
    let mut store = PxKvStore::new(0, SocketAddr::from(([127, 0, 0, 1], 0)));
    store.set_metrics_registry(Arc::clone(&registry));
    (Arc::new(store), registry)
}

fn count(reg: &Arc<Mutex<MetricsRegistry>>, suffix: &str) -> u64 {
    reg.lock()
        .unwrap()
        .snapshot("s.0.g.1.read.")
        .iter()
        .find(|(n, _)| n.ends_with(suffix))
        .and_then(|(_, v)| v.strip_prefix("c:"))
        .and_then(|v| v.split(':').next())
        .and_then(|n| n.parse::<u64>().ok())
        .unwrap_or(0)
}

/// Wait until the in-flight `ReadIndex` round has a pending-barrier batch
/// with exactly `waiters` queued reads. Bounded polling so a missed
/// enqueue fails the test fast rather than hanging.
async fn await_batch(group: &Arc<PxGroup>, waiters: usize) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        if group.has_pending_read_barrier_for_tests()
            && group.pending_read_barrier_waiters_for_tests() == waiters
        {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "batch never reached {waiters} waiters (in_flight={}, waiters={})",
            group.has_pending_read_barrier_for_tests(),
            group.pending_read_barrier_waiters_for_tests()
        );
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}

/// N concurrent linearizable reads with an expired lease, gated round →
/// all served by one `ReadIndex` round, all returning the same
/// `read_slot`, with `readindex_rounds.c == 1` and `readindex_path.c
/// == N`.
#[tokio::test]
async fn readindex_batch_serves_n_reads_with_one_round() {
    const N: usize = 4;
    let (store, registry) = store_with_registry();
    store.add_group(leader_group(1, 1));

    // Establish an applied frontier so reads have a value and a non-zero slot.
    let put = store.kv_put(1, b"rk", b"rv", 11, 1, 1, 1).await;
    assert!(put.ok);
    let slot = put.revision;

    let group = store.get_group(1).expect("group exists");

    // Hold the next ReadIndex round open so the burst deterministically
    // batches. The lease starts expired (fresh leader, no driver), so
    // every read takes the ReadIndex path.
    let (release_tx, release_rx) = oneshot::channel();
    group.set_readindex_round_gate_for_tests(release_rx);

    let mut handles = Vec::with_capacity(N);
    for i in 2..=N + 1 {
        let store = Arc::clone(&store);
        handles.push(tokio::spawn(async move {
            store.kv_get(1, b"rk", 0, 0, i as u64, i as u64).await
        }));
    }

    // Wait for exactly one round leader + N-1 batched waiters, then
    // release the single round.
    await_batch(&group, N - 1).await;
    release_tx.send(()).expect("release gate");

    let mut responses = Vec::with_capacity(N);
    for h in handles {
        let r = tokio::time::timeout(Duration::from_secs(5), h)
            .await
            .expect("read did not complete")
            .expect("task joined");
        responses.push(r);
    }

    // All reads succeed and observe the same freshness floor (the
    // pre-round contiguous_chosen, equal to the put's slot).
    for r in &responses {
        assert!(r.ok, "batched read should succeed");
        assert!(r.value.as_ref() == b"rv", "batched read returns the value");
        assert_eq!(
            r.read_slot, slot,
            "all batched reads share the pre-round read_slot"
        );
    }

    // One round served all N reads.
    assert_eq!(
        count(&registry, "read.readindex_rounds.c"),
        1,
        "one heartbeat round for the batch"
    );
    assert_eq!(
        count(&registry, "read.readindex_path.c"),
        N as u64,
        "all {N} reads took the ReadIndex path"
    );
}

/// N concurrent linearizable reads when the round cannot reach quorum →
/// all waiters receive `NoQuorum` (surfaced as `Unavailable`), still
/// served by a single round.
#[tokio::test]
async fn readindex_batch_propagates_no_quorum_to_all_waiters() {
    const N: usize = 3;
    let (store, registry) = store_with_registry();
    // 3-member group: leader + two unreachable voting remotes → quorum
    // (2) can never be reached, so the ReadIndex round returns NoQuorum.
    let mut group = leader_group(1, 1);
    group.add_remote_replica(PxRemoteReplica::new(2, "127.0.0.1:65501".to_string()));
    group.add_remote_replica(PxRemoteReplica::new(3, "127.0.0.1:65502".to_string()));
    store.add_group(group);

    // No put: with two unreachable voting remotes the group cannot reach
    // quorum, so a write would never commit. The reads only need to enter
    // the ReadIndex barrier; the key need not exist.
    let group = store.get_group(1).expect("group exists");
    let (release_tx, release_rx) = oneshot::channel();
    group.set_readindex_round_gate_for_tests(release_rx);

    let mut handles = Vec::with_capacity(N);
    for i in 2..=N + 1 {
        let store = Arc::clone(&store);
        handles.push(tokio::spawn(async move {
            store.kv_get(1, b"rk", 0, 0, i as u64, i as u64).await
        }));
    }

    await_batch(&group, N - 1).await;
    release_tx.send(()).expect("release gate");

    for h in handles {
        let r = tokio::time::timeout(Duration::from_secs(10), h)
            .await
            .expect("read did not complete")
            .expect("task joined");
        assert!(!r.ok, "no-quorum batched read must not succeed");
        assert!(
            r.not_leader_hint.is_empty(),
            "no-quorum surfaces as Unavailable, not a NotLeader redirect"
        );
    }

    assert_eq!(
        count(&registry, "read.readindex_rounds.c"),
        1,
        "one (failed) heartbeat round for the batch"
    );
}
