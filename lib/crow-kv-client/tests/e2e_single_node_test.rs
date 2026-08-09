// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! End-to-end test against a real single-node `PxKvStore` + `KvService`
//! (no mocks): topology discovery over a small `/topology` HTTP server
//! backed by the store's real `status`, then `put`/`get`/`delete`/
//! `batch_write`/`scan` through [`crow_kv_client::CrowkvClient`], covering
//! `ReadMode` routing and the `MinSlot` watermark
//! (C1-C3).

use std::sync::{Arc, Mutex};

use axum::extract::State;
use axum::routing::get;
use axum::{Json, Router};

use crow_kv::cluster::group::PxGroup;
use crow_kv::cluster::kv_server::KvServer;
use crow_kv::cluster::local_replica::{PxLocalReplica, PxLocalReplicaRole};
use crow_kv::cluster::px_kv_store::PxKvStore;
use crow_kv::metrics::MetricsRegistry;

use bytes::Bytes;
use crow_kv_client::{BatchOp, ClientConfig, CrowkvClient, GetOutcome, ReadMode};

const STORE_ID: u64 = 1;
const GROUP_ID: u64 = 1;

/// Starts a single-node, single-replica group already believing itself
/// leader (mirrors `crow_kv/tests/testkit/cluster.rs::start_cluster_inner`
/// for a 1-node cluster — no election driver needed).
async fn start_single_node_store() -> Arc<PxKvStore> {
    let replica = PxLocalReplica::new(STORE_ID, PxLocalReplicaRole::Leader);
    let store = PxKvStore::new(STORE_ID, "127.0.0.1:0".parse().unwrap());
    let server = Arc::new(store);

    let group = PxGroup::new(GROUP_ID, replica);
    server.add_group(group);
    server.start().await.expect("failed to start KvStore");
    server
}

/// Serves `GET /topology` returning the live `store.status` each time,
/// so the client's cache reflects whatever this store currently knows.
async fn spawn_topology_server(store: Arc<PxKvStore>) -> String {
    async fn handler(State(store): State<Arc<PxKvStore>>) -> Json<serde_json::Value> {
        Json(serde_json::json!({ "stores": [store.status()] }))
    }
    let app = Router::new().route("/topology", get(handler)).with_state(store);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}

#[tokio::test]
async fn put_get_delete_round_trip_via_topology_discovery() {
    let store = start_single_node_store().await;
    let seed = spawn_topology_server(store.clone()).await;
    let client = CrowkvClient::new(ClientConfig::new(vec![seed]));

    // No leader cached yet -- `put` must discover it via `/topology`.
    let outcome = client
        .put(STORE_ID, GROUP_ID, b"k1", b"v1", None)
        .await
        .expect("put");
    assert!(outcome.revision > 0);

    match client
        .get(STORE_ID, GROUP_ID, b"k1", ReadMode::Linearizable, None)
        .await
        .unwrap()
    {
        GetOutcome::Found { value, .. } => assert_eq!(value.as_ref(), b"v1"),
        GetOutcome::NotFound => panic!("expected k1 to be found"),
    }

    let del = client
        .delete(STORE_ID, GROUP_ID, b"k1", None)
        .await
        .expect("delete");
    assert!(
        del.revision > outcome.revision,
        "delete of an existing key gets a fresh, higher revision"
    );

    match client
        .get(STORE_ID, GROUP_ID, b"k1", ReadMode::MinSlot, None)
        .await
        .unwrap()
    {
        GetOutcome::NotFound => {}
        GetOutcome::Found { .. } => panic!("k1 should have been deleted"),
    }

    store.stop();
    store.join().await;
}

#[tokio::test]
async fn batch_write_and_scan() {
    let store = start_single_node_store().await;
    let seed = spawn_topology_server(store.clone()).await;
    let client = CrowkvClient::new(ClientConfig::new(vec![seed]));

    client
        .batch_write(
            STORE_ID,
            GROUP_ID,
            &[
                BatchOp::Put {
                    key: Bytes::from_static(b"a1"),
                    value: Bytes::from_static(b"1"),
                },
                BatchOp::Put {
                    key: Bytes::from_static(b"a2"),
                    value: Bytes::from_static(b"2"),
                },
            ],
        )
        .await
        .expect("batch_write");

    let scanned = client
        .scan(STORE_ID, GROUP_ID, b"a", &[], &[], 0, ReadMode::MinSlot, None)
        .await
        .expect("scan");
    assert_eq!(scanned.items.len(), 2);
    assert!(!scanned.truncated);

    store.stop();
    store.join().await;
}

#[tokio::test]
async fn read_your_writes_uses_auto_tracked_watermark() {
    let store = start_single_node_store().await;
    let seed = spawn_topology_server(store.clone()).await;
    let client = CrowkvClient::new(ClientConfig::new(vec![seed]));

    assert_eq!(client.read_your_writes_slot(STORE_ID, GROUP_ID), 0);

    let write = client
        .put(STORE_ID, GROUP_ID, b"session:42", b"active", None)
        .await
        .expect("put");
    assert_eq!(client.read_your_writes_slot(STORE_ID, GROUP_ID), write.revision);

    // No explicit `min_slot` -- the client auto-attaches its own
    // last-write watermark for `MinSlot`.
    match client
        .get(STORE_ID, GROUP_ID, b"session:42", ReadMode::MinSlot, None)
        .await
        .unwrap()
    {
        GetOutcome::Found { value, .. } => assert_eq!(value.as_ref(), b"active"),
        GetOutcome::NotFound => panic!("expected to observe our own write"),
    }

    store.stop();
    store.join().await;
}

/// A multi-page `Linearizable` scan pays the leader read barrier once
/// (page 1) then switches subsequent pages to `MinSlot` with page-1's
/// `read_slot` as the freshness floor — skipping the per-page barrier.
/// Verified by checking the store's `lease_path + readindex_path` counter
/// is 1 (not N) after an N-page scan.
#[tokio::test]
async fn linearizable_multi_page_scan_pays_barrier_once() {
    let registry = Arc::new(Mutex::new(MetricsRegistry::new()));
    let replica = PxLocalReplica::new(STORE_ID, PxLocalReplicaRole::Leader);
    let mut store = PxKvStore::new(STORE_ID, "127.0.0.1:0".parse().unwrap());
    store.set_metrics_registry(Arc::clone(&registry));
    // Small byte budget: 12 keys × (3B key + 20B value) = 23B/entry;
    // budget=100 → 4 entries/page → 3 pages → 3 barriers without the carry.
    store.set_scan_byte_budget(100);
    let server = Arc::new(store);

    let group = PxGroup::new(GROUP_ID, replica);
    server.add_group(group);
    server.start().await.expect("failed to start KvStore");

    let seed = spawn_topology_server(server.clone()).await;
    let client = CrowkvClient::new(ClientConfig::new(vec![seed]));

    // Write 12 keys: "k00".."k11", each with a 20-byte value.
    for i in 0..12u32 {
        let key = format!("k{i:02}");
        client
            .put(STORE_ID, GROUP_ID, key.as_bytes(), b"v0123456789abcdefxxx", None)
            .await
            .expect("put");
    }

    let scanned = client
        .scan(
            STORE_ID,
            GROUP_ID,
            b"k",
            &[],
            &[],
            0,
            ReadMode::Linearizable,
            None,
        )
        .await
        .expect("scan");

    // All 12 keys returned in order — no duplicates, no gaps.
    assert_eq!(scanned.items.len(), 12, "all 12 keys returned");
    for (i, (key, _)) in scanned.items.iter().enumerate() {
        assert_eq!(key.as_ref(), format!("k{i:02}").as_bytes(), "key {i} in order");
    }

    // Only page 1 paid the leader barrier; pages 2..3 used MinSlot.
    let snap = registry.lock().unwrap().snapshot("s.1.g.1.read.");
    let count = |suffix: &str| {
        snap.iter()
            .find(|(n, _)| n.ends_with(suffix))
            .and_then(|(_, v)| v.strip_prefix("c:"))
            .and_then(|v| v.split(':').next())
            .and_then(|n| n.parse::<u64>().ok())
            .unwrap_or(0)
    };
    let lease = count("read.lease_path.c");
    let readindex = count("read.readindex_path.c");
    let barriers = lease + readindex;
    assert_eq!(
        barriers, 1,
        "multi-page Linearizable scan should pay 1 barrier (page 1 only), got {barriers} (lease={lease}, readindex={readindex})"
    );

    server.stop();
    server.join().await;
}
