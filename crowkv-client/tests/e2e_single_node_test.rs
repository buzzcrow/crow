//! End-to-end test against a real single-node `PxKvStore` + `KvService`
//! (no mocks): topology discovery over a small `/topology` HTTP server
//! backed by the store's real `status()`, then `put`/`get`/`delete`/
//! `batch_write`/`scan` through [`crowkv_client::CrowkvClient`], covering
//! `ReadMode` routing and the `ReadYourWrites` watermark
//! (`doc/plan-client.md` §5 C1-C3).

use std::sync::Arc;

use axum::extract::State;
use axum::routing::get;
use axum::{Json, Router};

use crowkv::cluster::group::PxGroup;
use crowkv::cluster::kv_server::KvServer;
use crowkv::cluster::local_replica::{PxLocalReplica, PxLocalReplicaRole};
use crowkv::cluster::px_kv_store::PxKvStore;

use crowkv_client::{BatchOp, ClientConfig, CrowkvClient, GetOutcome, ReadMode};

const STORE_ID: u64 = 1;
const GROUP_ID: u64 = 1;

/// Starts a single-node, single-replica group already believing itself
/// leader (mirrors `crowkv/tests/testkit/cluster.rs::start_cluster_inner`
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

/// Serves `GET /topology` returning the live `store.status()` each time,
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
        GetOutcome::Found { value, .. } => assert_eq!(value, b"v1"),
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
        .get(STORE_ID, GROUP_ID, b"k1", ReadMode::BestEffort, None)
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
                    key: b"a1".to_vec(),
                    value: b"1".to_vec(),
                },
                BatchOp::Put {
                    key: b"a2".to_vec(),
                    value: b"2".to_vec(),
                },
            ],
        )
        .await
        .expect("batch_write");

    let scanned = client
        .scan(STORE_ID, GROUP_ID, b"a", 0, ReadMode::BestEffort)
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

    // No explicit `client_slot` -- the client auto-attaches its own
    // last-write watermark for `ReadYourWrites` (doc/plan-client.md §6
    // Issue 5).
    match client
        .get(STORE_ID, GROUP_ID, b"session:42", ReadMode::ReadYourWrites, None)
        .await
        .unwrap()
    {
        GetOutcome::Found { value, .. } => assert_eq!(value, b"active"),
        GetOutcome::NotFound => panic!("expected to observe our own write"),
    }

    store.stop();
    store.join().await;
}
