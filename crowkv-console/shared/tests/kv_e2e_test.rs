//! C6 end-to-end: spawn `crowkv-server`, create a store/group through the
//! management API, then via the gRPC `KvClient` exercise put → get →
//! delete → get-not-found.

use std::time::Duration;

use crowkv_console_shared::clients::grpc::{GetOutcome, KvClient};
use crowkv_console_shared::clients::http::ServerClient;
use crowkv_console_shared::config::NodeEntry;
use crowkv_console_shared::lifecycle::{self, crowkv_server_bin, DeployRequest};
use crowkv_console_shared::mgmt::{AddGroupRequest, AddStoreRequest};

fn pick_free_port() -> u16 {
    let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    port
}

async fn spawn_server() -> Option<(u32, String)> {
    let bin = crowkv_server_bin()?;
    if !bin.exists() {
        return None;
    }
    let node = NodeEntry {
        id: "n1".into(),
        rack_id: "r1".into(),
        host: "127.0.0.1".into(),
        ssh_port: 22,
        ssh_user: String::new(),
        ssh_key: None,
        ssh_password: None,
    };
    let req = DeployRequest {
        server_id: "s1".into(),
        mgmt_port: pick_free_port(),
        grpc_port: pick_free_port(),
        election_profile: Some("test".into()),
        binary: Some(bin),
    };
    let deployed = lifecycle::deploy_local(&req, &node).await.expect("deploy_local");
    Some((deployed.pid, deployed.mgmt_url))
}

async fn store_grpc_endpoint(mgmt_url: &str, store_id: u64) -> String {
    let mgmt = ServerClient::new(mgmt_url.to_string()).unwrap();
    let detail = mgmt.get_store(store_id).await.expect("get_store");
    let listen = detail.listen_addr.expect("listen_addr");
    let port = listen.rsplit(':').next().unwrap();
    format!("127.0.0.1:{port}")
}

#[tokio::test]
async fn put_get_delete_cycle() {
    let Some((pid, mgmt_url)) = spawn_server().await else {
        eprintln!("skipping: crowkv-server binary not built");
        return;
    };

    let mgmt = ServerClient::new(mgmt_url.clone()).unwrap();
    let store_id = 1;
    let group_id = 1;
    let replica_id = 1;
    mgmt.add_store(&AddStoreRequest { store_id, port: None })
        .await
        .expect("add_store");
    mgmt.add_group(
        store_id,
        &AddGroupRequest {
            group_id,
            replica_id,
            initial_role: None,
            start_election: None,
        },
    )
    .await
    .expect("add_group");
    lifecycle::wait_for_leader(&mgmt_url, store_id, group_id, Duration::from_secs(5))
        .await
        .expect("wait_for_leader");

    let endpoint = store_grpc_endpoint(&mgmt_url, store_id).await;
    let mut kv = KvClient::connect(endpoint).await.expect("connect");

    // Use the group created explicitly via the management API.

    // Put.
    let out = kv.put(group_id, b"hello", b"world", 0, 0).await.expect("put");
    assert_ne!(out.request_id, 0);

    // Get → Found.
    let got = kv.get(group_id, b"hello").await.expect("get");
    match got {
        GetOutcome::Found { value, .. } => assert_eq!(value, b"world"),
        GetOutcome::NotFound => panic!("expected Found"),
    }

    // Delete.
    let _ = kv.delete(group_id, b"hello", 0, 0).await.expect("delete");

    // Get → NotFound.
    let got = kv.get(group_id, b"hello").await.expect("get after delete");
    assert!(matches!(got, GetOutcome::NotFound));

    // Scan now goes through the real RPC. Seed three keys, scan with
    // a prefix, and assert sorted output + correct truncation.
    let _ = kv
        .put(group_id, b"alpha/1", b"a1", 0, 0)
        .await
        .expect("seed alpha/1");
    let _ = kv
        .put(group_id, b"alpha/2", b"a2", 0, 0)
        .await
        .expect("seed alpha/2");
    let _ = kv
        .put(group_id, b"beta/1", b"b1", 0, 0)
        .await
        .expect("seed beta/1");

    let out = kv.scan(group_id, b"alpha/", 0).await.expect("scan");
    assert!(!out.truncated);
    assert_eq!(out.items.len(), 2);
    assert_eq!(out.items[0].0, b"alpha/1");
    assert_eq!(out.items[0].1, b"a1");
    assert_eq!(out.items[1].0, b"alpha/2");

    // Truncation: limit < matching count.
    let out = kv.scan(group_id, b"alpha/", 1).await.expect("scan limit=1");
    assert!(out.truncated);
    assert_eq!(out.items.len(), 1);
    assert_eq!(out.items[0].0, b"alpha/1");

    // Empty prefix returns everything; limit=0 means "no limit".
    let out = kv.scan(group_id, b"", 0).await.expect("scan all");
    assert!(out.items.iter().any(|(k, _)| k == b"beta/1"));
    assert!(!out.truncated);

    // Unknown group surfaces ok=false → Err with "group ... not found".
    let err = kv.scan(9999, b"", 0).await.expect_err("scan missing group");
    assert!(format!("{err}").contains("not found"), "got: {err}");

    // A second connect to the same endpoint must succeed quickly even
    // after the upstream is briefly idle — this exercises the
    // process-wide channel cache. We don't measure timing (CI is
    // flaky); we only assert the call still works and a put + get on
    // the cached client round-trips.
    let endpoint = store_grpc_endpoint(&mgmt_url, store_id).await;
    let mut kv2 = KvClient::connect(&endpoint).await.expect("reconnect (cached)");
    let _ = kv2
        .put(group_id, b"cached", b"hit", 0, 0)
        .await
        .expect("put on cached client");
    match kv2.get(group_id, b"cached").await.expect("get on cached client") {
        GetOutcome::Found { value, .. } => assert_eq!(value, b"hit"),
        GetOutcome::NotFound => panic!("expected Found via cached channel"),
    }

    // Cleanup.
    let _ = lifecycle::stop_pid(pid);
    tokio::time::sleep(Duration::from_millis(50)).await;
    // Drop the cached channel so subsequent runs of the same test
    // process don't reuse a dead connection.
    KvClient::invalidate_cache(&endpoint);
}
