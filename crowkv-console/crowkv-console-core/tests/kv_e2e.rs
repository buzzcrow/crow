//! C6 end-to-end: spawn `crowkv-server`, then via the gRPC `KvClient`
//! exercise put → get → delete → get-not-found. The default server
//! bootstraps with `--stores 1 --groups 1 --replica 1`, so we use that
//! pre-existing store/group instead of creating a new one (the
//! pre-existing store is the leader for its group).

use std::time::Duration;

use crowkv_console_core::clients::grpc::{GetOutcome, KvClient};
use crowkv_console_core::clients::http::ServerClient;
use crowkv_console_core::config::NodeEntry;
use crowkv_console_core::lifecycle::{self, crowkv_server_bin, DeployRequest};

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
        binary: Some(bin),
    };
    let deployed = lifecycle::deploy_local(&req, &node).await.expect("deploy_local");
    Some((deployed.pid, deployed.mgmt_url))
}

async fn store1_grpc_endpoint(mgmt_url: &str) -> String {
    let mgmt = ServerClient::new(mgmt_url.to_string()).unwrap();
    let detail = mgmt.get_store(1).await.expect("get_store(1)");
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

    let endpoint = store1_grpc_endpoint(&mgmt_url).await;
    let mut kv = KvClient::connect(endpoint).await.expect("connect");

    // store 1 / group 1 / replica 1 is the bootstrap leader.
    let group_id = 1;

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

    // Scan still surfaces the "not implemented" error.
    let err = kv.scan(group_id, b"", 10).await.expect_err("scan");
    assert!(format!("{err}").contains("not yet implement"));

    // Cleanup.
    let _ = lifecycle::stop_pid(pid);
    tokio::time::sleep(Duration::from_millis(50)).await;
}
