//! C3 end-to-end: rack → node → deploy local `crowkv-server` → observe
//! the running instance via `topology::aggregate()`.
//!
//! The test expects the `crowkv-server` binary to be built and available
//! either via `$CROWKV_SERVER_BIN` or as a sibling of the current test
//! executable (the usual `cargo test` layout). If neither resolves, the
//! test is skipped with an `eprintln!` note instead of failing, so this
//! suite stays friendly on first run.

use std::time::Duration;

use crowkv_console_shared::{
    config::{NodeEntry, RackEntry},
    lifecycle::{self, crowkv_server_bin, DeployRequest},
    topology, ConsoleConfig, ServerEntry,
};

fn pick_free_port() -> u16 {
    let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    port
}

#[tokio::test]
async fn deploy_local_and_observe_topology() {
    let Some(bin) = crowkv_server_bin() else {
        eprintln!("skipping: crowkv-server binary not found (build it with `cargo build -p crowkv-server` or set $CROWKV_SERVER_BIN)");
        return;
    };
    if !bin.exists() {
        eprintln!("skipping: crowkv-server binary at {} does not exist", bin.display());
        return;
    }

    // Build a fresh in-memory config: 1 rack, 1 node.
    let mut cfg = ConsoleConfig::default();
    cfg.add_rack(RackEntry {
        id: "r1".into(),
        name: "rack-1".into(),
    })
    .unwrap();
    cfg.add_node(NodeEntry {
        id: "n1".into(),
        rack_id: "r1".into(),
        host: "127.0.0.1".into(),
        ssh_port: 22,
        ssh_user: String::new(),
        ssh_key: None,
        ssh_password: None,
    })
    .unwrap();

    let node = cfg.node("n1").unwrap().clone();
    let mgmt_port = pick_free_port();
    let grpc_port = pick_free_port();

    let req = DeployRequest {
        server_id: "s1".into(),
        mgmt_port,
        grpc_port,
        binary: Some(bin),
    };

    let deployed = match lifecycle::deploy_local(&req, &node).await {
        Ok(d) => d,
        Err(e) => {
            panic!("deploy_local failed: {e}");
        }
    };

    // Record into the registry as the CLI would.
    cfg.add_server(ServerEntry {
        id: deployed.server_id.clone(),
        url: deployed.mgmt_url.clone(),
        node_id: Some("n1".into()),
        grpc_url: Some(deployed.grpc_url.clone()),
        pid: Some(deployed.pid),
    })
    .unwrap();

    // Aggregate via the same path the CLI uses.
    let snapshot = topology::aggregate(&cfg.server_urls()).await.unwrap();
    let ok = snapshot.servers.iter().any(|s| s.error.is_none() && s.health.is_some());
    assert!(ok, "deployed server should appear healthy in the aggregate snapshot: {snapshot:#?}");

    // Clean up: stop the process we spawned so the test doesn't leak.
    let _ = lifecycle::stop_pid(deployed.pid);
    // Give the OS a moment to release the ports before the test ends.
    tokio::time::sleep(Duration::from_millis(50)).await;
}
