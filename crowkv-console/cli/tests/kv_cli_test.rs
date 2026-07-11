//! A12 CLI e2e: invoke the compiled `crowkv` binary through a live
//! `crowkv-web` (`--console`) which itself proxies to a real
//! `crowkv-server`. Exercises the `kv put / get / delete / scan` verbs
//! end-to-end and verifies the legacy `--server` flag is no longer
//! required for the four KV verbs.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::Command;
use std::time::Duration;

use crowkv_console_shared::clients::http::ServerClient;
use crowkv_console_shared::cluster::NodeHealth;
use crowkv_console_shared::config::{NodeEntry, RackEntry, ServerEntry};
use crowkv_console_shared::lifecycle::{self, crowkv_server_bin, DeployRequest};
use crowkv_console_shared::monitor::{legacy_topology_to_node_stores, NodeRecord};
use crowkv_console_shared::ConsoleConfig;
use crowkv_web::{router, AppState};

fn pick_free_port() -> u16 {
    let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    port
}

fn crowkv_cli_bin() -> PathBuf {
    if let Some(path) = option_env!("CARGO_BIN_EXE_crowkv") {
        return PathBuf::from(path);
    }
    let mut p = std::env::current_exe().expect("current_exe");
    while p.file_name().is_some_and(|n| n != "debug" && n != "release") {
        p.pop();
    }
    p.push("crowkv");
    p
}

struct Upstream {
    pid: u32,
    mgmt_url: String,
    grpc_url: String,
}

async fn spawn_upstream() -> Option<Upstream> {
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
        server_id: "n1".into(),
        mgmt_port: pick_free_port(),
        grpc_port: pick_free_port(),
        election_profile: Some("test".into()),
        binary: Some(bin),
    };
    let deployed = lifecycle::deploy_local(&req, &node).await.expect("deploy_local");
    Some(Upstream {
        pid: deployed.pid,
        mgmt_url: deployed.mgmt_url,
        grpc_url: deployed.grpc_url,
    })
}

async fn spawn_console(upstream: &Upstream) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    let mut cfg = ConsoleConfig::default();
    cfg.racks.push(RackEntry {
        id: "r1".into(),
        name: "r1".into(),
    });
    cfg.nodes.push(NodeEntry {
        id: "n1".into(),
        rack_id: "r1".into(),
        host: "127.0.0.1".into(),
        ssh_port: 22,
        ssh_user: String::new(),
        ssh_key: None,
        ssh_password: None,
    });
    cfg.add_server(ServerEntry {
        id: "n1".into(),
        url: upstream.mgmt_url.clone(),
        node_id: Some("n1".into()),
        grpc_url: Some(upstream.grpc_url.clone()),
        pid: Some(upstream.pid),
    })
    .unwrap();
    let state = AppState::with_config(cfg, None);

    // Seed the monitor cache so leader resolution works on the first
    // KV call (the CLI test does no GET-stores beforehand).
    let client = ServerClient::new(upstream.mgmt_url.clone()).unwrap();
    if let Ok(stores) = client.topology().await {
        let rec = NodeRecord {
            health: NodeHealth::Up,
            last_seen_ms: 1,
            stores: legacy_topology_to_node_stores("n1", &stores),
            last_error: None,
        };
        state.monitor_cache.set_node_report("n1".to_string(), rec).await;
    }

    tokio::spawn(async move {
        axum::serve(listener, router(state)).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    addr
}

fn run(cli: &PathBuf, console_url: &str, args: &[&str]) -> (i32, String, String) {
    let out = Command::new(cli)
        .arg("--console")
        .arg(console_url)
        .args(args)
        .output()
        .expect("spawn cli");
    (
        out.status.code().unwrap_or(-1),
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn kv_put_get_delete_round_trip() {
    let Some(upstream) = spawn_upstream().await else {
        eprintln!("skipping: crowkv-server binary not built");
        return;
    };
    let cli = crowkv_cli_bin();
    if !cli.exists() {
        eprintln!("skipping: crowkv CLI binary not built ({})", cli.display());
        let _ = lifecycle::stop_pid(upstream.pid);
        return;
    }

    let console = spawn_console(&upstream).await;
    let console_url = format!("http://{console}");

    // put
    let (code, stdout, stderr) = run(
        &cli,
        &console_url,
        &[
            "kv",
            "put",
            "--store-id",
            "1",
            "--group-id",
            "1",
            "--key",
            "color",
            "--value",
            "indigo",
        ],
    );
    assert_eq!(code, 0, "stdout={stdout}\nstderr={stderr}");
    assert!(stdout.contains("ok:"));

    // get
    let (code, stdout, _) = run(
        &cli,
        &console_url,
        &[
            "kv",
            "get",
            "--store-id",
            "1",
            "--group-id",
            "1",
            "--key",
            "color",
        ],
    );
    assert_eq!(code, 0);
    assert!(stdout.contains("indigo"), "stdout={stdout}");

    // delete
    let (code, _, stderr) = run(
        &cli,
        &console_url,
        &[
            "kv",
            "delete",
            "--store-id",
            "1",
            "--group-id",
            "1",
            "--key",
            "color",
        ],
    );
    assert_eq!(code, 0, "stderr={stderr}");

    // get → not found returns exit code 3
    let (code, stdout, _) = run(
        &cli,
        &console_url,
        &[
            "kv",
            "get",
            "--store-id",
            "1",
            "--group-id",
            "1",
            "--key",
            "color",
        ],
    );
    assert_eq!(code, 3);
    assert!(stdout.contains("not found"));

    // scan/list now returns real key/value rows. Seed two keys, then
    // scan with a prefix that captures only one of them.
    let _ = run(
        &cli,
        &console_url,
        &[
            "kv",
            "put",
            "--store-id",
            "1",
            "--group-id",
            "1",
            "--key",
            "scan/a",
            "--value",
            "1",
        ],
    );
    let _ = run(
        &cli,
        &console_url,
        &[
            "kv",
            "put",
            "--store-id",
            "1",
            "--group-id",
            "1",
            "--key",
            "scan/b",
            "--value",
            "2",
        ],
    );
    let (code, stdout, stderr) = run(
        &cli,
        &console_url,
        &[
            "kv",
            "list",
            "--store-id",
            "1",
            "--group-id",
            "1",
            "--prefix",
            "scan/",
        ],
    );
    assert_eq!(code, 0, "stderr={stderr}");
    assert!(stdout.contains("scan/a\t1"), "stdout={stdout}");
    assert!(stdout.contains("scan/b\t2"), "stdout={stdout}");

    // Cleanup.
    let _ = lifecycle::stop_pid(upstream.pid);
    tokio::time::sleep(Duration::from_millis(50)).await;
}
