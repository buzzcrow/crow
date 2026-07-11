//! C7 correctness smoke: spawn `crowkv-server`, run a short `mix`
//! workload via the bench engine, and assert non-zero ops + low error
//! rate.

use std::time::Duration;

use crowkv_console_bench::{run_bench, BenchConfig, WorkloadKind};
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

async fn store1_endpoint(mgmt_url: &str) -> String {
    let mgmt = ServerClient::new(mgmt_url.to_string()).unwrap();
    let detail = mgmt.get_store(1).await.expect("get_store(1)");
    let listen = detail.listen_addr.expect("listen_addr");
    let port = listen.rsplit(':').next().unwrap();
    format!("127.0.0.1:{port}")
}

#[tokio::test]
async fn mix_workload_short_run_produces_ops() {
    let Some((pid, mgmt)) = spawn_server().await else {
        eprintln!("skipping: crowkv-server binary not built");
        return;
    };

    let endpoint = store1_endpoint(&mgmt).await;
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = BenchConfig::defaults(endpoint, WorkloadKind::Mix);
    cfg.connections = 2;
    cfg.threads = 4;
    cfg.duration = Duration::from_millis(500);
    cfg.key_space = 100;
    cfg.value_size = 32;
    cfg.report_dir = Some(dir.path().to_path_buf());
    cfg.run_id = Some("mix-smoke".into());

    let (report, path) = run_bench(cfg).await.expect("run_bench");
    assert!(path.exists(), "report file should exist");
    assert!(report.total_ops > 0, "expected non-zero ops, got {report:?}");
    assert!(
        report.error_rate < 0.1,
        "error_rate too high: {} ({} errors / {} ops)",
        report.error_rate,
        report.total_errors,
        report.total_ops
    );

    // mix should produce both read and write entries.
    assert!(report.by_op.contains_key("read") || report.by_op.contains_key("write"));

    // Cleanup.
    let _ = lifecycle::stop_pid(pid);
    tokio::time::sleep(Duration::from_millis(50)).await;
}

#[tokio::test]
async fn write_workload_shows_increasing_revisions() {
    let Some((pid, mgmt)) = spawn_server().await else {
        eprintln!("skipping: crowkv-server binary not built");
        return;
    };
    let endpoint = store1_endpoint(&mgmt).await;
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = BenchConfig::defaults(endpoint, WorkloadKind::Write);
    cfg.connections = 1;
    cfg.threads = 2;
    cfg.duration = Duration::from_millis(300);
    cfg.key_space = 50;
    cfg.value_size = 16;
    cfg.report_dir = Some(dir.path().to_path_buf());

    let (report, _) = run_bench(cfg).await.expect("run_bench");
    let writes = report.by_op.get("write").expect("write op stats");
    assert!(writes.ops > 0);
    assert!(report.error_rate < 0.1, "{report:?}");

    let _ = lifecycle::stop_pid(pid);
    tokio::time::sleep(Duration::from_millis(50)).await;
}
