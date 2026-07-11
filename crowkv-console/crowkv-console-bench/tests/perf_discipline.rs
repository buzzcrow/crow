//! Loose performance-discipline smoke. The bench runner should:
//!  - finish close to `cfg.duration` (no runaway accumulation),
//!  - shut down workers quickly (no hot spin after deadline).
//!
//! These thresholds are intentionally generous so CI / shared runners
//! don't go red — the goal is to catch egregious regressions like a
//! worker that ignores the deadline.

use std::time::{Duration, Instant};

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

#[tokio::test]
async fn runner_wallclock_close_to_duration() {
    let Some(bin) = crowkv_server_bin() else {
        eprintln!("skipping: crowkv-server binary not built");
        return;
    };
    if !bin.exists() {
        eprintln!("skipping: crowkv-server binary not present");
        return;
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
    let deployed = lifecycle::deploy_local(&req, &node).await.unwrap();
    let mgmt = ServerClient::new(deployed.mgmt_url.clone()).unwrap();
    let detail = mgmt.get_store(1).await.unwrap();
    let port = detail.listen_addr.unwrap().rsplit(':').next().unwrap().to_string();
    let endpoint = format!("127.0.0.1:{port}");

    let dir = tempfile::tempdir().unwrap();
    let mut cfg = BenchConfig::defaults(endpoint, WorkloadKind::Mix);
    cfg.connections = 2;
    cfg.threads = 4;
    cfg.duration = Duration::from_millis(400);
    cfg.key_space = 50;
    cfg.value_size = 32;
    cfg.report_dir = Some(dir.path().to_path_buf());

    let t0 = Instant::now();
    let _ = run_bench(cfg).await.unwrap();
    let elapsed = t0.elapsed();

    // Allow up to 2x the configured duration for shutdown / connection
    // setup. The real signal we care about is "no runaway"; tighter
    // thresholds would flap on shared CI.
    assert!(elapsed >= Duration::from_millis(350), "bench finished too fast: {elapsed:?} (expected ~400ms)");
    assert!(elapsed < Duration::from_secs(2), "bench took too long to finish: {elapsed:?} (expected <2s)");

    let _ = lifecycle::stop_pid(deployed.pid);
    tokio::time::sleep(Duration::from_millis(50)).await;
}
