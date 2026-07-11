//! CLI e2e smoke for `bench run`: resolve the gRPC endpoint via the
//! console, drive a 1 s write workload, and assert a report is written.

mod testkit;

use std::time::Duration;

use crowkv_console_shared::lifecycle;
use testkit::console::{crowkv_cli_bin, run, spawn_console, spawn_upstream, wait_for_leader};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bench_run_write_smoke() {
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

    // Create store 1 / group 1 and wait for a leader before benching.
    let (code, _, stderr) = run(&cli, &console_url, &["store", "add", "--store-id", "1"]);
    assert_eq!(code, 0, "store add stderr={stderr}");
    let (code, _, stderr) = run(
        &cli,
        &console_url,
        &[
            "paxos",
            "add",
            "--store-id",
            "1",
            "--group-id",
            "1",
            "--replica-id",
            "1",
            "--nodes",
            "n1",
        ],
    );
    assert_eq!(code, 0, "paxos add stderr={stderr}");
    assert!(
        wait_for_leader(&console_url, 1, 1, Duration::from_secs(15)).await,
        "group 1 never elected a leader",
    );

    let (code, stdout, stderr) = run(
        &cli,
        &console_url,
        &[
            "bench",
            "run",
            "write",
            "--store-id",
            "1",
            "--group-id",
            "1",
            "--duration-secs",
            "1",
            "--threads",
            "2",
            "--connections",
            "2",
        ],
    );
    assert_eq!(code, 0, "bench run stdout={stdout}\nstderr={stderr}");
    assert!(stdout.contains("report:"), "stdout={stdout}");

    let _ = lifecycle::stop_pid(upstream.pid);
    tokio::time::sleep(Duration::from_millis(50)).await;
}
