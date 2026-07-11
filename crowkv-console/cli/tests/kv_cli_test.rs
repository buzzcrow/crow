//! A12 CLI e2e: invoke the compiled `crowkv` binary through a live
//! `crowkv-web` (`--console`) which itself proxies to a real
//! `crowkv-server`. Exercises the `kv put / get / delete / scan` verbs
//! end-to-end and verifies the legacy `--server` flag is no longer
//! required for the four KV verbs.

mod testkit;

use std::time::Duration;

use crowkv_console_shared::lifecycle;
use testkit::console::{crowkv_cli_bin, run, spawn_console, spawn_upstream};

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

    // Create store 1 and group 1 through the console's API (stores no longer
    // auto-create groups). This updates the monitor cache automatically.
    let http_client = reqwest::Client::new();
    let store_resp = http_client
        .post(format!("{console_url}/api/stores"))
        .json(&serde_json::json!({"store_id": 1, "nodes": ["n1"]}))
        .send()
        .await
        .expect("add_store");
    if !store_resp.status().is_success() {
        let status = store_resp.status();
        let body = store_resp.text().await.unwrap_or_default();
        panic!("add_store failed with status {status}: {body}");
    }
    assert_eq!(store_resp.status(), 201, "add_store failed");
    let group_resp = http_client
        .post(format!("{console_url}/api/stores/1/groups"))
        .json(&serde_json::json!({"group_id": 1, "replica_id": 1, "nodes": ["n1"]}))
        .send()
        .await
        .expect("add_group");
    if !group_resp.status().is_success() {
        let status = group_resp.status();
        let body = group_resp.text().await.unwrap_or_default();
        panic!("add_group failed with status {status}: {body}");
    }
    assert_eq!(group_resp.status(), 201, "add_group failed");

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
