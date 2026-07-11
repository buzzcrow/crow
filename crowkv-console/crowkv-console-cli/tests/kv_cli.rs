//! C6 CLI e2e: invoke the compiled `crowkv` binary against a live
//! `crowkv-server`, exercising the `kv put / get / delete / scan` verbs.

use std::path::PathBuf;
use std::process::Command;
use std::time::Duration;

use crowkv_console_core::config::NodeEntry;
use crowkv_console_core::lifecycle::{self, crowkv_server_bin, DeployRequest};

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

async fn spawn_upstream() -> Option<(u32, String)> {
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

fn run(cli: &PathBuf, server_url: &str, args: &[&str]) -> (i32, String, String) {
    let out = Command::new(cli).arg("--server").arg(server_url).args(args).output().expect("spawn cli");
    (
        out.status.code().unwrap_or(-1),
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

#[tokio::test]
async fn kv_put_get_delete_round_trip() {
    let Some((pid, upstream)) = spawn_upstream().await else {
        eprintln!("skipping: crowkv-server binary not built");
        return;
    };
    let cli = crowkv_cli_bin();
    if !cli.exists() {
        eprintln!("skipping: crowkv CLI binary not built ({})", cli.display());
        let _ = lifecycle::stop_pid(pid);
        return;
    }

    // put
    let (code, stdout, stderr) = run(&cli, &upstream, &["kv", "put", "--store-id", "1", "--group-id", "1", "--key", "color", "--value", "indigo"]);
    assert_eq!(code, 0, "stdout={stdout}\nstderr={stderr}");
    assert!(stdout.contains("ok:"));

    // get
    let (code, stdout, _) = run(&cli, &upstream, &["kv", "get", "--store-id", "1", "--group-id", "1", "--key", "color"]);
    assert_eq!(code, 0);
    assert!(stdout.contains("indigo"), "stdout={stdout}");

    // delete
    let (code, _, stderr) = run(&cli, &upstream, &["kv", "delete", "--store-id", "1", "--group-id", "1", "--key", "color"]);
    assert_eq!(code, 0, "stderr={stderr}");

    // get → not found returns exit code 3
    let (code, stdout, _) = run(&cli, &upstream, &["kv", "get", "--store-id", "1", "--group-id", "1", "--key", "color"]);
    assert_eq!(code, 3);
    assert!(stdout.contains("not found"));

    // scan/list returns the placeholder error.
    let (code, _, stderr) = run(&cli, &upstream, &["kv", "list", "--store-id", "1", "--group-id", "1"]);
    assert_eq!(code, 2);
    assert!(stderr.contains("not yet implement"), "stderr={stderr}");

    // Cleanup.
    let _ = lifecycle::stop_pid(pid);
    tokio::time::sleep(Duration::from_millis(50)).await;
}
