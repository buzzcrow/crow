// Copyright 2026-present Gian <crow.db@outlook.com>

use std::process::ExitCode;

use crate::bench::handle::{ClusterHandle, DeployKind, HandleTunables};
use crate::bench::target::kv::{BenchFixture, BenchMode, GROUP_ID, STORE_ID};

/// `bench deploy` — provision a named cluster and leave it running.
pub(crate) async fn bench_deploy(args: super::DeployArgs, json: bool) -> ExitCode {
    let Some(kind) = DeployKind::parse(&args.kind) else {
        eprintln!(
            "error: unknown --kind {:?} (expected: kv|rpc|chunk|storage)",
            args.kind
        );
        return ExitCode::from(2);
    };
    match kind {
        DeployKind::Kv => deploy_kv(&args, json).await,
        DeployKind::Rpc => deploy_rpc(&args, json),
        DeployKind::Chunk | DeployKind::Storage => {
            eprintln!("error: --kind {:?} not yet implemented", kind.label());
            ExitCode::from(2)
        }
    }
}

/// Deploy a 3-node KV cluster via an embedded console-web, then detach
/// the fixture so the `crowdb-kv-server` processes survive CLI exit.
async fn deploy_kv(args: &super::DeployArgs, json: bool) -> ExitCode {
    let Some(mode) = BenchMode::parse(&args.mode) else {
        eprintln!("error: unknown mode {:?} (expected: mem|file|block)", args.mode);
        return ExitCode::from(2);
    };

    let runtime_dir = ClusterHandle::runtime_dir(&args.name);
    let handle_path = runtime_dir.join("handle.json");
    if handle_path.exists() {
        eprintln!("error: deploy '{}' already exists; teardown first", args.name);
        return ExitCode::from(2);
    }

    let workspace_dir = runtime_dir.join("workspace");
    println!(
        "deploying cluster '{}' (kind=kv, mode={})...",
        args.name,
        mode.label()
    );
    let _ = std::io::Write::flush(&mut std::io::stdout());

    let mut fixture = match BenchFixture::new(
        mode,
        workspace_dir.clone(),
        args.max_inflight,
        args.metrics_interval,
        args.node_config.clone(),
        args.coalesce_max_keys,
        args.coalesce_drain_threshold,
        args.peer_pool_size,
        args.enable_nagle,
        args.quickack,
        args.event_write,
        args.send_queue_capacity,
    )
    .await
    {
        Ok(f) => f,
        Err(e) => {
            eprintln!("error: deploy failed: {e}");
            // Best-effort cleanup of the runtime dir.
            let _ = std::fs::remove_dir_all(&runtime_dir);
            return ExitCode::from(2);
        }
    };

    let handle = ClusterHandle {
        name: args.name.clone(),
        kind: DeployKind::Kv,
        store_id: STORE_ID,
        group_id: GROUP_ID,
        leader_endpoint: fixture.leader_endpoint().to_string(),
        node_ids: fixture.node_ids().to_vec(),
        node_pids: fixture.node_pids().to_vec(),
        node_rpc_urls: fixture.node_rpc_urls().to_vec(),
        node_mgmt_urls: fixture.node_mgmt_urls().to_vec(),
        workspace_dir: workspace_dir.clone(),
        mode: mode.label().to_string(),
        tunables: HandleTunables {
            max_inflight: args.max_inflight,
            metrics_interval: args.metrics_interval,
            peer_pool_size: args.peer_pool_size,
            enable_nagle: args.enable_nagle,
            quickack: args.quickack,
            event_write: args.event_write,
            send_queue_capacity: args.send_queue_capacity,
        },
        console_url: None,
        console_pid: None,
        created_at: chrono::Utc::now(),
    };

    // Detach the fixture: abort the embedded console-web task but leave
    // the deployed servers running. Their pids are in the handle.
    fixture.detach();

    match handle.save() {
        Ok(path) => {
            if json {
                return crate::utils::print_json(&serde_json::json!({
                    "name": handle.name,
                    "kind": handle.kind.label(),
                    "mode": handle.mode,
                    "leader_endpoint": handle.leader_endpoint,
                    "node_count": handle.node_ids.len(),
                    "handle_path": path.to_string_lossy(),
                }));
            }
            println!(
                "deployed cluster '{}' (kind=kv, mode={}, {} nodes, leader={})",
                handle.name,
                handle.mode,
                handle.node_ids.len(),
                handle.leader_endpoint
            );
            println!("handle: {}", path.display());
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("error: failed to save handle: {e}");
            ExitCode::from(2)
        }
    }
}

/// Deploy an RPC fb server as a detached child process.
fn deploy_rpc(args: &super::DeployArgs, json: bool) -> ExitCode {
    use std::process::{Command, Stdio};

    let runtime_dir = ClusterHandle::runtime_dir(&args.name);
    let handle_path = runtime_dir.join("handle.json");
    if handle_path.exists() {
        eprintln!("error: deploy '{}' already exists; teardown first", args.name);
        return ExitCode::from(2);
    }

    let Some(fb_bin) = locate_fb_server_bin() else {
        eprintln!("error: crowdb-rpc-fb-server binary not found (run `pixi run build-cpp` or set $CROWDB_RPC_FB_SERVER_BIN)");
        return ExitCode::from(2);
    };
    if !fb_bin.exists() {
        eprintln!(
            "error: crowdb-rpc-fb-server binary not found at {}",
            fb_bin.display()
        );
        return ExitCode::from(2);
    }

    let port = crowdb_console_shared::test_ports::unique_test_port();
    let log_dir = runtime_dir.join("log");
    let _ = std::fs::create_dir_all(&log_dir);

    println!("deploying cluster '{}' (kind=rpc, port={})...", args.name, port);
    let _ = std::io::Write::flush(&mut std::io::stdout());

    let mut cmd = Command::new(&fb_bin);
    cmd.arg(format!("--port={port}"))
        .arg("--io_workers=2")
        .arg(format!("--logdir={}", log_dir.display()))
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    // Detach: new process group so the child survives CLI exit.
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        cmd.process_group(0);
    }

    let child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("error: failed to spawn crowdb-rpc-fb-server: {e}");
            return ExitCode::from(2);
        }
    };
    let pid = child.id();
    // Detach: drop the handle without killing.
    let _ = child;

    // Wait for the fb server to bind.
    let mut ready = false;
    for _ in 0..50 {
        if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
            ready = true;
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    if !ready {
        eprintln!("error: crowdb-rpc-fb-server did not bind on port {port}");
        let _ = std::fs::remove_dir_all(&runtime_dir);
        return ExitCode::from(2);
    }

    let handle = build_rpc_handle(args, &runtime_dir, port, pid);

    match handle.save() {
        Ok(path) => {
            if json {
                return crate::utils::print_json(&serde_json::json!({
                    "name": handle.name,
                    "kind": handle.kind.label(),
                    "port": port,
                    "pid": pid,
                    "handle_path": path.to_string_lossy(),
                }));
            }
            println!(
                "deployed cluster '{}' (kind=rpc, port={port}, pid={pid})",
                handle.name
            );
            println!("handle: {}", path.display());
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("error: failed to save handle: {e}");
            ExitCode::from(2)
        }
    }
}

/// Build the `ClusterHandle` for a deployed RPC fb server.
fn build_rpc_handle(
    args: &super::DeployArgs,
    runtime_dir: &std::path::Path,
    port: u16,
    pid: u32,
) -> ClusterHandle {
    ClusterHandle {
        name: args.name.clone(),
        kind: DeployKind::Rpc,
        store_id: 0,
        group_id: 0,
        leader_endpoint: format!("http://127.0.0.1:{port}"),
        node_ids: vec![0],
        node_pids: vec![pid],
        node_rpc_urls: vec![format!("http://127.0.0.1:{port}")],
        node_mgmt_urls: vec![],
        workspace_dir: runtime_dir.to_path_buf(),
        mode: "rpc".to_string(),
        tunables: HandleTunables {
            max_inflight: 32,
            metrics_interval: args.metrics_interval,
            peer_pool_size: 0,
            enable_nagle: args.enable_nagle,
            quickack: args.quickack,
            event_write: false,
            send_queue_capacity: 4096,
        },
        console_url: None,
        console_pid: None,
        created_at: chrono::Utc::now(),
    }
}

/// Locate the `crowdb-rpc-fb-server` binary. Mirrors the search order
/// in `tests/common/console.rs::crowdb_rpc_fb_server_bin`:
/// 1. `$CROWDB_RPC_FB_SERVER_BIN`
/// 2. `lib/crowdb-rpc/build/crowdb-rpc-fb-server` relative to the
///    workspace root (pixi `build-cpp` output).
fn locate_fb_server_bin() -> Option<std::path::PathBuf> {
    use std::path::PathBuf;
    if let Ok(p) = std::env::var("CROWDB_RPC_FB_SERVER_BIN") {
        return Some(PathBuf::from(p));
    }
    let exe = std::env::current_exe().ok()?;
    let mut dir = exe.parent()?.to_path_buf();
    for _ in 0..5 {
        let candidate = dir
            .join("lib")
            .join("crowdb-rpc")
            .join("build")
            .join("crowdb-rpc-fb-server");
        if candidate.exists() {
            return Some(candidate);
        }
        if !dir.pop() {
            break;
        }
    }
    None
}
