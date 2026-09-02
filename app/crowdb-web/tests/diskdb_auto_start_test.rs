// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version.0.

//! Verifies that a persisted `DiskDB` entry with `auto_start: true`
//! is respawned by `startup_topology_check` after a console restart.
//! Regression: `restore_persisted_topology` only called
//! `ensure_server_running` (KV-only deploy path) for all servers,
//! so `DiskDB` entries were silently skipped on startup.

use std::time::Duration;

use crowdb_console_shared::config::{ConsoleConfig, NodeEntry, RackEntry, ServerEntry, ServiceType};
use crowdb_console_shared::lifecycle::{crowdb_diskdb_bin, stop_pid_with_timeout};
use crowdb_protocol::port_alloc;
use crowdb_protocol::ServicePort;
use crowdb_web::mgmt::startup_topology_check;
use crowdb_web::AppState;

struct PidGuard {
    pids: Vec<u32>,
}

impl Drop for PidGuard {
    fn drop(&mut self) {
        for pid in &self.pids {
            let _ = stop_pid_with_timeout(*pid, Duration::from_secs(5));
        }
    }
}

/// Seed a config with one node + one `DiskDB` server entry (`auto_start`),
/// no KV server. `startup_topology_check` should detect `Missing`
/// (no reachable group-0) and call `restore_persisted_topology`, which
/// should spawn the diskdb process via `ensure_diskdb_running`.
#[tokio::test]
async fn diskdb_auto_starts_on_console_restart() {
    // Skip if crowdb-diskdb binary is not available.
    if crowdb_diskdb_bin().is_none() {
        eprintln!("skipping: crowdb-diskdb binary not found (set CROWDB_DISKDB_BIN)");
        return;
    }

    let dir = crowdb_test_harness::test_dirs::test_data_dir().join(format!(
        "crowdb-web-ddb-autostart-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();

    let node_id: u64 = 7777;
    // Allocate independent ports for the diskdb's listen, http, and
    // rpc listeners. The auto-start path derives listen/http from
    // rpc_port, so we only store rpc_port + http in the config.
    let _listen_port = port_alloc::alloc_test_port(ServicePort::DiskdbListen);
    let http_port = port_alloc::alloc_test_port(ServicePort::DiskdbHttp);
    let rpc_port = port_alloc::alloc_test_port(ServicePort::DiskdbRpc);

    let mut cfg = ConsoleConfig::default();
    cfg.add_rack(RackEntry {
        id: 1,
        name: "test-rack".into(),
    })
    .unwrap();
    cfg.add_node(NodeEntry {
        id: node_id,
        rack_id: 1,
        host: "127.0.0.1".into(),
        ssh_port: 22,
        ssh_user: String::new(),
        ssh_key: None,
        ssh_password: None,
    })
    .unwrap();

    let rpc_url = format!("http://127.0.0.1:{rpc_port}");
    cfg.add_server(ServerEntry {
        id: format!("diskdb-{node_id}"),
        url: format!("http://127.0.0.1:{http_port}"),
        node_id: Some(node_id),
        rpc_url: Some(rpc_url),
        rest_port: None,
        rpc_port: Some(rpc_port),
        auto_start: true,
        binary: None,
        election_profile: None,
        pid: None,
        service_type: ServiceType::Diskdb,
        rpc_workers: None,
        no_fsync: false,
    })
    .unwrap();

    let cfg_path = dir.join("console.toml");
    let state = AppState::with_config(cfg, Some(cfg_path));

    // Run the startup restore — should spawn the diskdb process.
    startup_topology_check(&state).await;

    // The diskdb PID should now be tracked.
    let pid = state
        .diskdb_runtime_pid(node_id)
        .expect("diskdb was not auto-started by startup_topology_check");

    let _guard = PidGuard { pids: vec![pid] };

    // Verify the process is actually alive.
    assert!(
        crowdb_console_shared::lifecycle::process_is_alive(pid),
        "diskdb pid {pid} is not alive after auto-start"
    );

    // Clean up the workspace dir.
    let _ = std::fs::remove_dir_all(&dir);
}
