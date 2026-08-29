// Copyright 2026-present Gian <crow.db@outlook.com>

use std::process::ExitCode;
use std::time::Duration;

use crate::bench::handle::ClusterHandle;
use crowdb_console_shared::lifecycle::{process_is_alive, stop_pid_with_timeout};

/// `bench teardown` — stop a deployed cluster and remove its handle.
/// Idempotent: if the handle is already missing, exits 0.
pub(crate) async fn bench_teardown(args: super::TeardownArgs, json: bool) -> ExitCode {
    let handle = match ClusterHandle::load(&args.target) {
        Ok(h) => h,
        Err(e) => {
            // Idempotent: if the handle is already gone, treat as success.
            let msg = format!("{e}");
            if msg.contains("not found") {
                if json {
                    return crate::utils::print_json(&serde_json::json!({
                        "target": args.target,
                        "already_torn_down": true,
                    }));
                }
                println!("cluster '{}' already torn down", args.target);
                return ExitCode::SUCCESS;
            }
            eprintln!("error: {e}");
            return ExitCode::from(2);
        }
    };

    let mut stopped = 0u32;
    let mut already_dead = 0u32;

    // Stop node server pids.
    for &pid in &handle.node_pids {
        let was_alive = process_is_alive(pid);
        let _ = stop_pid_with_timeout(pid, Duration::from_secs(5));
        if was_alive {
            stopped += 1;
        } else {
            already_dead += 1;
        }
    }

    // Stop console-web pid if present (--web deploy).
    if let Some(console_pid) = handle.console_pid {
        let was_alive = process_is_alive(console_pid);
        let _ = stop_pid_with_timeout(console_pid, Duration::from_secs(5));
        if was_alive {
            stopped += 1;
        } else {
            already_dead += 1;
        }
    }

    // Remove the handle file (mark as torn down). Leave the rest of
    // runtime/<name>/ (logs, reports) for post-mortem.
    let _ = ClusterHandle::remove_handle_file(&args.target);

    if json {
        return crate::utils::print_json(&serde_json::json!({
            "target": args.target,
            "stopped": stopped,
            "already_dead": already_dead,
        }));
    }
    println!(
        "torn down cluster '{}' (stopped {stopped}, already dead {already_dead})",
        args.target
    );
    ExitCode::SUCCESS
}
