// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_console_shared::lifecycle::{process_is_alive, stop_pid, stop_pid_with_timeout};
use std::process::Command;
use std::time::Duration;

/// `stop_pid` on a non-existent pid should return `Ok(false)` without
/// hanging or erroring.
#[test]
fn stop_pid_nonexistent_returns_false() {
    let result = stop_pid(999_999).unwrap();
    assert!(!result, "stop_pid on non-existent pid should return false");
}

/// `stop_pid` on a live process should send SIGTERM, wait for the
/// process to exit, and return `Ok(true)`.
#[test]
fn stop_pid_terminates_live_process() {
    let mut child = Command::new("sleep")
        .arg("30")
        .spawn()
        .expect("failed to spawn sleep");
    let pid = child.id();

    assert!(
        process_is_alive(pid),
        "child process should be alive before stop_pid"
    );

    let result = stop_pid(pid).unwrap();
    assert!(
        result,
        "stop_pid should return true for a successfully stopped process"
    );

    assert!(
        !process_is_alive(pid),
        "child process should be gone after stop_pid"
    );

    let _ = child.wait();
}

/// `stop_pid` on a process that ignores SIGTERM should force-kill it
/// after the timeout and return `Ok(false)`.
#[test]
fn stop_pid_force_kills_unresponsive_process() {
    // The shell writes a sentinel file once `trap '' TERM` is installed,
    // so we can wait deterministically for the trap to be in place before
    // sending SIGTERM. A fixed sleep is racy under CI load — if SIGTERM
    // arrives before the trap, the shell dies instantly (~1ms) and the
    // timing assertion below fails.
    let sentinel = std::env::temp_dir().join(format!("stop_pid_test_{}.ready", std::process::id()));
    let _ = std::fs::remove_file(&sentinel);
    let sentinel_str = sentinel.to_string_lossy();
    let mut child = Command::new("sh")
        .arg("-c")
        .arg(format!(
            "trap '' TERM; touch {sentinel_str}; while true; do sleep 1; done"
        ))
        .spawn()
        .expect("failed to spawn shell");
    let pid = child.id();

    assert!(
        process_is_alive(pid),
        "child process should be alive before stop_pid"
    );

    // Wait for the shell to install the trap (sentinel file appears).
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !sentinel.exists() {
        if std::time::Instant::now() > deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("shell did not install trap within 5s");
        }
        std::thread::sleep(Duration::from_millis(10));
    }

    let start = std::time::Instant::now();
    let result = stop_pid_with_timeout(pid, Duration::from_secs(2)).unwrap();
    let elapsed = start.elapsed();

    assert!(
        elapsed >= Duration::from_secs(1),
        "stop_pid should have waited ~2s before force-killing, took {elapsed:?}"
    );
    assert!(!result, "stop_pid should return false when force-killing");

    assert!(
        !process_is_alive(pid),
        "child process should be gone after force-kill"
    );

    let _ = child.wait();
    let _ = std::fs::remove_file(&sentinel);
}
