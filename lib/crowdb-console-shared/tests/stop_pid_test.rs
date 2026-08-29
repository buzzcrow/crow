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
    let mut child = Command::new("sh")
        .arg("-c")
        .arg("trap '' TERM; while true; do sleep 1; done")
        .spawn()
        .expect("failed to spawn shell");
    let pid = child.id();

    assert!(
        process_is_alive(pid),
        "child process should be alive before stop_pid"
    );

    // Give the shell time to execute `trap '' TERM` before we send SIGTERM.
    // Without this, a SIGTERM arriving before the trap is installed kills
    // the shell instantly, causing the timing assertion below to fail.
    std::thread::sleep(Duration::from_millis(200));

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
}
