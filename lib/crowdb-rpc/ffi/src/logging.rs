// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Safe wrappers for the crowdb-rpc C++ spdlog logger.
//!
//! Mirrors `crowdb_tree_ffi::ct_*_logging`. Call `init_logging` once at
//! process startup (before any `RpcServer::listen` / `connect`), and
//! `shutdown_logging` at process exit. No-op when the C++ build was
//! compiled without `CROWDB_HAVE_SPDLOG`.

use crate::sys;
use std::ffi::CString;
use std::sync::Once;

static TEST_LOGGING_INIT: Once = Once::new();

/// Initialize the C++ spdlog async file logger.
///
/// - `log_dir` — directory for log files (empty => stderr)
/// - `level` — spdlog level name (trace/debug/info/warn/error/off)
/// - `max_file_mb` — max file size before rotation (0 => 30)
/// - `max_files` — max rotated files to keep (0 => 5)
/// - `file_prefix` — filename prefix (empty => "crowdb-rpc")
pub fn init_logging(log_dir: &str, level: &str, max_file_mb: usize, max_files: usize, file_prefix: &str) {
    let dir_c = CString::new(log_dir).unwrap_or_default();
    let level_c = CString::new(level).unwrap_or_default();
    let prefix_c = CString::new(file_prefix).unwrap_or_default();
    unsafe {
        sys::crowdb_rpc_init_logging(
            dir_c.as_ptr(),
            level_c.as_ptr(),
            max_file_mb,
            max_files,
            prefix_c.as_ptr(),
        );
    }
}

/// Flush buffered C++ log messages to disk without stopping the logger.
pub fn flush_logging() {
    unsafe { sys::crowdb_rpc_flush_logging() };
}

/// Add a stderr sink with a per-sink level filter (e.g. "error").
/// Only messages at or above `level` go to stderr; file sinks keep their
/// original level. No-op when the C++ build has no spdlog.
pub fn add_log_stderr(level: &str) {
    let level_c = CString::new(level).unwrap_or_default();
    unsafe { sys::crowdb_rpc_add_log_stderr(level_c.as_ptr()) };
}

/// Flush and stop the C++ spdlog logger. Call at process exit.
pub fn shutdown_logging() {
    unsafe { sys::crowdb_rpc_shutdown_logging() };
}

/// Start periodic metrics flush to `log_path` + optionally stdout.
pub fn metrics_start(
    log_path: &str,
    interval_secs: f64,
    max_file_mb: usize,
    max_files: usize,
    console: bool,
) {
    let path_c = CString::new(log_path).unwrap_or_default();
    unsafe {
        sys::crowdb_rpc_metrics_start(
            path_c.as_ptr(),
            interval_secs,
            max_file_mb,
            max_files,
            if console { 1 } else { 0 },
        );
    }
}

/// Stop the metrics flush thread and do a final flush.
pub fn metrics_stop() {
    unsafe { sys::crowdb_rpc_metrics_stop() };
}

/// Initialize C++ spdlog to write to a per-process directory under
/// `<workspace_root>/test-logs/`. Idempotent (guarded by `Once`); safe
/// to call from every test. Redirects C++ transport/engine logs (e.g.
/// `socket_transport.cpp` worker teardown) to files under
/// `test-logs/crowdb-rpc-test-<pid>/` instead of stderr. Error-level
/// messages are also mirrored to stderr so they are visible in CI output
/// for debugging. No-op when the C++ build was compiled without
/// `CROWDB_HAVE_SPDLOG`.
pub fn init_test_logging() {
    TEST_LOGGING_INIT.call_once(|| {
        let dir = test_log_dir().join(format!("crowdb-rpc-test-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let dir_str = dir.to_string_lossy().into_owned();
        init_logging(&dir_str, "info", 30, 5, "test");
        add_log_stderr("error");
    });
}

/// Find the workspace root by walking up from `CARGO_MANIFEST_DIR`
/// until a `pixi.toml` marker is found. Falls back to
/// `CARGO_MANIFEST_DIR` if not found.
fn workspace_root() -> std::path::PathBuf {
    let mut dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    while !dir.join("pixi.toml").exists() {
        if !dir.pop() {
            return std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        }
    }
    dir
}

/// `<workspace_root>/test-logs/` — created if it does not exist.
fn test_log_dir() -> std::path::PathBuf {
    let dir = workspace_root().join("test-logs");
    let _ = std::fs::create_dir_all(&dir);
    dir
}
