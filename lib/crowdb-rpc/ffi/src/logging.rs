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
