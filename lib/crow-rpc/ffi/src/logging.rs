// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Safe wrappers for the crow-rpc C++ spdlog logger.
//!
//! Mirrors `crow_tree_ffi::ct_*_logging`. Call `init_logging` once at
//! process startup (before any `RpcServer::listen` / `connect`), and
//! `shutdown_logging` at process exit. No-op when the C++ build was
//! compiled without `CROW_HAVE_SPDLOG`.

use crate::sys;
use std::ffi::CString;

/// Initialize the C++ spdlog async file logger.
///
/// - `log_dir` — directory for log files (empty => stderr)
/// - `level` — spdlog level name (trace/debug/info/warn/error/off)
/// - `max_file_mb` — max file size before rotation (0 => 30)
/// - `max_files` — max rotated files to keep (0 => 5)
/// - `file_prefix` — filename prefix (empty => "crow-rpc")
pub fn init_logging(log_dir: &str, level: &str, max_file_mb: usize, max_files: usize, file_prefix: &str) {
    let dir_c = CString::new(log_dir).unwrap_or_default();
    let level_c = CString::new(level).unwrap_or_default();
    let prefix_c = CString::new(file_prefix).unwrap_or_default();
    unsafe {
        sys::crow_rpc_init_logging(
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
    unsafe { sys::crow_rpc_flush_logging() };
}

/// Flush and stop the C++ spdlog logger. Call at process exit.
pub fn shutdown_logging() {
    unsafe { sys::crow_rpc_shutdown_logging() };
}
