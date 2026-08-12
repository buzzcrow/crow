// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Re-export of the logging primitives now living in `crow-common`.
//! The `crow_kv`-specific `init_file_logging` /
//! `init_file_and_console_logging` wrappers preserve the original 4-arg
//! signature and supply the `crow_kv` default `EnvFilter` so existing
//! call sites compile unchanged; the implementation moved to
//! `crow_common::logging` (R12).

pub use crow_common::logging::{open_metrics_log, LogGuards, RotatingLogWriter};

/// `crow_kv` default `EnvFilter` directive used when `RUST_LOG` is unset.
pub(crate) const CROW_KV_DEFAULT_FILTER: &str =
    "warn,crow_kv=info,crow_kv_server=info,crow_web=info,crow_console_shared=info,crow_cli=info";

/// Initializes file logging to the specified directory using the
/// `crow_kv` default `EnvFilter`. Thin wrapper over
/// `crow_common::logging::init_file_logging`.
///
/// # Errors
/// Returns `Err` if the log directory cannot be created due to permission issues or invalid path.
pub fn init_file_logging(
    log_dir: impl AsRef<std::path::Path>,
    process_name: &str,
    max_file_mb: usize,
    max_files: usize,
) -> Result<LogGuards, String> {
    crow_common::logging::init_file_logging(
        log_dir,
        process_name,
        max_file_mb,
        max_files,
        CROW_KV_DEFAULT_FILTER,
    )
}

/// Initializes file and console logging to the specified directory using
/// the `crow_kv` default `EnvFilter`. Thin wrapper over
/// `crow_common::logging::init_file_and_console_logging`.
///
/// # Errors
/// Returns `Err` if the log directory cannot be created due to permission issues or invalid path.
pub fn init_file_and_console_logging(
    log_dir: impl AsRef<std::path::Path>,
    process_name: &str,
    max_file_mb: usize,
    max_files: usize,
) -> Result<LogGuards, String> {
    crow_common::logging::init_file_and_console_logging(
        log_dir,
        process_name,
        max_file_mb,
        max_files,
        CROW_KV_DEFAULT_FILTER,
    )
}
