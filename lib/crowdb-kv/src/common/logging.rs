// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Re-export of the logging primitives now living in `crowdb-common`.
//! The `crowdb_kv`-specific `init_file_logging` /
//! `init_file_and_console_logging` wrappers preserve the original 4-arg
//! signature and supply the `crowdb_kv` default `EnvFilter` so existing
//! call sites compile unchanged; the implementation moved to
//! `crowdb_common::logging` (R12).

pub use crowdb_common::logging::{open_metrics_log, LogGuards, RotatingLogWriter};

/// `crowdb_kv` default `EnvFilter` directive used when `RUST_LOG` is unset.
pub(crate) const CROWDB_KV_DEFAULT_FILTER: &str =
    "warn,crowdb_kv=info,crowdb_kv_server=info,crowdb_web=info,crowdb_console_shared=info,crowdb_cli=info";

/// Initializes file logging to the specified directory using the
/// `crowdb_kv` default `EnvFilter`. Thin wrapper over
/// `crowdb_common::logging::init_file_logging`.
///
/// # Errors
/// Returns `Err` if the log directory cannot be created due to permission issues or invalid path.
pub fn init_file_logging(
    log_dir: impl AsRef<std::path::Path>,
    process_name: &str,
    max_file_mb: usize,
    max_files: usize,
) -> Result<LogGuards, String> {
    crowdb_common::logging::init_file_logging(
        log_dir,
        process_name,
        max_file_mb,
        max_files,
        CROWDB_KV_DEFAULT_FILTER,
    )
}

/// Initializes file and console logging to the specified directory using
/// the `crowdb_kv` default `EnvFilter`. Thin wrapper over
/// `crowdb_common::logging::init_file_and_console_logging`.
///
/// # Errors
/// Returns `Err` if the log directory cannot be created due to permission issues or invalid path.
pub fn init_file_and_console_logging(
    log_dir: impl AsRef<std::path::Path>,
    process_name: &str,
    max_file_mb: usize,
    max_files: usize,
) -> Result<LogGuards, String> {
    crowdb_common::logging::init_file_and_console_logging(
        log_dir,
        process_name,
        max_file_mb,
        max_files,
        CROWDB_KV_DEFAULT_FILTER,
    )
}
