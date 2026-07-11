//! `tracing-subscriber` initialization for the server and console
//! binaries. Provides file-only and file+console variants, each
//! returning a [`LogGuards`] handle whose `Drop` flushes the
//! non-blocking appender.

use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::fmt;
use tracing_subscriber::layer::{Layer, SubscriberExt};
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

pub struct LogGuards {
    _file: WorkerGuard,
}

/// Initializes file logging to the specified directory.
///
/// # Errors
/// Returns `Err` if the log directory cannot be created due to permission issues or invalid path.
pub fn init_file_logging(log_dir: impl AsRef<Path>, process_name: &str) -> Result<LogGuards, String> {
    std::fs::create_dir_all(log_dir.as_ref()).map_err(|e| {
        format!(
            "failed to create log directory {}; next step: check path permissions: {e}",
            log_dir.as_ref().display()
        )
    })?;

    let started_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| format!("system clock is before unix epoch; next step: check host clock: {e}"))?
        .as_secs();
    let file_name = format!("{process_name}-{started_at}.log");
    let file_appender = tracing_appender::rolling::never(log_dir, file_name);
    let (file_writer, file_guard) = tracing_appender::non_blocking(file_appender);

    let file_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("debug"));
    let file_layer = fmt::layer()
        .with_writer(file_writer)
        .with_ansi(false)
        .with_target(true)
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_filter(file_filter);

    tracing_subscriber::registry()
        .with(file_layer)
        .try_init()
        .map_err(|e| format!("failed to initialize tracing subscriber; next step: initialize logging only once per process: {e}"))?;

    Ok(LogGuards { _file: file_guard })
}

/// Initializes file and console logging to the specified directory.
///
/// # Errors
/// Returns `Err` if the log directory cannot be created due to permission issues or invalid path.
pub fn init_file_and_console_logging(
    log_dir: impl AsRef<Path>,
    process_name: &str,
) -> Result<LogGuards, String> {
    std::fs::create_dir_all(log_dir.as_ref()).map_err(|e| {
        format!(
            "failed to create log directory {}; next step: check path permissions: {e}",
            log_dir.as_ref().display()
        )
    })?;

    let started_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| format!("system clock is before unix epoch; next step: check host clock: {e}"))?
        .as_secs();
    let file_name = format!("{process_name}-{started_at}.log");
    let file_appender = tracing_appender::rolling::never(log_dir, file_name);
    let (file_writer, file_guard) = tracing_appender::non_blocking(file_appender);

    let file_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("debug"));
    let file_layer = fmt::layer()
        .with_writer(file_writer)
        .with_ansi(false)
        .with_target(true)
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_filter(file_filter);

    let console_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let console_layer = fmt::layer()
        .with_ansi(true)
        .with_target(true)
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_filter(console_filter);

    tracing_subscriber::registry()
        .with(file_layer)
        .with(console_layer)
        .try_init()
        .map_err(|e| format!("failed to initialize tracing subscriber; next step: initialize logging only once per process: {e}"))?;

    Ok(LogGuards { _file: file_guard })
}
