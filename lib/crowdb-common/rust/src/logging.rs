// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `tracing-subscriber` initialization for the server and console
//! binaries. Provides file-only and file+console variants, each
//! returning a [`LogGuards`] handle whose `Drop` flushes the
//! non-blocking appender.

use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use flate2::write::GzEncoder;
use flate2::Compression;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::fmt;
use tracing_subscriber::layer::{Layer, SubscriberExt};
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

/// Default max log file size: 30 MiB.
pub const DEFAULT_LOG_MAX_FILE_MB: usize = 30;
/// Default number of rotated files to keep.
pub const DEFAULT_LOG_MAX_FILES: usize = 5;

pub struct LogGuards {
    _file: WorkerGuard,
}

/// Rotating log writer that creates a new file on each rotation.
/// Both the initial file and rotated files follow the naming pattern
/// `{prefix}-{YYYYMMDD-HHMMSS.mmm}-{pid}.log`. Rotated files are
/// gzip-compressed to `.log.gz` and pruned to `max_files`.
pub struct RotatingLogWriter {
    dir: PathBuf,
    prefix: String,
    pid: u32,
    max_file_bytes: usize,
    max_files: usize,
    current_file: Option<fs::File>,
    current_path: Option<PathBuf>,
    current_size: usize,
}

impl RotatingLogWriter {
    /// Create a new writer and open the initial log file.
    ///
    /// # Errors
    /// Returns `Err` if the log directory cannot be created or the file
    /// cannot be opened.
    pub fn new(
        dir: PathBuf,
        prefix: &str,
        pid: u32,
        max_file_mb: usize,
        max_files: usize,
    ) -> io::Result<Self> {
        fs::create_dir_all(&dir)?;
        let mut writer = Self {
            dir,
            prefix: prefix.to_string(),
            pid,
            max_file_bytes: max_file_mb * 1024 * 1024,
            max_files,
            current_file: None,
            current_path: None,
            current_size: 0,
        };
        writer.open_new_file()?;
        Ok(writer)
    }

    fn open_new_file(&mut self) -> io::Result<()> {
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_millis());
        let file_name = format!("{}-{}-{}.log", self.prefix, format_timestamp(millis), self.pid);
        let path = self.dir.join(&file_name);
        let file = fs::OpenOptions::new().create(true).append(true).open(&path)?;
        self.current_file = Some(file);
        self.current_path = Some(path);
        self.current_size = 0;
        Ok(())
    }

    fn rotate(&mut self) {
        if let Some(mut file) = self.current_file.take() {
            let _ = file.flush();
        }
        if let Some(path) = self.current_path.take() {
            let _ = gzip_file(&path);
        }
        self.prune_rotated();
        let _ = self.open_new_file();
    }

    fn prune_rotated(&self) {
        let suffix = format!("-{}.log.gz", self.pid);
        let prefix = format!("{}-", self.prefix);
        let Ok(entries) = fs::read_dir(&self.dir) else {
            return;
        };
        let mut rotated: Vec<PathBuf> = entries
            .flatten()
            .filter(|e| {
                let name = e.file_name();
                let name = name.to_string_lossy();
                name.starts_with(&prefix) && name.ends_with(&suffix)
            })
            .map(|e| e.path())
            .collect();
        // Sort descending (newest first) — timestamps sort chronologically.
        rotated.sort();
        rotated.reverse();
        for path in rotated.iter().skip(self.max_files) {
            let _ = fs::remove_file(path);
        }
    }
}

impl Write for RotatingLogWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.current_file.is_none() {
            self.open_new_file()?;
        }
        if let Some(ref mut file) = self.current_file {
            file.write_all(buf)?;
            self.current_size += buf.len();
            if self.current_size >= self.max_file_bytes {
                self.rotate();
            }
            Ok(buf.len())
        } else {
            Err(io::Error::other("no log file open"))
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        if let Some(ref mut file) = self.current_file {
            file.flush()
        } else {
            Ok(())
        }
    }
}

/// Gzip-compress a file to `{path}.gz` and delete the original.
fn gzip_file(path: &Path) -> io::Result<()> {
    let input = fs::File::open(path)?;
    let gz_path = format!("{}.gz", path.display());
    let output = fs::File::create(&gz_path)?;
    let mut encoder = GzEncoder::new(output, Compression::default());
    let mut reader = io::BufReader::new(input);
    io::copy(&mut reader, &mut encoder)?;
    encoder.finish()?;
    fs::remove_file(path)?;
    Ok(())
}

/// Format the current wall clock as `YYYYMMDD-HHMM.SS` (UTC). Used for
/// per-invocation log directory names.
#[must_use]
pub fn timestamp_secs() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    format_secs(secs)
}

/// Derive the C++ log level string from `RUST_LOG` or a fallback.
///
/// If `RUST_LOG` is set and non-empty, the first global directive (the
/// part before any `=`) is used — e.g. `RUST_LOG=debug` → `"debug"`,
/// `RUST_LOG=crowdb_kv=info` → `"info"`. If `RUST_LOG` is unset or
/// empty, `fallback` is used. If the derived level is not a valid
/// spdlog level, `"info"` is returned.
#[must_use]
pub fn cpp_level_from_rust_log(fallback: &str) -> String {
    let valid = ["trace", "debug", "info", "warn", "error", "off"];
    if let Some(rust_log) = std::env::var("RUST_LOG").ok().filter(|s| !s.is_empty()) {
        // Take the first comma-separated directive; if it has no '=',
        // it is a global level; otherwise it is target-specific and
        // we fall back to the default.
        let first = rust_log.split(',').next().unwrap_or(&rust_log);
        if let Some((level, _)) = first.split_once('=') {
            // target=level — not a global level; use fallback
            let _ = level; // target name, unused
            return fallback.to_string();
        }
        let candidate = first.trim();
        if valid.contains(&candidate) {
            return candidate.to_string();
        }
    }
    fallback.to_string()
}

/// Format epoch millis as `YYYYMMDD-HHMMSS.mmm` (UTC).
fn format_timestamp(millis: u128) -> String {
    let secs = u64::try_from(millis / 1000).unwrap_or(u64::MAX);
    let ms = millis % 1000;
    let days = secs / 86_400;
    let rem = secs % 86_400;
    let hour = rem / 3600;
    let min = (rem % 3600) / 60;
    let sec = rem % 60;
    let (year, m, d) = civil_from_days(days);
    let date = format!("{year}{m:02}{d:02}");
    let hms = format!("{hour:02}{min:02}{sec:02}");
    format!("{date}-{hms}.{ms:03}")
}

/// Split epoch seconds into `(YYYYMMDD, HHMM.SS)` (UTC).
fn split_secs(secs: u64) -> (String, String) {
    let days = secs / 86_400;
    let rem = secs % 86_400;
    let hour = rem / 3600;
    let min = (rem % 3600) / 60;
    let sec = rem % 60;
    let (year, m, d) = civil_from_days(days);
    (
        format!("{year}{m:02}{d:02}"),
        format!("{hour:02}{min:02}.{sec:02}"),
    )
}

/// Format epoch seconds as `YYYYMMDD-HHMM.SS` (UTC).
fn format_secs(secs: u64) -> String {
    let (date, hms) = split_secs(secs);
    format!("{date}-{hms}")
}

/// Civil (year, month, day) from days since 1970-01-01 (Howard Hinnant's algorithm).
fn civil_from_days(days: u64) -> (i64, i64, i64) {
    let z = i64::try_from(days).unwrap_or(i64::MAX) + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = y + i64::from(m <= 2);
    (year, m, d)
}

/// Initializes file logging to the specified directory.
///
/// `default_filter` is the `EnvFilter` directive string used when
/// `RUST_LOG` is unset — callers pass their own project-specific
/// default (e.g. `crowdb_kv` passes its `crowdb_kv`/`crowdb_kv_server`/... list)
/// so the shared library does not bake in one project's targets.
///
/// # Errors
/// Returns `Err` if the log directory cannot be created due to permission issues or invalid path.
pub fn init_file_logging(
    log_dir: impl AsRef<Path>,
    process_name: &str,
    max_file_mb: usize,
    max_files: usize,
    default_filter: &str,
) -> Result<LogGuards, String> {
    let pid = std::process::id();
    let file_appender = RotatingLogWriter::new(
        log_dir.as_ref().to_path_buf(),
        process_name,
        pid,
        max_file_mb,
        max_files,
    )
    .map_err(|e| format!("failed to open log file; next step: check path permissions: {e}"))?;
    let (file_writer, file_guard) = tracing_appender::non_blocking(file_appender);

    let file_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_filter));
    let file_layer = fmt::layer()
        .with_writer(file_writer)
        .with_ansi(false)
        .with_target(true)
        .with_thread_names(true)
        .with_filter(file_filter);

    tracing_subscriber::registry()
        .with(file_layer)
        .try_init()
        .map_err(|e| format!("failed to initialize tracing subscriber; next step: initialize logging only once per process: {e}"))?;

    Ok(LogGuards { _file: file_guard })
}

/// Opens a rotating log file with a caller-supplied prefix.
/// File naming: `{prefix}-{YYYYMMDD-HHMMSS.mmm}-{pid}.log`.
///
/// # Errors
/// Returns `Err` if the log directory cannot be created.
pub fn open_named_log(
    log_dir: impl AsRef<Path>,
    prefix: &str,
    max_file_mb: usize,
    max_files: usize,
) -> Result<RotatingLogWriter, String> {
    let pid = std::process::id();
    RotatingLogWriter::new(
        log_dir.as_ref().to_path_buf(),
        prefix,
        pid,
        max_file_mb,
        max_files,
    )
    .map_err(|e| format!("failed to open log file; next step: check path permissions: {e}"))
}

/// Opens a metrics log file in the specified directory with size-based
/// rotation and gzip compression on rotated files.
/// File naming: `{process_name}-metrics-{YYYYMMDD-HHMMSS.mmm}-{pid}.log`.
///
/// # Errors
/// Returns `Err` if the log directory cannot be created.
pub fn open_metrics_log(
    log_dir: impl AsRef<Path>,
    process_name: &str,
    max_file_mb: usize,
    max_files: usize,
) -> Result<RotatingLogWriter, String> {
    let prefix = format!("{process_name}-metrics");
    open_named_log(log_dir, &prefix, max_file_mb, max_files)
}

/// Initializes file and console logging with **split** default filters:
/// the file sink captures `file_default_filter` (typically `info`) while
/// the console sink captures `console_default_filter` (typically `warn`).
/// `RUST_LOG` overrides both sinks when set. The file layer uses a
/// rotating `RotatingLogWriter`; the console layer writes to stdout with
/// ANSI colors. Returns a `LogGuards` whose `Drop` flushes the
/// non-blocking file appender — keep it alive for the process lifetime.
///
/// # Errors
/// Returns `Err` if the log directory cannot be created due to permission issues or invalid path.
pub fn init_file_and_console_logging_split(
    log_dir: impl AsRef<Path>,
    process_name: &str,
    max_file_mb: usize,
    max_files: usize,
    file_default_filter: &str,
    console_default_filter: &str,
) -> Result<LogGuards, String> {
    let pid = std::process::id();
    let file_appender = RotatingLogWriter::new(
        log_dir.as_ref().to_path_buf(),
        process_name,
        pid,
        max_file_mb,
        max_files,
    )
    .map_err(|e| format!("failed to open log file; next step: check path permissions: {e}"))?;
    let (file_writer, file_guard) = tracing_appender::non_blocking(file_appender);

    // `EnvFilter::try_from_default_env()` returns `Ok(OFF)` for an
    // empty `RUST_LOG` string (not an error), so the fallback would
    // never fire. Treat empty/unset the same: use the caller's default.
    let env = std::env::var("RUST_LOG").ok().filter(|s| !s.is_empty());
    let file_filter = match env.as_deref() {
        Some(s) => EnvFilter::new(s),
        None => EnvFilter::new(file_default_filter),
    };
    let file_layer = fmt::layer()
        .with_writer(file_writer)
        .with_ansi(false)
        .with_target(true)
        .with_thread_names(true)
        .with_filter(file_filter);

    let console_filter = match env.as_deref() {
        Some(s) => EnvFilter::new(s),
        None => EnvFilter::new(console_default_filter),
    };
    let console_layer = fmt::layer()
        .with_ansi(true)
        .with_target(true)
        .with_thread_names(true)
        .with_filter(console_filter);

    tracing_subscriber::registry()
        .with(file_layer)
        .with(console_layer)
        .try_init()
        .map_err(|e| format!("failed to initialize tracing subscriber; next step: initialize logging only once per process: {e}"))?;

    Ok(LogGuards { _file: file_guard })
}

/// Initializes file and console logging to the specified directory.
///
/// `default_filter` is the `EnvFilter` directive string used when
/// `RUST_LOG` is unset — callers pass their own project-specific
/// default (e.g. `crowdb_kv` passes its `crowdb_kv`/`crowdb_kv_server`/... list)
/// so the shared library does not bake in one project's targets.
///
/// # Errors
/// Returns `Err` if the log directory cannot be created due to permission issues or invalid path.
pub fn init_file_and_console_logging(
    log_dir: impl AsRef<Path>,
    process_name: &str,
    max_file_mb: usize,
    max_files: usize,
    default_filter: &str,
) -> Result<LogGuards, String> {
    let pid = std::process::id();
    let file_appender = RotatingLogWriter::new(
        log_dir.as_ref().to_path_buf(),
        process_name,
        pid,
        max_file_mb,
        max_files,
    )
    .map_err(|e| format!("failed to open log file; next step: check path permissions: {e}"))?;
    let (file_writer, file_guard) = tracing_appender::non_blocking(file_appender);

    let file_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_filter));
    let file_layer = fmt::layer()
        .with_writer(file_writer)
        .with_ansi(false)
        .with_target(true)
        .with_thread_names(true)
        .with_filter(file_filter);

    let console_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_filter));
    let console_layer = fmt::layer()
        .with_ansi(true)
        .with_target(true)
        .with_thread_names(true)
        .with_filter(console_filter);

    tracing_subscriber::registry()
        .with(file_layer)
        .with(console_layer)
        .try_init()
        .map_err(|e| format!("failed to initialize tracing subscriber; next step: initialize logging only once per process: {e}"))?;

    Ok(LogGuards { _file: file_guard })
}

#[cfg(test)]
mod tests {
    use super::cpp_level_from_rust_log;
    use std::sync::{Mutex, OnceLock};

    /// Serialize tests that mutate the process-global `RUST_LOG` env
    /// var — without this, parallel tests race on `set_var`/`remove_var`
    /// and flake.
    fn rust_log_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    #[test]
    fn cpp_level_global_directive() {
        let _guard = rust_log_lock().lock().unwrap();
        std::env::set_var("RUST_LOG", "debug");
        assert_eq!(cpp_level_from_rust_log("info"), "debug");
        std::env::remove_var("RUST_LOG");
    }

    #[test]
    fn cpp_level_target_directive_uses_fallback() {
        let _guard = rust_log_lock().lock().unwrap();
        std::env::set_var("RUST_LOG", "crowdb_kv=info");
        assert_eq!(cpp_level_from_rust_log("info"), "info");
        std::env::remove_var("RUST_LOG");
    }

    #[test]
    fn cpp_level_empty_rust_log_uses_fallback() {
        let _guard = rust_log_lock().lock().unwrap();
        std::env::set_var("RUST_LOG", "");
        assert_eq!(cpp_level_from_rust_log("info"), "info");
        std::env::remove_var("RUST_LOG");
    }

    #[test]
    fn cpp_level_unset_rust_log_uses_fallback() {
        let _guard = rust_log_lock().lock().unwrap();
        std::env::remove_var("RUST_LOG");
        assert_eq!(cpp_level_from_rust_log("warn"), "warn");
    }

    #[test]
    fn cpp_level_invalid_level_uses_fallback() {
        let _guard = rust_log_lock().lock().unwrap();
        std::env::set_var("RUST_LOG", "bogus");
        assert_eq!(cpp_level_from_rust_log("info"), "info");
        std::env::remove_var("RUST_LOG");
    }
}
