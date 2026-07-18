// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! `tracing-subscriber` initialization for the server and console
//! binaries. Provides file-only and file+console variants, each
//! returning a [`LogGuards`] handle whose `Drop` flushes the
//! non-blocking appender.

use std::io;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use file_rotate::compression::Compression;
use file_rotate::suffix::{Representation, SuffixScheme};
use file_rotate::{ContentLimit, FileRotate};
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

/// Type alias for the rotating writer used by both service and metrics logs.
pub type RotatingLogWriter = FileRotate<UtcTimestampScheme>;

/// Suffix representation: UTC timestamp `YYYYMMDD-HHMMSS` with an
/// optional collision counter (`.1`, `.2` when the same second hits
/// twice).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UtcTimestampSuffix {
    timestamp: String,
    number: Option<usize>,
}

impl std::fmt::Display for UtcTimestampSuffix {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.number {
            Some(n) => write!(f, "{}.{}", self.timestamp, n),
            None => write!(f, "{}", self.timestamp),
        }
    }
}

impl Ord for UtcTimestampSuffix {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match other.timestamp.cmp(&self.timestamp) {
            std::cmp::Ordering::Equal => other.number.cmp(&self.number),
            unequal => unequal,
        }
    }
}

impl PartialOrd for UtcTimestampSuffix {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Representation for UtcTimestampSuffix {}

/// Suffix scheme that uses UTC timestamps instead of numeric indices.
/// Rotated files are named `<base>.YYYYMMDD-HHMMSS[.N]`.
pub struct UtcTimestampScheme {
    max_files: usize,
}

impl UtcTimestampScheme {
    fn new(max_files: usize) -> Self {
        Self { max_files }
    }
}

impl SuffixScheme for UtcTimestampScheme {
    type Repr = UtcTimestampSuffix;

    fn rotate_file(
        &mut self,
        _basepath: &Path,
        newest_suffix: Option<&UtcTimestampSuffix>,
        suffix: &Option<UtcTimestampSuffix>,
    ) -> io::Result<UtcTimestampSuffix> {
        if suffix.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "UtcTimestampScheme only rotates the main file",
            ));
        }
        let ts = utc_timestamp_secs();
        let number = if let Some(ns) = newest_suffix {
            if ns.timestamp == ts {
                Some(ns.number.unwrap_or(0) + 1)
            } else {
                None
            }
        } else {
            None
        };
        Ok(UtcTimestampSuffix {
            timestamp: ts,
            number,
        })
    }

    fn parse(&self, suffix: &str) -> Option<UtcTimestampSuffix> {
        let (ts, n) = if let Some(dot) = suffix.find('.') {
            if let Ok(n) = suffix[(dot + 1)..].parse::<usize>() {
                (&suffix[..dot], Some(n))
            } else {
                return None;
            }
        } else {
            (suffix, None)
        };
        if is_valid_utc_timestamp(ts) {
            Some(UtcTimestampSuffix {
                timestamp: ts.to_string(),
                number: n,
            })
        } else {
            None
        }
    }

    fn too_old(&self, _suffix: &UtcTimestampSuffix, file_number: usize) -> bool {
        file_number >= self.max_files
    }
}

/// Generate a UTC timestamp string in `YYYYMMDD-HHMMSS` format.
fn utc_timestamp_secs() -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis());
    let secs = u64::try_from(millis / 1000).unwrap_or(0);
    let days = secs / 86_400;
    let rem = secs % 86_400;
    let hour = rem / 3600;
    let min = (rem % 3600) / 60;
    let sec = rem % 60;
    let z = i64::try_from(days).unwrap_or(0) + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = y + i64::from(m <= 2);
    format!("{year}{m:02}{d:02}-{hour:02}{min:02}{sec:02}")
}

/// Validate that `s` matches `YYYYMMDD-HHMMSS` (15 chars, dash at pos 8).
fn is_valid_utc_timestamp(s: &str) -> bool {
    s.len() == 15
        && s.as_bytes()[8] == b'-'
        && s.as_bytes()[..8].iter().all(u8::is_ascii_digit)
        && s.as_bytes()[9..].iter().all(u8::is_ascii_digit)
}

/// Create a `FileRotate` writer with the given path, size limit, file count,
/// and gzip compression on rotated files.
fn make_rotating_writer(path: PathBuf, max_file_mb: usize, max_files: usize) -> RotatingLogWriter {
    FileRotate::new(
        path,
        UtcTimestampScheme::new(max_files),
        ContentLimit::Bytes(max_file_mb * 1024 * 1024),
        Compression::OnRotate(max_files),
        None,
    )
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

    // Civil from days since 1970-01-01 (Howard Hinnant's algorithm).
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

    format!("{year}{m:02}{d:02}-{hour:02}{min:02}{sec:02}.{ms:03}")
}

/// Initializes file logging to the specified directory.
///
/// # Errors
/// Returns `Err` if the log directory cannot be created due to permission issues or invalid path.
pub fn init_file_logging(
    log_dir: impl AsRef<Path>,
    process_name: &str,
    max_file_mb: usize,
    max_files: usize,
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
        .as_millis();
    let pid = std::process::id();
    let file_name = format!("{process_name}-{}-{pid}.log", format_timestamp(started_at));
    let file_path = log_dir.as_ref().join(file_name);
    let file_appender = make_rotating_writer(file_path, max_file_mb, max_files);
    let (file_writer, file_guard) = tracing_appender::non_blocking(file_appender);

    let file_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("warn,crowkv=debug,crowkv_server=debug,crowkv_web=debug,crowkv_console_shared=debug,crowkv_cli=debug"));
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
    std::fs::create_dir_all(log_dir.as_ref()).map_err(|e| {
        format!(
            "failed to create log directory {}; next step: check path permissions: {e}",
            log_dir.as_ref().display()
        )
    })?;

    let started_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| format!("system clock is before unix epoch; next step: check host clock: {e}"))?
        .as_millis();
    let pid = std::process::id();
    let file_name = format!(
        "{process_name}-metrics-{}-{pid}.log",
        format_timestamp(started_at)
    );
    let file_path = log_dir.as_ref().join(file_name);

    Ok(make_rotating_writer(file_path, max_file_mb, max_files))
}

/// Initializes file and console logging to the specified directory.
///
/// # Errors
/// Returns `Err` if the log directory cannot be created due to permission issues or invalid path.
pub fn init_file_and_console_logging(
    log_dir: impl AsRef<Path>,
    process_name: &str,
    max_file_mb: usize,
    max_files: usize,
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
        .as_millis();
    let pid = std::process::id();
    let file_name = format!("{process_name}-{}-{pid}.log", format_timestamp(started_at));
    let file_path = log_dir.as_ref().join(file_name);
    let file_appender = make_rotating_writer(file_path, max_file_mb, max_files);
    let (file_writer, file_guard) = tracing_appender::non_blocking(file_appender);

    let file_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("warn,crowkv=debug,crowkv_server=debug,crowkv_web=debug,crowkv_console_shared=debug,crowkv_cli=debug"));
    let file_layer = fmt::layer()
        .with_writer(file_writer)
        .with_ansi(false)
        .with_target(true)
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_filter(file_filter);

    let console_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        EnvFilter::new(
            "warn,crowkv=info,crowkv_server=info,crowkv_web=info,crowkv_console_shared=info,crowkv_cli=info",
        )
    });
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
