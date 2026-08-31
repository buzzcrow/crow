// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Per-session operation log for the console (CLI + web).
//!
//! Each console session opens one append-only text file under
//! `~/.crowdb-kv/log/`. Every outbound action issued by `shared::clients`
//! (HTTP, crowdb-rpc, SSH) appends one human-readable line carrying:
//!
//! - timestamp  — wall-clock time of the request.
//! - kind       — `"http"` / `"rpc"` / `"ssh"` / `"bench_report"`.
//! - op         — `"GET /api/..."`, `"PxKvStore.Put"`, etc.
//! - target     — target URL or `host:port`.
//! - status     — HTTP status, crowdb-rpc code, exit status.
//! - duration   — wall-clock cost of the call.
//! - body       — optional short summary (no secrets).
//!
//! Reproducibility intent: an operator can read the recorded line
//! and replay the step via curl/rpcurl/ssh.
//!
//! Key work: lazy global init (`init`/`init_default`), thread-safe
//! append (one `std::sync::Mutex` around the file handle), default
//! path derivation per design `~/.crowdb-kv/log/console-<role>-<ts>-<pid>.log`,
//! and convenience helpers (`append_http`).

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;
use serde_json::{json, Value};

static OPS_LOG: OnceLock<OpsLog> = OnceLock::new();

/// One open append-only operation log file.
pub struct OpsLog {
    file: Mutex<File>,
    path: PathBuf,
}

impl OpsLog {
    /// Append `record` as one human-readable line. Errors are swallowed
    /// and logged via `tracing::warn!` so a misbehaving filesystem can
    /// never abort an in-flight user action.
    pub fn append(&self, record: &Value) {
        let line = format_record(record);
        let mut guard = match self.file.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        if let Err(e) = writeln!(*guard, "{line}") {
            tracing::warn!(error = %e, path = %self.path.display(), "ops_log: write failed");
        }
    }

    /// The absolute path this log writes to. Surfaced so the CLI can
    /// print it on `--verbose` and exit messages can point at it.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// Initialise the global operation log with an explicit path. Returns
/// `Err` if the file cannot be opened. Subsequent calls are no-ops
/// (the first init wins).
///
/// # Errors
/// Returns the underlying `io::Error` when the file or its parent
/// directory cannot be created.
pub(crate) fn init(path: PathBuf) -> std::io::Result<()> {
    if OPS_LOG.get().is_some() {
        return Ok(());
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let file = OpenOptions::new().create(true).append(true).open(&path)?;
    let _ = OPS_LOG.set(OpsLog {
        file: Mutex::new(file),
        path,
    });
    Ok(())
}

/// Initialise the global operation log at the default path for `role`
/// (`"web"` or `"cli"`). On a fresh process this opens
/// `~/.crowdb-kv/log/console-<role>-<unix_secs>-<pid>.log`. Errors are
/// logged and discarded — operation logging is best-effort.
pub fn init_default(role: &str) {
    let path = default_path(role);
    if let Err(e) = init(path.clone()) {
        tracing::warn!(error = %e, path = %path.display(), "ops_log: init_default failed");
    }
}

/// Initialise the global operation log inside an explicit `dir` for
/// `role`. The file is `dir/console-<role>-<unix_secs>-<pid>.log`. Used
/// by the CLI to land the ops log alongside the tracing/RPC logs in a
/// per-invocation directory. Errors are logged and discarded.
pub fn init_in(dir: &Path, role: &str) {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    let pid = std::process::id();
    let path = dir.join(format!("console-{role}-{secs}-{pid}.log"));
    if let Err(e) = init(path.clone()) {
        tracing::warn!(error = %e, path = %path.display(), "ops_log: init_in failed");
    }
}

/// Borrow the global log handle, if initialised.
#[must_use]
pub fn current() -> Option<&'static OpsLog> {
    OPS_LOG.get()
}

/// Build the default log path for one console session.
///
/// `role` is a short tag (`"web"`, `"cli"`) so concurrent sessions
/// don't write to the same file. Logs are written to a project-local
/// `cli-log/` directory (resolved from CWD) so they survive for
/// inspection instead of being hidden in `~/.crowdb-kv/log/`.
#[must_use]
pub(crate) fn default_path(role: &str) -> PathBuf {
    let dir = std::env::current_dir()
        .unwrap_or_else(|_| dirs::home_dir().unwrap_or_else(std::env::temp_dir))
        .join("cli-log");
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    let pid = std::process::id();
    dir.join(format!("console-{role}-{secs}-{pid}.log"))
}

/// Append a custom record with a `kind` tag and an arbitrary JSON
/// `body`. The record is merged with `ts_unix_ms` and `kind` fields
/// so it follows the same JSON-Lines schema as `append_http` etc.
/// Used by bench commands to log the final report into the ops log.
pub fn append_custom(kind: &str, body: &Value) {
    if let Some(log) = OPS_LOG.get() {
        let mut record = body.clone();
        if let Some(obj) = record.as_object_mut() {
            obj.insert("ts_unix_ms".to_string(), json!(now_ms()));
            obj.insert("kind".to_string(), json!(kind));
        } else {
            record = json!({"ts_unix_ms": now_ms(), "kind": kind, "body": body});
        }
        log.append(&record);
    }
}

/// Append one HTTP-call record. `body_summary` is included verbatim
/// (callers strip secrets / truncate as appropriate).
pub fn append_http(
    corr_id: &str,
    method: &str,
    url: &str,
    status: u16,
    duration_ms: u128,
    body_summary: Option<&str>,
) {
    if let Some(log) = OPS_LOG.get() {
        log.append(&json!({
            "ts_unix_ms": now_ms(),
            "corr_id": corr_id,
            "kind": "http",
            "op": format!("{method} {url}"),
            "target": url,
            "status": status,
            "duration_ms": u64::try_from(duration_ms).unwrap_or(u64::MAX),
            "body": body_summary,
        }));
    }
}

/// Append one crowdb-rpc-call record. `service_method` is the standard
/// `package.Service/Method` form.
#[allow(dead_code)]
pub(crate) fn append_rpc(corr_id: &str, target: &str, service_method: &str, status: &str, duration_ms: u128) {
    if let Some(log) = OPS_LOG.get() {
        log.append(&json!({
            "ts_unix_ms": now_ms(),
            "corr_id": corr_id,
            "kind": "rpc",
            "op": service_method,
            "target": target,
            "status": status,
            "duration_ms": u64::try_from(duration_ms).unwrap_or(u64::MAX),
        }));
    }
}

/// Append one SSH-call record.
#[allow(dead_code)]
pub(crate) fn append_ssh(
    corr_id: &str,
    target: &str,
    command: &str,
    exit_code: Option<i32>,
    duration_ms: u128,
) {
    if let Some(log) = OPS_LOG.get() {
        log.append(&json!({
            "ts_unix_ms": now_ms(),
            "corr_id": corr_id,
            "kind": "ssh",
            "op": "exec",
            "target": target,
            "command": command,
            "status": exit_code,
            "duration_ms": u64::try_from(duration_ms).unwrap_or(u64::MAX),
        }));
    }
}

/// Custom record type for callers that need more fields than the
/// `append_*` shortcuts expose.
#[derive(Debug, Serialize)]
#[allow(dead_code)]
pub(crate) struct CustomRecord<'a> {
    pub corr_id: &'a str,
    pub kind: &'a str,
    pub op: &'a str,
    pub target: &'a str,
    pub status: &'a str,
    pub duration_ms: u64,
}

fn now_ms() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_millis()),
    )
    .unwrap_or(u64::MAX)
}

/// Format a JSON record value as a human-readable log line.
///
/// Standard fields (`ts_unix_ms`, `kind`, `op`, `target`, `status`,
/// `duration_ms`, `body`, `corr_id`) are extracted and printed in a
/// fixed order. Any remaining fields are appended as `key=value` pairs.
/// Example output:
///   `2026-08-31T07:32:25.672Z http GET http://127.0.0.1:46581/topology status=0 0ms transport error: ...`
fn format_record(record: &Value) -> String {
    let Some(obj) = record.as_object() else {
        return serde_json::to_string(record).unwrap_or_default();
    };

    let ts = obj
        .get("ts_unix_ms")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let kind = obj
        .get("kind")
        .and_then(Value::as_str)
        .unwrap_or("?");
    let op = obj.get("op").and_then(Value::as_str).unwrap_or("");
    let target = obj.get("target").and_then(Value::as_str).unwrap_or("");
    let status = obj.get("status");
    let duration_ms = obj
        .get("duration_ms")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let body = obj.get("body").and_then(Value::as_str);
    let corr_id = obj.get("corr_id").and_then(Value::as_str).unwrap_or("");

    let ts_str = format_ts(ts);

    let mut parts: Vec<String> = vec![ts_str, kind.to_string()];
    if !op.is_empty() {
        parts.push(op.to_string());
    }
    if !target.is_empty() && target != op {
        parts.push(format!("target={target}"));
    }
    if let Some(s) = status {
        parts.push(format!("status={}", format_json_value(s)));
    }
    if duration_ms > 0 {
        parts.push(format!("{duration_ms}ms"));
    }
    if !corr_id.is_empty() {
        parts.push(format!("corr={corr_id}"));
    }
    if let Some(b) = body {
        if !b.is_empty() {
            parts.push(b.to_string());
        }
    }

    // Append any non-standard fields as key=value.
    let standard: [&str; 8] = [
        "ts_unix_ms",
        "kind",
        "op",
        "target",
        "status",
        "duration_ms",
        "body",
        "corr_id",
    ];
    for (k, v) in obj {
        if standard.contains(&k.as_str()) {
            continue;
        }
        parts.push(format!("{k}={}", format_json_value(v)));
    }

    parts.join(" ")
}

/// Format a JSON value for human-readable output.
fn format_json_value(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Null => "null".to_string(),
        other => other.to_string(),
    }
}

/// Format epoch millis as `YYYY-MM-DDTHH:MM:SS.mmmZ` (UTC).
fn format_ts(ms: u64) -> String {
    let secs = ms / 1000;
    let millis = ms % 1000;
    let days = secs / 86_400;
    let rem = secs % 86_400;
    let hour = rem / 3600;
    let min = (rem % 3600) / 60;
    let sec = rem % 60;
    let (year, m, d) = civil_from_days(days);
    format!("{year:04}-{m:02}-{d:02}T{hour:02}:{min:02}:{sec:02}.{millis:03}Z")
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
    (y, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crowdb_test_harness::test_dirs;
    use std::io::{BufRead, BufReader};

    #[test]
    fn default_path_is_under_cli_log() {
        let p = default_path("test");
        let s = p.to_string_lossy().to_string();
        assert!(s.contains("cli-log"), "{s}");
        assert!(s.contains("console-test-"), "{s}");
    }

    #[test]
    fn ops_log_writes_readable_record() {
        let tmp = test_dirs::test_data_dir().join(format!("ops-log-test-{}.log", std::process::id()));
        let _ = std::fs::remove_file(&tmp);
        let log = OpsLog {
            file: Mutex::new(OpenOptions::new().create(true).append(true).open(&tmp).unwrap()),
            path: tmp.clone(),
        };
        log.append(&json!({"kind": "http", "op": "GET /test", "ts_unix_ms": 0}));
        log.append(&json!({"kind": "ssh", "op": "exec", "ts_unix_ms": 0}));

        let f = std::fs::File::open(&tmp).unwrap();
        let lines: Vec<String> = BufReader::new(f)
            .lines()
            .map_while(std::result::Result::ok)
            .collect();
        assert_eq!(lines.len(), 2);
        assert!(lines[0].contains("http"), "line 0: {lines[0]}");
        assert!(lines[0].contains("GET /test"), "line 0: {lines[0]}");
        assert!(lines[1].contains("ssh"), "line 1: {lines[1]}");
        assert!(lines[1].contains("exec"), "line 1: {lines[1]}");

        let _ = std::fs::remove_file(&tmp);
    }
}
