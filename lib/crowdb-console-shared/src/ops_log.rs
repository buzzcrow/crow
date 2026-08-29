// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Per-session operation log for the console (CLI + web).
//!
//! Each console session opens one append-only JSON-Lines file under
//! `~/.crowdb-kv/log/`. Every outbound action issued by `shared::clients`
//! (HTTP, crowdb-rpc, SSH) appends one record carrying:
//!
//! - `ts_unix_ms`  — wall-clock time of the request.
//! - `corr_id`     — current `corr_id` task-local (see `crate::corr_id`).
//! - `kind`        — `"http"` / `"rpc"` / `"ssh"`.
//! - `target`      — target URL or `host:port`.
//! - `op`          — `"GET /api/..."`, `"PxKvStore.Put"`, etc.
//! - `status`      — HTTP status, crowdb-rpc code, exit status.
//! - `duration_ms` — wall-clock cost of the call.
//! - `body`        — optional short JSON summary (no secrets).
//!
//! Reproducibility intent: an operator can paste the recorded URL +
//! body into curl/rpcurl/ssh and replay any failed step.
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
    /// Append `record` as one JSON line. Errors are swallowed and
    /// logged via `tracing::warn!` so a misbehaving filesystem can
    /// never abort an in-flight user action.
    pub fn append(&self, record: &Value) {
        let line = match serde_json::to_string(record) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(error = %e, "ops_log: failed to encode record");
                return;
            }
        };
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

/// Borrow the global log handle, if initialised.
#[must_use]
pub fn current() -> Option<&'static OpsLog> {
    OPS_LOG.get()
}

/// Build the default log path for one console session.
///
/// `role` is a short tag (`"web"`, `"cli"`) so concurrent sessions
/// don't write to the same file.
#[must_use]
pub(crate) fn default_path(role: &str) -> PathBuf {
    let dir = dirs::home_dir()
        .unwrap_or_else(std::env::temp_dir)
        .join(".crowdb-kv")
        .join("log");
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    let pid = std::process::id();
    dir.join(format!("console-{role}-{secs}-{pid}.log"))
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead, BufReader};

    #[test]
    fn default_path_is_under_crowdb_kv_log() {
        let p = default_path("test");
        let s = p.to_string_lossy().to_string();
        assert!(s.contains(".crowdb-kv"), "{s}");
        assert!(s.contains("log"), "{s}");
        assert!(s.contains("console-test-"), "{s}");
    }

    #[test]
    fn ops_log_writes_jsonl_record() {
        let tmp = std::env::temp_dir().join(format!("ops-log-test-{}.jsonl", std::process::id()));
        let _ = std::fs::remove_file(&tmp);
        let log = OpsLog {
            file: Mutex::new(OpenOptions::new().create(true).append(true).open(&tmp).unwrap()),
            path: tmp.clone(),
        };
        log.append(&json!({"kind": "http", "op": "GET /test"}));
        log.append(&json!({"kind": "ssh", "op": "exec"}));

        let f = std::fs::File::open(&tmp).unwrap();
        let lines: Vec<String> = BufReader::new(f)
            .lines()
            .map_while(std::result::Result::ok)
            .collect();
        assert_eq!(lines.len(), 2);
        let v0: serde_json::Value = serde_json::from_str(&lines[0]).unwrap();
        assert_eq!(v0["op"], "GET /test");
        let v1: serde_json::Value = serde_json::from_str(&lines[1]).unwrap();
        assert_eq!(v1["kind"], "ssh");

        let _ = std::fs::remove_file(&tmp);
    }
}
