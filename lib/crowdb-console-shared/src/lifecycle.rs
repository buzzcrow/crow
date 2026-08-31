// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Server-instance lifecycle (deploy / start / stop).
//!
//! C3 status: **local-spawn placeholder**. `deploy_local` runs
//! `tokio::process::Command` against the `crowdb-kv-server` binary on the
//! current host; the `node.host` is honored for URL construction but
//! ignored for transport. C4 replaces this module's body with `russh`,
//! preserving the public API.

use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::time::Instant;
use tracing::{debug, warn};

use crate::clients::http::ServerClient;
use crate::config::NodeEntry;
use crate::error::{Error, Result};

/// Inputs for a deploy. The console picks the ports; the user provides ids.
#[derive(Debug, Clone, Default)]
pub struct DeployRequest {
    pub server_id: String,
    pub rest_port: u16,
    pub rpc_port: u16,
    /// Optional override of the binary path. Defaults via
    /// `crowdb_kv_server_bin()` resolution: `$CROWDB_KV_SERVER_BIN` →
    /// `$PATH` → `target/{debug,release}/crowdb-kv-server` next to the
    /// current executable.
    pub binary: Option<PathBuf>,
    pub election_profile: Option<String>,
    /// `--kv-backend` value (e.g. `"file"`, `"block"`, `"mem-block"`).
    pub kv_backend: Option<String>,
    /// `--wal-backend` value (e.g. `"file"`, `"mem-block"`, `"block-device"`).
    pub wal_backend: Option<String>,
    /// Sets `--no-fsync` on the spawned server when `true`
    /// (benchmark path-overhead isolation mode).
    pub no_fsync: bool,
    /// `--metrics-interval` value in seconds. `None` leaves the
    /// spawned server's own default in effect.
    pub metrics_interval: Option<u64>,
    /// `--max-inflight` value. `None` leaves the spawned server's
    /// own default in effect.
    pub max_inflight: Option<usize>,
    /// `--coalesce-max-keys` value. `None` leaves the spawned server's
    /// own default in effect.
    pub coalesce_max_keys: Option<usize>,
    /// `--coalesce-drain-threshold` value. `None` leaves the spawned
    /// server's own default in effect.
    pub coalesce_drain_threshold: Option<usize>,
    /// `--peer-pool-size` value. `None` leaves the spawned server's
    /// own default in effect.
    pub peer_pool_size: Option<usize>,
    /// `--enable-nagle` flag. `None` leaves the spawned server's own
    /// default in effect.
    pub enable_nagle: Option<bool>,
    /// `--quickack` flag. `None` leaves the spawned server's own
    /// default in effect.
    pub quickack: Option<bool>,
    /// `--event-write` flag. `None` leaves the spawned server's own
    /// default in effect.
    pub event_write: Option<bool>,
    /// `--send-queue-capacity` value. `None` leaves the spawned
    /// server's own default in effect.
    pub send_queue_capacity: Option<u32>,
    /// Optional `--config` TOML path for `crowdb-kv-server` (first-boot
    /// tunable overrides only; ignored in restore mode).
    pub config: Option<PathBuf>,
    /// `--rpc-workers` value for the spawned `crowdb-kv-server`. `None`
    /// leaves the server's default (2) in effect.
    pub rpc_workers: Option<u32>,
}

/// Result of a successful deploy. Persist these fields onto the
/// `ServerEntry` so `stop` can locate the process later.
#[derive(Debug, Clone)]
pub struct DeployedServer {
    pub server_id: String,
    pub mgmt_url: String,
    pub rpc_url: String,
    pub pid: u32,
}

/// Spawn `crowdb-kv-server` locally. The `node.host` is folded into the
/// returned URLs so the rest of the console can address the instance
/// uniformly with the SSH path coming in C4.
///
/// # Errors
/// Returns `Error::Validation` for bad inputs and `Error::Io` for spawn
/// or readiness failures.
pub async fn deploy_local(req: &DeployRequest, node: &NodeEntry) -> Result<DeployedServer> {
    deploy_local_in_workspace(req, node, None, &[]).await
}

/// Deploys a server in a specific workspace directory.
///
/// # Errors
///
/// Returns `Error::Validation` for bad inputs and `Error::Io` for spawn
/// or readiness failures.
pub async fn deploy_local_in_dir(
    req: &DeployRequest,
    node: &NodeEntry,
    workspace_dir: &std::path::Path,
) -> Result<DeployedServer> {
    deploy_local_in_workspace(req, node, Some(workspace_dir), &[]).await
}

/// Deploys a server in a specific workspace directory, passing extra
/// CLI arguments to the spawned `crowdb-kv-server` binary. Used by tests
/// to bootstrap previously-created stores/groups on restart so the
/// server recovers from WAL and rejoins the cluster.
///
/// # Errors
///
/// Returns `Error::Validation` for bad inputs and `Error::Io` for spawn
/// or readiness failures.
pub async fn deploy_local_in_dir_with_extra_args(
    req: &DeployRequest,
    node: &NodeEntry,
    workspace_dir: &std::path::Path,
    extra_args: &[String],
) -> Result<DeployedServer> {
    deploy_local_in_workspace(req, node, Some(workspace_dir), extra_args).await
}

/// Append `--kv-backend`/`--wal-backend`/`--no-fsync`/`--metrics-interval`/
/// `--max-inflight` flags to the spawned `crowdb-kv-server` command per `req`.
/// Split out of `deploy_local_in_workspace` to keep it under the line-count
/// lint.
fn apply_benchmark_flags(cmd: &mut Command, req: &DeployRequest) {
    if let Some(kv_backend) = &req.kv_backend {
        cmd.arg("--kv-backend").arg(kv_backend);
    }
    if let Some(wal_backend) = &req.wal_backend {
        cmd.arg("--wal-backend").arg(wal_backend);
    }
    if req.no_fsync {
        cmd.arg("--no-fsync");
    }
    if let Some(metrics_interval) = req.metrics_interval {
        cmd.arg("--metrics-interval").arg(metrics_interval.to_string());
    }
    if let Some(max_inflight) = req.max_inflight {
        cmd.arg("--max-inflight").arg(max_inflight.to_string());
    }
    if let Some(max_keys) = req.coalesce_max_keys {
        cmd.arg("--coalesce-max-keys").arg(max_keys.to_string());
    }
    if let Some(threshold) = req.coalesce_drain_threshold {
        cmd.arg("--coalesce-drain-threshold").arg(threshold.to_string());
    }
    if let Some(workers) = req.rpc_workers {
        cmd.arg("--rpc-workers").arg(workers.to_string());
    }
    if let Some(pool_size) = req.peer_pool_size {
        cmd.arg("--peer-pool-size").arg(pool_size.to_string());
    }
    if let Some(true) = req.enable_nagle {
        cmd.arg("--enable-nagle");
    }
    if let Some(true) = req.quickack {
        cmd.arg("--quickack");
    }
    if let Some(true) = req.event_write {
        cmd.arg("--event-write");
    }
    if let Some(cap) = req.send_queue_capacity {
        cmd.arg("--send-queue-capacity").arg(cap.to_string());
    }
}

/// Resolve the `--config` path for a deploy. When `req.config` is set,
/// it is used verbatim. When unset, returns `None` — the server boots
/// with `CrowDBConfig::default()` tunables (no toml needed). The
/// `--config` flag is now optional and only used for first-boot
/// tunable overrides.
fn resolve_config_path(req: &DeployRequest) -> Option<PathBuf> {
    req.config.clone()
}

async fn deploy_local_in_workspace(
    req: &DeployRequest,
    node: &NodeEntry,
    workspace_dir: Option<&std::path::Path>,
    extra_args: &[String],
) -> Result<DeployedServer> {
    if req.rest_port == 0 || req.rpc_port == 0 {
        return Err(Error::Validation {
            field: "port".into(),
            message: "rest_port and rpc_port must be non-zero".into(),
        });
    }
    if req.rest_port == req.rpc_port {
        return Err(Error::Validation {
            field: "port".into(),
            message: "rest_port and rpc_port must differ".into(),
        });
    }

    let binary = match &req.binary {
        Some(p) => p.clone(),
        None => crowdb_kv_server_bin().ok_or_else(|| Error::Validation {
            field: "binary".into(),
            message: "could not locate crowdb-kv-server binary; set $CROWDB_KV_SERVER_BIN".into(),
        })?,
    };
    let launch_binary = if let Some(dir) = workspace_dir {
        stage_server_binary(&binary, dir)?
    } else {
        binary.clone()
    };

    let config_path = resolve_config_path(req);

    let mgmt_url = format!("http://{}:{}", node.host, req.rest_port);
    let rpc_url = format!("http://{}:{}", node.host, req.rpc_port);

    let mut cmd = Command::new(&launch_binary);
    cmd.arg("--management-addr")
        .arg("127.0.0.1")
        .arg("--management-port")
        .arg(req.rest_port.to_string())
        .arg("--ports")
        .arg(req.rpc_port.to_string())
        .arg("--election-profile")
        .arg(
            req.election_profile
                .as_deref()
                .map(str::to_owned)
                .or_else(|| std::env::var("CROWDB_KV_SERVER_ELECTION_PROFILE").ok())
                .unwrap_or_else(|| "default".into()),
        )
        .kill_on_drop(false);
    if let Some(config) = &config_path {
        cmd.arg("--config").arg(config);
    }
    apply_benchmark_flags(&mut cmd, req);
    for arg in extra_args {
        cmd.arg(arg);
    }
    if let Some(dir) = workspace_dir {
        // The workspace dir is the node root; waldata/conf/ctdata/log
        // are derived subdirs.
        cmd.arg("--root").arg(dir);
        // Merge stdout and stderr into one file. We open a temp file before
        // spawn (PID unknown), then rename it with the PID after spawn.
        let log_dir = dir.join("log");
        let tmp_path = log_dir.join("crowdb-kv-server.stdout.log");
        let out = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&tmp_path)
            .map_err(Error::Io)?;
        cmd.current_dir(dir);
        cmd.stdout(Stdio::from(out.try_clone().map_err(Error::Io)?));
        cmd.stderr(Stdio::from(out));
    } else {
        // Non-workspace deploy: create a unique temp dir as the node
        // root (waldata/conf/ctdata/log derived under it).
        let root =
            std::path::Path::new("temp-data").join(format!("crowdb-kv-server-deploy-{}", req.rest_port));
        std::fs::create_dir_all(&root).map_err(Error::Io)?;
        cmd.arg("--root").arg(&root);
        cmd.stdout(Stdio::piped());
        cmd.stderr(Stdio::piped());
    }
    let mut child = cmd.spawn().map_err(Error::Io)?;

    let pid = child.id().ok_or_else(|| Error::Validation {
        field: "pid".into(),
        message: "spawned child has no pid".into(),
    })?;

    // Rename the temp stdout file to include the PID.
    if let Some(dir) = workspace_dir {
        let log_dir = dir.join("log");
        let from = log_dir.join("crowdb-kv-server.stdout.log");
        let to = log_dir.join(format!("crowdb-kv-server-{pid}.out.log"));
        let _ = std::fs::rename(&from, &to);
    }

    // Drain stdout/stderr to a debug logger so the child doesn't block on
    // a full pipe. We deliberately don't wait for "management_addr=" here:
    // the user supplied the port, so we know mgmt_url; readiness is
    // confirmed by polling /health.
    if workspace_dir.is_none() {
        if let Some(stdout) = child.stdout.take() {
            spawn_log_pipe(stdout, "crowdb-kv-server stdout");
        }
        if let Some(stderr) = child.stderr.take() {
            spawn_log_pipe(stderr, "crowdb-kv-server stderr");
        }
    }

    // Detach: drop the Child handle so the process is not killed when
    // this function returns. The pid is the user's tracking handle.
    std::mem::forget(child);

    wait_for_ready(&mgmt_url, Duration::from_secs(30)).await?;

    Ok(DeployedServer {
        server_id: req.server_id.clone(),
        mgmt_url,
        rpc_url,
        pid,
    })
}

/// Send SIGTERM to a tracked pid on the **local** host. Returns
/// `Ok(false)` if the pid is already gone. Implemented by shelling out
/// to `/bin/kill`, matching `crowdb-kv-server/tests/testkit/process.rs` so
/// that both paths behave identically and the workspace
/// `unsafe_code = deny` lint is kept.
///
/// For SSH-deployed servers, use `crowdb_kv_ssh` to run the
/// equivalent command on the remote host.
///
/// # Errors
/// Surfaces spawn / wait failures as `Error::Io`.
pub fn stop_pid(pid: u32) -> Result<bool> {
    stop_pid_with_timeout(pid, std::time::Duration::from_secs(15))
}

/// Same as [`stop_pid`] but with a configurable wait timeout before
/// force-killing. Used by tests to keep test runtime short.
///
/// # Errors
/// Surfaces spawn / wait failures as `Error::Io`.
pub fn stop_pid_with_timeout(pid: u32, timeout: std::time::Duration) -> Result<bool> {
    let status = std::process::Command::new("kill")
        .arg("-TERM")
        .arg(pid.to_string())
        .status()
        .map_err(Error::Io)?;
    if !status.success() {
        return Ok(false);
    }
    // Wait for the process to actually exit so the caller can safely
    // reuse resources (ports, WAL files) without racing the old process.
    //
    // We use `ps -p PID -o stat=` instead of `kill -0` because `kill -0`
    // returns success for zombie processes (exited but not reaped by
    // parent), causing a false "still alive" result.
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        if !process_is_alive(pid) {
            return Ok(true);
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    // Process didn't exit within timeout — force kill.
    let _ = std::process::Command::new("kill")
        .arg("-KILL")
        .arg(pid.to_string())
        .status();
    // Wait briefly for the kernel to process SIGKILL so the caller can
    // safely reuse resources (ports, WAL files) without racing a
    // not-yet-reaped process.
    let kill_deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while std::time::Instant::now() < kill_deadline {
        if !process_is_alive(pid) {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    Ok(false)
}

/// Check if a process is alive (running or sleeping, not zombie or gone).
/// Uses `ps -p PID -o stat=` which returns empty for non-existent PIDs
/// and 'Z' for zombies.
#[must_use]
pub fn process_is_alive(pid: u32) -> bool {
    // Fast path: read /proc/{pid}/stat (Linux) — no subprocess spawn.
    // Falls back to `ps` on non-Linux platforms (macOS, etc.).
    #[cfg(target_os = "linux")]
    {
        if let Ok(stat) = std::fs::read_to_string(format!("/proc/{pid}/stat")) {
            // Field 3 (state) is after the comm field in parentheses.
            // The state char: 'R'=running, 'S'=sleeping, 'Z'=zombie, etc.
            // Zombie means the process has exited but not been reaped.
            let state = stat.rsplit(')').next().unwrap_or("").trim_start();
            let state_char = state.chars().next().unwrap_or('Z');
            return state_char != 'Z';
        }
        false
    }
    #[cfg(not(target_os = "linux"))]
    {
        let Ok(output) = std::process::Command::new("ps")
            .arg("-p")
            .arg(pid.to_string())
            .arg("-o")
            .arg("stat=")
            .output()
        else {
            return false;
        };
        let stat = String::from_utf8_lossy(&output.stdout).trim().to_string();
        !stat.is_empty() && !stat.starts_with('Z')
    }
}

/// Render the shell command that brings up `crowdb-kv-server` on the remote
/// host. Public so the SSH path can reuse it without duplicating arg
/// formatting.
#[must_use]
pub(crate) fn remote_start_command(req: &DeployRequest, server_bin: &str) -> String {
    // `nohup ... &` + redirected fds detaches the child from the SSH
    // channel; the trailing `echo $!` prints the pid we want to capture.
    // The node root is a per-port dir under /tmp; waldata/conf/ctdata/log
    // are derived subdirs.
    let config_arg = req
        .config
        .as_ref()
        .map_or_else(String::new, |c| format!(" --config {}", c.display()));
    format!(
        "nohup {bin}{config_arg} --root /tmp/crowdb-kv-server-{mp} --management-addr 127.0.0.1 --management-port {mp} --ports {gp} \
         >/tmp/crowdb-kv-server.{mp}.out 2>/tmp/crowdb-kv-server.{mp}.err </dev/null & echo $!",
        bin = server_bin,
        mp = req.rest_port,
        gp = req.rpc_port,
    )
}

async fn wait_for_ready(mgmt_url: &str, timeout: Duration) -> Result<()> {
    let client = ServerClient::new(mgmt_url.to_string())?;
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if client.health().await.is_ok() {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Err(Error::UpstreamRpc {
        node_id: mgmt_url.to_string(),
        status: "did not become healthy within timeout".into(),
    })
}

/// Poll `GET /health` until it returns HTTP 200, without requiring
/// the response body to match `HealthResponse`. Used for diskdb,
/// whose `/health` endpoint returns a different JSON shape
/// (`{"phase":"up","degraded":true,"ready":true}`).
async fn wait_for_http_ok(url: &str, timeout: Duration) -> Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .map_err(|e| Error::UpstreamRpc {
            node_id: url.to_string(),
            status: format!("client build failed: {e}"),
        })?;
    let health_url = format!("{url}/health");
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if let Ok(resp) = client.get(&health_url).send().await {
            if resp.status().is_success() {
                return Ok(());
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Err(Error::UpstreamRpc {
        node_id: url.to_string(),
        status: "did not become healthy within timeout".into(),
    })
}

/// Poll a server's `/topology` until `(store_id, group_id)` reports a
/// non-zero `leader_id`, meaning the per-group Paxos election driver has
/// elected a leader.
///
/// # Errors
/// Returns `Error::UpstreamRpc` if the timeout elapses without seeing a
/// leader.
pub async fn wait_for_leader(mgmt_url: &str, store_id: u64, group_id: u64, timeout: Duration) -> Result<()> {
    let client = ServerClient::new(mgmt_url.to_string())?;
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if let Ok(detail) = client.get_store(store_id).await {
            if detail
                .groups
                .iter()
                .find(|g| g.group_id == group_id)
                .is_some_and(|g| g.leader_id != 0)
            {
                return Ok(());
            }
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    Err(Error::UpstreamRpc {
        node_id: mgmt_url.to_string(),
        status: format!("group {group_id} in store {store_id} did not elect a leader within {timeout:?}"),
    })
}

fn spawn_log_pipe<R>(reader: R, tag: &'static str)
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        let mut lines = BufReader::new(reader).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            debug!(target = "crowdb_console_lifecycle", "{tag}: {line}");
        }
    });
}

fn stage_server_binary(binary: &std::path::Path, workspace_dir: &std::path::Path) -> Result<PathBuf> {
    use std::os::unix::fs::PermissionsExt;
    let source = resolve_binary_path(binary).ok_or_else(|| Error::Validation {
        field: "binary".into(),
        message: format!("could not resolve server binary path: {}", binary.display()),
    })?;
    let staged = workspace_dir.join("bin").join(
        source
            .file_name()
            .unwrap_or_else(|| std::ffi::OsStr::new("crowdb-kv-server")),
    );
    if staged.exists() {
        std::fs::remove_file(&staged).map_err(Error::Io)?;
    }
    if let Ok(()) = std::os::unix::fs::symlink(&source, &staged) {
        Ok(staged)
    } else {
        std::fs::copy(&source, &staged).map_err(Error::Io)?;
        let mut perms = std::fs::metadata(&staged).map_err(Error::Io)?.permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&staged, perms).map_err(Error::Io)?;
        Ok(staged)
    }
}

fn resolve_binary_path(binary: &std::path::Path) -> Option<PathBuf> {
    if binary.is_absolute() {
        return binary.exists().then(|| binary.to_path_buf());
    }
    if binary.components().count() > 1 {
        return std::fs::canonicalize(binary).ok().or_else(|| {
            std::env::current_dir().ok().and_then(|cwd| {
                let candidate = cwd.join(binary);
                candidate.exists().then_some(candidate)
            })
        });
    }
    find_in_path(binary.as_os_str())
}

/// Resolve the path to the `crowdb-kv-server` binary.
///
/// Search order:
/// 1. `$CROWDB_KV_SERVER_BIN`.
/// 2. A sibling named `crowdb-kv-server` next to the current executable
///    (covers `cargo run -p crowdb-console-cli`).
/// 3. `crowdb-kv-server` on `$PATH` (returned as a relative path so the OS
///    resolves it at exec time).
#[must_use]
pub fn crowdb_kv_server_bin() -> Option<PathBuf> {
    if let Ok(p) = std::env::var("CROWDB_KV_SERVER_BIN") {
        return Some(PathBuf::from(p));
    }
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            // dir is e.g. target/debug/. Walk up if we are deeper (deps/).
            let mut p = dir.to_path_buf();
            for _ in 0..3 {
                let candidate = p.join("crowdb-kv-server");
                if is_executable(&candidate) {
                    return Some(candidate);
                }
                if !p.pop() {
                    break;
                }
            }
        }
    }
    // Fall back to PATH.
    if let Some(path) = find_in_path(std::ffi::OsStr::new("crowdb-kv-server")) {
        return Some(path);
    }
    warn!("crowdb-kv-server binary not found via env, sibling, or $PATH");
    None
}

fn find_in_path(name: &std::ffi::OsStr) -> Option<PathBuf> {
    if let Ok(path) = std::env::var("PATH") {
        for dir in std::env::split_paths(&path) {
            let candidate = dir.join(name);
            if is_executable(&candidate) {
                return Some(candidate);
            }
        }
    }
    None
}

/// Check that a path is a non-empty executable. Skips 0-byte
/// placeholders left by interrupted builds, which would cause
/// `ENOEXEC` (os error 8) at spawn time.
fn is_executable(path: &std::path::Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    path.is_file()
        && path
            .metadata()
            .is_ok_and(|m| m.len() > 0 && m.permissions().mode() & 0o111 != 0)
}

// ── DiskDB deploy (R77) ───────────────────────────────────────────

/// Inputs for a diskdb deploy (R77). The binary and config file are
/// pre-copied to the node workspace (`<workspace>/bin/crowdb-diskdb`
/// and `<workspace>/conf/crowdb_diskdb_config.toml`); the deploy only
/// needs the crowdb-rpc port to override `--listen-addr`. The HTTP port is
/// read from the config file for readiness checking.
#[derive(Debug, Clone, Default)]
pub struct DiskdbDeployRequest {
    pub server_id: String,
    pub rpc_port: u16,
    /// KV-server management URLs for group-0 discovery. If non-empty,
    /// the auto-generated config uses these instead of the default port.
    pub kv_server_mgmt_seeds: Vec<String>,
}

/// Resolve the path to the `crowdb-diskdb` binary.
///
/// Search order:
/// 1. `$CROWDB_DISKDB_BIN`.
/// 2. A sibling named `crowdb-diskdb` next to the current executable.
/// 3. `crowdb-diskdb` on `$PATH`.
#[must_use]
pub fn crowdb_diskdb_bin() -> Option<PathBuf> {
    if let Ok(p) = std::env::var("CROWDB_DISKDB_BIN") {
        return Some(PathBuf::from(p));
    }
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            let mut p = dir.to_path_buf();
            for _ in 0..3 {
                let candidate = p.join("crowdb-diskdb");
                if candidate.exists() {
                    return Some(candidate);
                }
                if !p.pop() {
                    break;
                }
            }
        }
    }
    if let Some(path) = find_in_path(std::ffi::OsStr::new("crowdb-diskdb")) {
        return Some(path);
    }
    warn!("crowdb-diskdb binary not found via env, sibling, or $PATH");
    None
}

/// Locate the standalone `crowdb-rpc-fb-server` C++ binary (built via
/// `pixi run build-cpp`). Search order: `$CROWDB_RPC_FB_SERVER_BIN`,
/// `lib/crowdb-rpc/build/crowdb-rpc-fb-server` relative to the repo
/// root (walked up from the current exe), then `$PATH`.
pub fn crowdb_rpc_fb_server_bin() -> Option<PathBuf> {
    if let Ok(p) = std::env::var("CROWDB_RPC_FB_SERVER_BIN") {
        return Some(PathBuf::from(p));
    }
    // The fb-server is a CMake target under lib/crowdb-rpc/build/.
    // Walk up from the exe dir to find the repo root, then check the
    // expected build path.
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            let mut p = dir.to_path_buf();
            for _ in 0..5 {
                let candidate = p.join("lib/crowdb-rpc/build/crowdb-rpc-fb-server");
                if is_executable(&candidate) {
                    return Some(candidate);
                }
                if !p.pop() {
                    break;
                }
            }
        }
    }
    if let Some(path) = find_in_path(std::ffi::OsStr::new("crowdb-rpc-fb-server")) {
        return Some(path);
    }
    warn!("crowdb-rpc-fb-server binary not found via env, repo build, or $PATH");
    None
}

/// Resolve the `--config` path for a diskdb deploy. Uses the
/// pre-copied config at `<workspace>/conf/crowdb_diskdb_config.toml`.
/// Falls back to a minimal auto-generated config with all required
/// sections if the file is missing. The auto-generated config sets
/// `http_listen_addr` to `0.0.0.0:{rpc_port + 1}` so each instance
/// gets a unique HTTP port. When `kv_server_mgmt_seeds` is non-empty,
/// the config is always (re)written so the diskdb can discover group-0
/// on the actual kv-server management port.
fn resolve_diskdb_config_path(
    workspace_dir: &std::path::Path,
    rpc_port: u16,
    kv_server_mgmt_seeds: &[String],
) -> Result<PathBuf> {
    let conf = workspace_dir.join("conf");
    std::fs::create_dir_all(&conf).map_err(Error::Io)?;
    let path = conf.join("crowdb_diskdb_config.toml");
    if path.exists() && kv_server_mgmt_seeds.is_empty() {
        return Ok(path);
    }
    let http_port = rpc_port.saturating_add(1);
    let seeds = if kv_server_mgmt_seeds.is_empty() {
        format!("\"http://127.0.0.1:{}\"", crowdb_protocol::KV_SERVER_MGMT_BASE)
    } else {
        kv_server_mgmt_seeds
            .iter()
            .map(|s| format!("\"{s}\""))
            .collect::<Vec<_>>()
            .join(", ")
    };
    // Minimal valid config — only [server] is required; all other
    // sections default via `#[serde(default)]` on `DdbConfig` fields
    // (values match `DdbConfig::default()`).
    let rpc_listen_port = rpc_port.saturating_add(2);
    let config = format!(
        "[server]\n\
         listen_addr = \"0.0.0.0:{rpc_port}\"\n\
         http_listen_addr = \"0.0.0.0:{http_port}\"\n\
         rpc_listen_addr = \"0.0.0.0:{rpc_listen_port}\"\n\
         kv_server_mgmt_seeds = [{seeds}]\n",
    );
    std::fs::write(&path, config).map_err(Error::Io)?;
    Ok(path)
}

/// Minimal shape for extracting `server.http_listen_addr` from a
/// diskdb config TOML file.
#[derive(serde::Deserialize)]
struct DiskdbConfigHttpAddr {
    server: Option<DiskdbConfigServerSection>,
}

#[derive(serde::Deserialize)]
struct DiskdbConfigServerSection {
    http_listen_addr: Option<String>,
}

/// Extract the HTTP listen address from a diskdb config TOML file.
/// Returns `None` if the file cannot be parsed or the field is absent.
fn http_listen_addr_from_config(config_path: &std::path::Path) -> Option<String> {
    let content = match std::fs::read_to_string(config_path) {
        Ok(c) => c,
        Err(e) => {
            warn!("diskdb config read failed {config_path:?}: {e}");
            return None;
        }
    };
    let parsed: DiskdbConfigHttpAddr = match toml::from_str(&content) {
        Ok(p) => p,
        Err(e) => {
            warn!("diskdb config parse failed {config_path:?}: {e}");
            return None;
        }
    };
    parsed.server?.http_listen_addr
}

/// Spawn `crowdb-diskdb` locally. The binary and config file are
/// pre-copied to the node workspace (`<workspace>/bin/crowdb-diskdb`
/// and `<workspace>/conf/crowdb_diskdb_config.toml`). The deploy only
/// overrides `--listen-addr` with the crowdb-rpc port; the HTTP port comes
/// from the config file.
///
/// # Errors
/// Returns `Error::Validation` for bad inputs and `Error::Io` for
/// spawn or readiness failures.
pub async fn deploy_diskdb_local(
    req: &DiskdbDeployRequest,
    node: &NodeEntry,
    workspace_dir: &std::path::Path,
) -> Result<DeployedServer> {
    if req.rpc_port == 0 {
        return Err(Error::Validation {
            field: "port".into(),
            message: "rpc_port must be non-zero".into(),
        });
    }

    // Use the pre-copied binary in the workspace bin/ dir, falling
    // back to a PATH/env search if not yet staged.
    let staged = workspace_dir.join("bin").join("crowdb-diskdb");
    let launch_binary = if staged.exists() {
        staged
    } else {
        let binary = crowdb_diskdb_bin().ok_or_else(|| Error::Validation {
            field: "binary".into(),
            message: "could not locate crowdb-diskdb binary; set $CROWDB_DISKDB_BIN".into(),
        })?;
        stage_server_binary(&binary, workspace_dir)?
    };

    let config_path = resolve_diskdb_config_path(workspace_dir, req.rpc_port, &req.kv_server_mgmt_seeds)?;
    let rpc_url = format!("http://{}:{}", node.host, req.rpc_port);

    // Read the HTTP listen address from the config file for readiness
    // checking. If absent, the diskdb binary uses its default HTTP
    // port (DISKDB_HTTP_BASE = 9942). Replace 0.0.0.0 with the node
    // host so the readiness check can actually connect.
    let http_addr = http_listen_addr_from_config(&config_path);
    let mgmt_url = match &http_addr {
        Some(addr) if addr.starts_with("http") => addr.replace("0.0.0.0", &node.host),
        Some(addr) => format!("http://{}", addr.replace("0.0.0.0", &node.host)),
        None => format!("http://{}:{}", node.host, crowdb_protocol::DISKDB_HTTP_BASE),
    };

    let mut cmd = Command::new(&launch_binary);
    cmd.arg("--config").arg(&config_path);
    cmd.arg("--listen-addr")
        .arg(format!("{}:{}", node.host, req.rpc_port));
    cmd.kill_on_drop(false);
    let log_dir = workspace_dir.join("log");
    let tmp_path = log_dir.join("crowdb-diskdb.stdout.log");
    let out = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&tmp_path)
        .map_err(Error::Io)?;
    cmd.current_dir(workspace_dir);
    cmd.stdout(Stdio::from(out.try_clone().map_err(Error::Io)?));
    cmd.stderr(Stdio::from(out));
    let child = cmd.spawn().map_err(Error::Io)?;
    let pid = child.id().ok_or_else(|| Error::Validation {
        field: "pid".into(),
        message: "spawned child has no pid".into(),
    })?;
    let from = log_dir.join("crowdb-diskdb.stdout.log");
    let to = log_dir.join(format!("crowdb-diskdb-{pid}.out.log"));
    let _ = std::fs::rename(&from, &to);
    // Detach: drop the Child handle so the process is not killed.
    std::mem::forget(child);
    if wait_for_http_ok(&mgmt_url, Duration::from_secs(30))
        .await
        .is_err()
    {
        // Include the last log lines for diagnostics — the process may
        // have crashed on startup (missing dep, config error, etc).
        let log_tail = std::fs::read_to_string(&to)
            .unwrap_or_default()
            .lines()
            .rev()
            .take(20)
            .collect::<Vec<_>>()
            .join("\n");
        let alive = process_is_alive(pid);
        return Err(Error::UpstreamRpc {
            node_id: mgmt_url.clone(),
            status: format!(
                "did not become healthy within 30s (pid={pid}, alive={alive})\n--- log tail ---\n{log_tail}"
            ),
        });
    }
    Ok(DeployedServer {
        server_id: req.server_id.clone(),
        mgmt_url,
        rpc_url,
        pid,
    })
}
