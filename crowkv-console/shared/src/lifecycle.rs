//! Server-instance lifecycle (deploy / start / stop).
//!
//! C3 status: **local-spawn placeholder**. `deploy_local` runs
//! `tokio::process::Command` against the `crowkv-server` binary on the
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
#[derive(Debug, Clone)]
pub struct DeployRequest {
    pub server_id: String,
    pub mgmt_port: u16,
    pub grpc_port: u16,
    /// Optional override of the binary path. Defaults via
    /// `crowkv_server_bin()` resolution: `$CROWKV_SERVER_BIN` →
    /// `$PATH` → `target/{debug,release}/crowkv-server` next to the
    /// current executable.
    pub binary: Option<PathBuf>,
    pub election_profile: Option<String>,
}

/// Result of a successful deploy. Persist these fields onto the
/// `ServerEntry` so `stop` can locate the process later.
#[derive(Debug, Clone)]
pub struct DeployedServer {
    pub server_id: String,
    pub mgmt_url: String,
    pub grpc_url: String,
    pub pid: u32,
}

/// Spawn `crowkv-server` locally. The `node.host` is folded into the
/// returned URLs so the rest of the console can address the instance
/// uniformly with the SSH path coming in C4.
///
/// # Errors
/// Returns `Error::Validation` for bad inputs and `Error::Io` for spawn
/// or readiness failures.
pub async fn deploy_local(req: &DeployRequest, node: &NodeEntry) -> Result<DeployedServer> {
    deploy_local_in_workspace(req, node, None).await
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
    deploy_local_in_workspace(req, node, Some(workspace_dir)).await
}

async fn deploy_local_in_workspace(
    req: &DeployRequest,
    node: &NodeEntry,
    workspace_dir: Option<&std::path::Path>,
) -> Result<DeployedServer> {
    if req.mgmt_port == 0 || req.grpc_port == 0 {
        return Err(Error::Validation {
            field: "port".into(),
            message: "mgmt_port and grpc_port must be non-zero".into(),
        });
    }
    if req.mgmt_port == req.grpc_port {
        return Err(Error::Validation {
            field: "port".into(),
            message: "mgmt_port and grpc_port must differ".into(),
        });
    }

    let binary = match &req.binary {
        Some(p) => p.clone(),
        None => crowkv_server_bin().ok_or_else(|| Error::Validation {
            field: "binary".into(),
            message: "could not locate crowkv-server binary; set $CROWKV_SERVER_BIN".into(),
        })?,
    };
    let launch_binary = if let Some(dir) = workspace_dir {
        stage_server_binary(&binary, dir)?
    } else {
        binary.clone()
    };

    let mgmt_url = format!("http://{}:{}", node.host, req.mgmt_port);
    let grpc_url = format!("http://{}:{}", node.host, req.grpc_port);

    let mut cmd = Command::new(&launch_binary);
    cmd.arg("--management-addr")
        .arg("127.0.0.1")
        .arg("--management-port")
        .arg(req.mgmt_port.to_string())
        .arg("--election-profile")
        .arg(
            req.election_profile
                .as_deref()
                .map(str::to_owned)
                .or_else(|| std::env::var("CROWKV_SERVER_ELECTION_PROFILE").ok())
                .unwrap_or_else(|| "default".into()),
        )
        .kill_on_drop(false);
    if let Some(dir) = workspace_dir {
        let stdout = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(dir.join("log").join("crowkv-server.stdout.log"))
            .map_err(Error::Io)?;
        let stderr = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(dir.join("log").join("crowkv-server.stderr.log"))
            .map_err(Error::Io)?;
        cmd.current_dir(dir);
        cmd.stdout(Stdio::from(stdout));
        cmd.stderr(Stdio::from(stderr));
    } else {
        cmd.stdout(Stdio::piped());
        cmd.stderr(Stdio::piped());
    }
    let mut child = cmd.spawn().map_err(Error::Io)?;

    let pid = child.id().ok_or_else(|| Error::Validation {
        field: "pid".into(),
        message: "spawned child has no pid".into(),
    })?;

    // Drain stdout/stderr to a debug logger so the child doesn't block on
    // a full pipe. We deliberately don't wait for "management_addr=" here:
    // the user supplied the port, so we know mgmt_url; readiness is
    // confirmed by polling /health.
    if workspace_dir.is_none() {
        if let Some(stdout) = child.stdout.take() {
            spawn_log_pipe(stdout, "crowkv-server stdout");
        }
        if let Some(stderr) = child.stderr.take() {
            spawn_log_pipe(stderr, "crowkv-server stderr");
        }
    }

    // Detach: drop the Child handle so the process is not killed when
    // this function returns. The pid is the user's tracking handle.
    std::mem::forget(child);

    wait_for_ready(&mgmt_url, Duration::from_secs(10)).await?;

    Ok(DeployedServer {
        server_id: req.server_id.clone(),
        mgmt_url,
        grpc_url,
        pid,
    })
}

/// Send SIGTERM to a tracked pid on the **local** host. Returns
/// `Ok(false)` if the pid is already gone. Implemented by shelling out
/// to `/bin/kill`, matching `crowkv-server/tests/testkit/process.rs` so
/// that both paths behave identically and the workspace
/// `unsafe_code = deny` lint is kept.
///
/// For SSH-deployed servers, use `crowkv_ssh` to run the
/// equivalent command on the remote host.
///
/// # Errors
/// Surfaces spawn / wait failures as `Error::Io`.
pub fn stop_pid(pid: u32) -> Result<bool> {
    let status = std::process::Command::new("kill")
        .arg("-TERM")
        .arg(pid.to_string())
        .status()
        .map_err(Error::Io)?;
    // `kill` exits non-zero when the pid is already gone. Treat that as
    // "not alive" rather than an error.
    Ok(status.success())
}

/// Render the shell command that brings up `crowkv-server` on the remote
/// host. Public so the SSH path can reuse it without duplicating arg
/// formatting.
#[must_use]
pub fn remote_start_command(req: &DeployRequest, server_bin: &str) -> String {
    // `nohup ... &` + redirected fds detaches the child from the SSH
    // channel; the trailing `echo $!` prints the pid we want to capture.
    format!(
        "nohup {bin} --management-addr 127.0.0.1 --management-port {mp} --ports {gp} \
         >/tmp/crowkv-server.{mp}.out 2>/tmp/crowkv-server.{mp}.err </dev/null & echo $!",
        bin = server_bin,
        mp = req.mgmt_port,
        gp = req.grpc_port,
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
            debug!(target = "crowkv_console_lifecycle", "{tag}: {line}");
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
            .unwrap_or_else(|| std::ffi::OsStr::new("crowkv-server")),
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

/// Resolve the path to the `crowkv-server` binary.
///
/// Search order:
/// 1. `$CROWKV_SERVER_BIN`.
/// 2. A sibling named `crowkv-server` next to the current executable
///    (covers `cargo run -p crowkv-console-cli`).
/// 3. `crowkv-server` on `$PATH` (returned as a relative path so the OS
///    resolves it at exec time).
#[must_use]
pub fn crowkv_server_bin() -> Option<PathBuf> {
    if let Ok(p) = std::env::var("CROWKV_SERVER_BIN") {
        return Some(PathBuf::from(p));
    }
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            // dir is e.g. target/debug/. Walk up if we are deeper (deps/).
            let mut p = dir.to_path_buf();
            for _ in 0..3 {
                let candidate = p.join("crowkv-server");
                if candidate.exists() {
                    return Some(candidate);
                }
                if !p.pop() {
                    break;
                }
            }
        }
    }
    // Fall back to PATH.
    if let Some(path) = find_in_path(std::ffi::OsStr::new("crowkv-server")) {
        return Some(path);
    }
    warn!("crowkv-server binary not found via env, sibling, or $PATH");
    None
}

fn find_in_path(name: &std::ffi::OsStr) -> Option<PathBuf> {
    if let Ok(path) = std::env::var("PATH") {
        for dir in std::env::split_paths(&path) {
            let candidate = dir.join(name);
            if candidate.is_file() {
                return Some(candidate);
            }
        }
    }
    None
}
