// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

use std::io as std_io;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

pub struct ServerHandle {
    child: Child,
    base_url: String,
    _wal_dir: tempfile::TempDir,
    _config_dir: tempfile::TempDir,
}

impl ServerHandle {
    #[must_use]
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    pub async fn wait_for_ready(&self, timeout: Duration) -> std_io::Result<()> {
        let client = reqwest::Client::new();
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if let Ok(resp) = client.get(format!("{}/health", self.base_url)).send().await {
                if resp.status().is_success() || resp.status().as_u16() == 503 {
                    return Ok(());
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Err(std_io::Error::new(
            std_io::ErrorKind::TimedOut,
            "server was not ready before timeout",
        ))
    }
}

impl Drop for ServerHandle {
    fn drop(&mut self) {
        let pid = self.child.id();
        let _ = std::process::Command::new("kill")
            .arg("-TERM")
            .arg(pid.to_string())
            .status();
        let start = Instant::now();
        loop {
            match self.child.try_wait() {
                Ok(Some(_)) | Err(_) => break,
                Ok(None) => {
                    if start.elapsed() >= Duration::from_secs(1) {
                        let _ = std::process::Command::new("kill")
                            .arg("-KILL")
                            .arg(pid.to_string())
                            .status();
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(10));
                }
            }
        }
    }
}

#[allow(dead_code)]
pub async fn start_test_server(args: &[&str]) -> std_io::Result<ServerHandle> {
    start_test_server_with_ports(args, &[0]).await
}

/// Like [`start_test_server`] but lets the caller supply one port per store
/// (e.g. `&[0, 0]` for a two-store process). Each entry maps to a store in
/// the order given by `--stores`; `0` lets the OS assign a port.
pub async fn start_test_server_with_ports(args: &[&str], ports: &[u16]) -> std_io::Result<ServerHandle> {
    let wal_dir = tempfile::tempdir()?;
    let wal_root = wal_dir.path().join("wal");

    // Write a minimal TOML config so --config is satisfied.
    let config_dir = tempfile::tempdir()?;
    let config_path = config_dir.path().join("crow_kv_server_config.toml");
    std::fs::write(&config_path, "# test config\n")?;

    let ports_str = ports.iter().map(u16::to_string).collect::<Vec<_>>().join(",");

    // `parse_id_list` dedupes via a HashSet, so `0,0` collapses to a single
    // port and fails the `--ports`/`--stores` length check. When every port is
    // 0, omit `--ports` entirely — the server defaults to one OS-assigned
    // (port 0) slot per store, which is what the caller asked for.
    let all_zero = ports.iter().all(|&p| p == 0);

    let bin = crow_kv_server_bin();
    let mut cmd = Command::new(bin);
    cmd.args(args)
        .arg("--config")
        .arg(&config_path)
        .arg("--management-addr")
        .arg("127.0.0.1")
        .arg("--management-port")
        .arg("0");
    if !all_zero {
        cmd.arg("--ports").arg(&ports_str);
    }
    cmd.arg("--election-profile")
        .arg("e2e")
        .arg("--wal-root")
        .arg(&wal_root)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = cmd.spawn()?;

    let stdout = child.stdout.take().expect("stdout should be captured");
    let stderr = child.stderr.take().expect("stderr should be captured");
    let (tx, rx) = mpsc::channel();
    let stderr_buf = Arc::new(Mutex::new(Vec::<String>::new()));
    let stderr_buf_clone = Arc::clone(&stderr_buf);

    thread::spawn(move || {
        use std::io::{BufRead, BufReader};
        let reader = BufReader::new(stdout);
        for line in reader.lines() {
            if let Ok(l) = line {
                if l.contains("management_addr=") {
                    if let Some(idx) = l.find("management_addr=") {
                        let after = &l[idx + "management_addr=".len()..];
                        let _ = tx.send(after.trim().to_string());
                        break;
                    }
                }
            } else {
                // Stdio error - process likely exited early, stop reading
                break;
            }
        }
    });

    thread::spawn(move || {
        use std::io::{BufRead, BufReader};
        let reader = BufReader::new(stderr);
        for line in reader.lines().map_while(Result::ok) {
            stderr_buf_clone.lock().unwrap().push(line);
        }
    });

    let deadline = Instant::now() + Duration::from_secs(10);
    #[allow(clippy::never_loop)]
    let addr = loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        match rx.recv_timeout(remaining) {
            Ok(addr) => break addr,
            Err(mpsc::RecvTimeoutError::Timeout) => {
                return Err(std_io::Error::new(
                    std_io::ErrorKind::TimedOut,
                    "management_addr was not found in stdout before timeout",
                ));
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                let _ = child.wait();
                let stderr_lines = stderr_buf.lock().unwrap();
                let msg = if stderr_lines.is_empty() {
                    "stdout reader thread disconnected (process exited early)".to_string()
                } else {
                    format!(
                        "stdout reader thread disconnected; stderr:\n{}",
                        stderr_lines.join("\n")
                    )
                };
                return Err(std_io::Error::new(std_io::ErrorKind::BrokenPipe, msg));
            }
        }
    };

    let handle = ServerHandle {
        child,
        base_url: format!("http://{addr}"),
        _wal_dir: wal_dir,
        _config_dir: config_dir,
    };
    handle.wait_for_ready(Duration::from_secs(10)).await?;
    Ok(handle)
}

fn crow_kv_server_bin() -> PathBuf {
    if let Some(path) = option_env!("CARGO_BIN_EXE_crow-kv-server") {
        return PathBuf::from(path);
    }

    // Walk up from test executable (target/debug/deps/test-name) to target/debug/
    let mut path = std::env::current_exe().expect("current test executable path");
    while path
        .file_name()
        .is_some_and(|name| name != "debug" && name != "release")
    {
        path.pop();
    }
    // Now at target/debug/ or target/release/, push the binary name
    path.push("crow-kv-server");
    path
}
