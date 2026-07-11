use std::io as std_io;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

pub struct ServerHandle {
    child: Child,
    base_url: String,
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
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        Err(std_io::Error::new(std_io::ErrorKind::TimedOut, "server was not ready before timeout"))
    }
}

impl Drop for ServerHandle {
    fn drop(&mut self) {
        let pid = self.child.id();
        let _ = std::process::Command::new("kill").arg("-TERM").arg(pid.to_string()).status();
        let start = Instant::now();
        loop {
            match self.child.try_wait() {
                Ok(Some(_)) | Err(_) => break,
                Ok(None) => {
                    if start.elapsed() >= Duration::from_secs(5) {
                        let _ = std::process::Command::new("kill").arg("-KILL").arg(pid.to_string()).status();
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(50));
                }
            }
        }
    }
}

pub async fn start_test_server(args: &[&str]) -> std_io::Result<ServerHandle> {
    use std::time::{SystemTime, UNIX_EPOCH};

    // Generate random port in range 20000-30000
    let seed = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().subsec_nanos();
    let port = 20000 + (seed % 10000) as u16;

    let bin = crowkv_server_bin();
    let mut child = Command::new(bin)
        .args(args)
        .arg("--management-addr")
        .arg("127.0.0.1")
        .arg("--management-port")
        .arg(port.to_string())
        .arg("--ports")
        .arg("0")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;

    // Use a thread to read stdout and find the bound management address
    let stdout = child.stdout.take().expect("stdout should be captured");
    let (tx, rx) = mpsc::channel();

    thread::spawn(move || {
        use std::io::{BufRead, BufReader};
        let reader = BufReader::new(stdout);
        for line in reader.lines().map_while(Result::ok) {
            if line.contains("management_addr=") {
                if let Some(idx) = line.find("management_addr=") {
                    let after = &line[idx + "management_addr=".len()..];
                    let _ = tx.send(after.trim().to_string());
                    break;
                }
            }
        }
    });

    // Wait for address from thread (with timeout)
    let deadline = Instant::now() + Duration::from_secs(10);
    #[allow(clippy::never_loop)]
    let addr = loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        match rx.recv_timeout(remaining) {
            Ok(addr) => break addr,
            Err(mpsc::RecvTimeoutError::Timeout) => {
                return Err(std_io::Error::new(std_io::ErrorKind::TimedOut, "management_addr was not found in stdout before timeout"));
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                return Err(std_io::Error::new(std_io::ErrorKind::BrokenPipe, "stdout reader thread disconnected"));
            }
        }
    };

    let handle = ServerHandle {
        child,
        base_url: format!("http://{addr}"),
    };
    handle.wait_for_ready(Duration::from_secs(10)).await?;
    Ok(handle)
}

fn crowkv_server_bin() -> PathBuf {
    if let Some(path) = option_env!("CARGO_BIN_EXE_crowkv-server") {
        return PathBuf::from(path);
    }

    // Walk up from test executable (target/debug/deps/test-name) to target/debug/
    let mut path = std::env::current_exe().expect("current test executable path");
    while path.file_name().is_some_and(|name| name != "debug" && name != "release") {
        path.pop();
    }
    // Now at target/debug/ or target/release/, push the binary name
    path.push("crowkv-server");
    path
}
