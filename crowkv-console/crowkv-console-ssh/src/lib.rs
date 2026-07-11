//! SSH transport for the `CrowKV` Console.
//!
//! Wraps `russh` 0.45 in a small synchronous-feeling helper used by the
//! console lifecycle path: open a session, run a command, capture
//! stdout/stderr/exit. C4 status: minimal client side; key auth via
//! explicit `ssh_key` path or `~/.ssh/id_ed25519` / `~/.ssh/id_rsa`
//! defaults; password fallback. No persistent `known_hosts` (TOFU accept).

#![cfg_attr(not(test), allow(dead_code))]

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use crowkv_console_core::config::NodeEntry;
use crowkv_console_core::error::{Error, Result};
use russh::client::{self, Handle};
use russh::keys::key::{KeyPair, PublicKey};
use russh::keys::load_secret_key;
use russh::ChannelMsg;
use tracing::debug;

/// Authentication strategy resolved from a `NodeEntry`.
#[derive(Debug, Clone)]
pub enum SshCreds {
    /// PEM private key file path.
    KeyPath(PathBuf),
    /// Plaintext password.
    Password(String),
}

impl SshCreds {
    /// Resolve creds from a node entry. Priority:
    /// 1. Explicit `ssh_key` path on the node.
    /// 2. `ssh_password` on the node.
    /// 3. `~/.ssh/id_ed25519` then `~/.ssh/id_rsa` if either exists.
    ///
    /// # Errors
    /// `Error::Validation` if the node has no usable creds.
    pub fn resolve(node: &NodeEntry) -> Result<Self> {
        if let Some(p) = &node.ssh_key {
            return Ok(Self::KeyPath(PathBuf::from(p)));
        }
        if let Some(pw) = &node.ssh_password {
            return Ok(Self::Password(pw.clone()));
        }
        if let Some(home) = dirs::home_dir() {
            for name in ["id_ed25519", "id_rsa"] {
                let candidate = home.join(".ssh").join(name);
                if candidate.exists() {
                    return Ok(Self::KeyPath(candidate));
                }
            }
        }
        Err(Error::Validation {
            field: "ssh".into(),
            message: format!("no ssh creds found for node {} (set ssh_key or ssh_password)", node.id),
        })
    }
}

/// Result of `Session::exec`.
#[derive(Debug, Clone)]
pub struct ExecOutput {
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub exit: Option<u32>,
}

impl ExecOutput {
    /// `true` if the remote command exited zero.
    #[must_use]
    pub fn success(&self) -> bool {
        self.exit == Some(0)
    }

    #[must_use]
    pub fn stdout_str(&self) -> std::borrow::Cow<'_, str> {
        String::from_utf8_lossy(&self.stdout)
    }

    #[must_use]
    pub fn stderr_str(&self) -> std::borrow::Cow<'_, str> {
        String::from_utf8_lossy(&self.stderr)
    }
}

/// A connected SSH session. Cheap to keep alive; not pooled in C4.
pub struct Session {
    handle: Handle<ClientHandler>,
}

impl Session {
    /// Connect and authenticate.
    ///
    /// # Errors
    /// Returns `Error::ServerRpc` for any russh / auth failure, with the
    /// `host:port` as the synthetic server id.
    pub async fn connect(node: &NodeEntry, creds: &SshCreds) -> Result<Self> {
        if node.ssh_user.is_empty() {
            return Err(Error::Validation {
                field: "ssh_user".into(),
                message: format!("node {} has no ssh_user", node.id),
            });
        }
        let addr = (node.host.as_str(), node.ssh_port);
        let id = format!("{}:{}", node.host, node.ssh_port);

        let config = Arc::new(client::Config {
            inactivity_timeout: Some(Duration::from_secs(60)),
            ..Default::default()
        });

        let mut handle = client::connect(config, addr, ClientHandler).await.map_err(|e| ssh_err(&id, &e))?;

        let authed = match creds {
            SshCreds::KeyPath(path) => {
                let key: KeyPair = load_secret_key(path, None).map_err(|e| Error::Validation {
                    field: "ssh_key".into(),
                    message: format!("load {}: {e}", path.display()),
                })?;
                handle.authenticate_publickey(node.ssh_user.clone(), Arc::new(key)).await.map_err(|e| ssh_err(&id, &e))?
            }
            SshCreds::Password(pw) => handle.authenticate_password(node.ssh_user.clone(), pw.clone()).await.map_err(|e| ssh_err(&id, &e))?,
        };

        if !authed {
            return Err(Error::ServerRpc {
                server_id: id,
                status: "ssh authentication failed".into(),
            });
        }

        Ok(Self { handle })
    }

    /// Run `command` on the remote host and return captured output.
    ///
    /// # Errors
    /// `Error::ServerRpc` for channel / exec / IO failures.
    pub async fn exec(&mut self, command: &str) -> Result<ExecOutput> {
        let id = "<ssh>".to_string();
        let mut channel = self.handle.channel_open_session().await.map_err(|e| ssh_err(&id, &e))?;
        channel.exec(true, command).await.map_err(|e| ssh_err(&id, &e))?;

        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let mut exit = None;

        while let Some(msg) = channel.wait().await {
            match msg {
                ChannelMsg::Data { ref data } => stdout.extend_from_slice(data),
                ChannelMsg::ExtendedData { ref data, ext } => {
                    // ext == 1 is stderr per RFC 4254 §5.2.
                    if ext == 1 {
                        stderr.extend_from_slice(data);
                    } else {
                        debug!("ssh: ignoring ExtendedData ext={ext}, len={}", data.len());
                    }
                }
                ChannelMsg::ExitStatus { exit_status } => exit = Some(exit_status),
                _ => {}
            }
        }

        Ok(ExecOutput { stdout, stderr, exit })
    }

    /// Send a clean disconnect. Best-effort — errors are ignored.
    pub async fn close(self) {
        let _ = self.handle.disconnect(russh::Disconnect::ByApplication, "", "").await;
    }
}

/// Run a no-op command (`echo crowkv-ping`) and assert the round trip.
///
/// # Errors
/// `Error::ServerRpc` on connect, auth, or exec failure; or if the
/// remote echoed an unexpected payload.
pub async fn probe(node: &NodeEntry) -> Result<()> {
    let creds = SshCreds::resolve(node)?;
    let mut session = Session::connect(node, &creds).await?;
    let out = session.exec("echo crowkv-ping").await?;
    session.close().await;

    if !out.success() {
        return Err(Error::ServerRpc {
            server_id: node.host.clone(),
            status: format!("probe exit={:?}, stderr={}", out.exit, out.stderr_str()),
        });
    }
    if !out.stdout_str().contains("crowkv-ping") {
        return Err(Error::ServerRpc {
            server_id: node.host.clone(),
            status: format!("probe got unexpected stdout: {}", out.stdout_str()),
        });
    }
    Ok(())
}

fn ssh_err(id: &str, e: &impl std::fmt::Display) -> Error {
    Error::ServerRpc {
        server_id: id.to_string(),
        status: format!("ssh: {e}"),
    }
}

struct ClientHandler;

#[async_trait::async_trait]
impl client::Handler for ClientHandler {
    type Error = russh::Error;

    async fn check_server_key(&mut self, _server_public_key: &PublicKey) -> std::result::Result<bool, Self::Error> {
        // C4: TOFU accept. C8 hand-off: persist `~/.crowkv/known_hosts`.
        Ok(true)
    }
}

/// Convenience: resolve creds and run [`Session::exec`] in one call. Use
/// this from lifecycle helpers that don't need to keep the session open.
///
/// # Errors
/// Forwarded from `Session::connect` / `Session::exec`.
pub async fn run_remote(node: &NodeEntry, command: &str) -> Result<ExecOutput> {
    let creds = SshCreds::resolve(node)?;
    let mut session = Session::connect(node, &creds).await?;
    let out = session.exec(command).await?;
    session.close().await;
    Ok(out)
}

/// SSH-driven deploy: log into `node`, exec the start command from
/// `crowkv_console_core::lifecycle::remote_start_command`, capture the
/// printed pid, then wait for `/health` to come up.
///
/// # Errors
/// Surfaces SSH or readiness failures.
pub async fn deploy_via_ssh(req: &crowkv_console_core::lifecycle::DeployRequest, node: &NodeEntry, server_bin: &str) -> Result<crowkv_console_core::lifecycle::DeployedServer> {
    use crowkv_console_core::clients::http::ServerClient;
    use std::time::Instant;
    use tokio::time::sleep;

    let cmd = crowkv_console_core::lifecycle::remote_start_command(req, server_bin);
    let out = run_remote(node, &cmd).await?;
    if !out.success() {
        return Err(Error::ServerRpc {
            server_id: node.host.clone(),
            status: format!("remote start failed: exit={:?} stderr={}", out.exit, out.stderr_str()),
        });
    }
    let pid: u32 = out.stdout_str().trim().lines().last().unwrap_or_default().parse().map_err(|e| Error::ServerRpc {
        server_id: node.host.clone(),
        status: format!("could not parse remote pid from stdout {:?}: {e}", out.stdout_str()),
    })?;

    // Poll /health up to 10s, like the local-fork path.
    let mgmt_url = format!("http://{}:{}", node.host, req.mgmt_port);
    let grpc_url = format!("http://{}:{}", node.host, req.grpc_port);
    let client = ServerClient::new(mgmt_url.clone())?;
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if client.health().await.is_ok() {
            return Ok(crowkv_console_core::lifecycle::DeployedServer {
                server_id: req.server_id.clone(),
                mgmt_url,
                grpc_url,
                pid,
            });
        }
        sleep(Duration::from_millis(100)).await;
    }
    Err(Error::ServerRpc {
        server_id: mgmt_url,
        status: "remote server did not become healthy within timeout".into(),
    })
}

/// SSH-driven stop: send SIGTERM to a recorded pid on `node`.
///
/// # Errors
/// SSH/exec failures. A non-existent pid yields `Ok(false)`.
pub async fn stop_via_ssh(node: &NodeEntry, pid: u32) -> Result<bool> {
    let out = run_remote(node, &format!("kill -TERM {pid}")).await?;
    Ok(out.success())
}

/// Default SSH key path used when [`SshCreds::resolve`] would walk
/// `~/.ssh/`. Exposed for unit tests.
#[must_use]
pub fn default_key_candidates() -> Vec<PathBuf> {
    let mut v = Vec::new();
    if let Some(home) = dirs::home_dir() {
        v.push(home.join(".ssh").join("id_ed25519"));
        v.push(home.join(".ssh").join("id_rsa"));
    }
    v
}

/// Parse just the SSH-related fields of a `NodeEntry` from a TOML
/// snippet. Helper for tests that don't want to re-derive the whole
/// config struct.
///
/// # Errors
/// Returns `Error::Config` on parse failure.
pub fn parse_node_toml(toml_str: &str) -> Result<NodeEntry> {
    toml::from_str(toml_str).map_err(|e| Error::Config(format!("parse: {e}")))
}

#[cfg(test)]
mod tests {
    use super::{parse_node_toml, SshCreds};
    use std::path::Path;

    fn node(host: &str, user: &str, key: Option<&str>, password: Option<&str>) -> crowkv_console_core::config::NodeEntry {
        crowkv_console_core::config::NodeEntry {
            id: "n".into(),
            rack_id: "r".into(),
            host: host.into(),
            ssh_port: 22,
            ssh_user: user.into(),
            ssh_key: key.map(Into::into),
            ssh_password: password.map(Into::into),
        }
    }

    #[test]
    fn resolve_explicit_key_path_wins() {
        let n = node("h", "u", Some("/tmp/k"), Some("p"));
        let c = SshCreds::resolve(&n).unwrap();
        match c {
            SshCreds::KeyPath(p) => assert_eq!(p, Path::new("/tmp/k")),
            SshCreds::Password(_) => panic!("should pick key over password"),
        }
    }

    #[test]
    fn resolve_password_when_no_key() {
        let n = node("h", "u", None, Some("pw"));
        let c = SshCreds::resolve(&n).unwrap();
        assert!(matches!(c, SshCreds::Password(s) if s == "pw"));
    }

    #[test]
    fn parse_toml_full_creds() {
        let s = r#"
            id = "n1"
            rack_id = "r1"
            host = "10.0.0.1"
            ssh_port = 2222
            ssh_user = "ops"
            ssh_key = "/home/ops/.ssh/id_ed25519"
        "#;
        let n = parse_node_toml(s).unwrap();
        assert_eq!(n.host, "10.0.0.1");
        assert_eq!(n.ssh_port, 2222);
        assert_eq!(n.ssh_user, "ops");
        assert_eq!(n.ssh_key.as_deref(), Some("/home/ops/.ssh/id_ed25519"));
        assert!(n.ssh_password.is_none());
    }

    #[test]
    fn parse_toml_defaults_port_and_password_optional() {
        let s = r#"
            id = "n1"
            rack_id = "r1"
            host = "10.0.0.1"
        "#;
        let n = parse_node_toml(s).unwrap();
        assert_eq!(n.ssh_port, 22);
        assert!(n.ssh_password.is_none());
        assert!(!n.ssh_enabled());
    }
}
