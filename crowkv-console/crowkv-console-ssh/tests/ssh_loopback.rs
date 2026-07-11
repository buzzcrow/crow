//! Real-SSH loopback tests, gated by `CROWKV_TEST_SSH=1`.
//!
//! Setup the operator runs once on a dev box:
//!   1. `ssh-keygen -t ed25519 -N '' -f ~/.ssh/id_ed25519` (if missing).
//!   2. `cat ~/.ssh/id_ed25519.pub >> ~/.ssh/authorized_keys`.
//!   3. `chmod 600 ~/.ssh/authorized_keys`.
//!   4. Make sure `sshd` is up: `systemctl status sshd`.
//!   5. `ssh $USER@127.0.0.1 echo ok` should succeed.
//!   6. `CROWKV_TEST_SSH=1 cargo test -p crowkv-console-ssh`.

use crowkv_console_core::config::NodeEntry;
use crowkv_console_ssh::{probe, run_remote};

fn loopback_node() -> Option<NodeEntry> {
    if std::env::var("CROWKV_TEST_SSH").ok().as_deref() != Some("1") {
        return None;
    }
    let user = std::env::var("USER").ok()?;
    Some(NodeEntry {
        id: "loopback".into(),
        rack_id: "r".into(),
        host: "127.0.0.1".into(),
        ssh_port: 22,
        ssh_user: user,
        ssh_key: None,
        ssh_password: None,
    })
}

#[tokio::test]
async fn probe_loopback() {
    let Some(node) = loopback_node() else {
        eprintln!("skipping: set CROWKV_TEST_SSH=1 to enable real-SSH tests");
        return;
    };
    probe(&node).await.expect("probe must succeed against loopback ssh");
}

#[tokio::test]
async fn echo_command_loopback() {
    let Some(node) = loopback_node() else {
        eprintln!("skipping: set CROWKV_TEST_SSH=1 to enable real-SSH tests");
        return;
    };
    let out = run_remote(&node, "echo hello-from-russh").await.expect("exec");
    assert!(out.success(), "exit={:?} stderr={}", out.exit, out.stderr_str());
    assert!(out.stdout_str().contains("hello-from-russh"), "got {:?}", out.stdout_str());
}
