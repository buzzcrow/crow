// Copyright 2026-present Gian <crow.db@outlook.com>

//! `ClusterHandle` — persistent deploy metadata for the bench lifecycle
//! verbs (deploy/prepare/run/teardown). Serialized as JSON at
//! `runtime/<deploy-name>/handle.json` so separate CLI invocations can
//! attach to the same deployed cluster.

use std::path::PathBuf;

#[cfg(test)]
use std::path::Path;

use crowdb_console_shared::error::{Error, Result};
use serde::{Deserialize, Serialize};

/// Deploy kind: which bench target provisioned the cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum DeployKind {
    Kv,
    Rpc,
    Chunk,
    Storage,
}

impl DeployKind {
    /// Parse a `--kind` CLI value.
    #[must_use]
    pub(crate) fn parse(s: &str) -> Option<Self> {
        match s {
            "kv" => Some(Self::Kv),
            "rpc" => Some(Self::Rpc),
            "chunk" => Some(Self::Chunk),
            "storage" => Some(Self::Storage),
            _ => None,
        }
    }

    #[must_use]
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Kv => "kv",
            Self::Rpc => "rpc",
            Self::Chunk => "chunk",
            Self::Storage => "storage",
        }
    }
}

/// Snapshot of deploy-time tunables, recorded in the handle so
/// `bench run` can rebuild the client with the same RPC settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct HandleTunables {
    pub max_inflight: usize,
    pub metrics_interval: u64,
    pub peer_pool_size: usize,
    pub enable_nagle: bool,
    pub quickack: bool,
    pub event_write: bool,
    pub send_queue_capacity: u32,
}

/// Persistent deploy metadata. Written by `bench deploy`, read by
/// `bench prepare`/`run`/`teardown`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ClusterHandle {
    pub name: String,
    pub kind: DeployKind,
    pub store_id: u64,
    pub group_id: u64,
    /// Elected leader's crowdb-rpc URL at deploy time. `bench run`
    /// seeds its client from this; if the leader has since changed,
    /// the client's retry/redirect path resolves the new leader.
    pub leader_endpoint: String,
    pub node_ids: Vec<u64>,
    /// OS pids of the deployed server processes, for `bench teardown`
    /// SIGTERM.
    pub node_pids: Vec<u32>,
    pub node_rpc_urls: Vec<String>,
    /// Per-node management API URLs (`http://host:port`), for flush /
    /// topology / wipe endpoints.
    pub node_mgmt_urls: Vec<String>,
    /// Workspace dir (relative to `runtime/<name>/`) where node data
    /// and logs live.
    pub workspace_dir: PathBuf,
    /// Storage mode label: `mem`, `file`, `block-device`, or `rpc`.
    pub mode: String,
    pub tunables: HandleTunables,
    /// Console-web URL when deployed with `--web`. `None` for headless.
    pub console_url: Option<String>,
    /// Console-web pid when deployed with `--web`. `None` for headless.
    pub console_pid: Option<u32>,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

impl ClusterHandle {
    /// Top-level runtime folder: `runtime/` under the current working
    /// directory.
    pub(crate) fn runtime_root() -> PathBuf {
        PathBuf::from("runtime")
    }

    /// Per-deploy folder: `runtime/<name>/`.
    pub(crate) fn runtime_dir(name: &str) -> PathBuf {
        Self::runtime_root().join(name)
    }

    /// Handle file path: `runtime/<name>/handle.json`.
    fn handle_path(name: &str) -> PathBuf {
        Self::runtime_dir(name).join("handle.json")
    }

    /// Serialize to `runtime/<name>/handle.json`. Creates
    /// `runtime/<name>/` if missing. Errors if the deploy name already
    /// exists (re-deploy guard).
    ///
    /// # Errors
    /// `Error::Config` if the deploy already exists or serialization
    /// fails.
    pub(crate) fn save(&self) -> Result<PathBuf> {
        let dir = Self::runtime_dir(&self.name);
        let handle_path = dir.join("handle.json");
        if handle_path.exists() {
            return Err(Error::Config(format!(
                "deploy '{}' already exists; teardown first",
                self.name
            )));
        }
        std::fs::create_dir_all(&dir)?;
        let json = serde_json::to_string_pretty(self)
            .map_err(|e| Error::Config(format!("serialize handle: {e}")))?;
        std::fs::write(&handle_path, json)?;
        Ok(handle_path)
    }

    /// Read + deserialize from `runtime/<name>/handle.json`.
    ///
    /// # Errors
    /// `Error::Config` if the file is missing (with a list of existing
    /// deploys) or deserialization fails.
    pub(crate) fn load(name: &str) -> Result<Self> {
        let path = Self::handle_path(name);
        if !path.exists() {
            let deploys = Self::list_deploys();
            let msg = if deploys.is_empty() {
                format!("deploy '{name}' not found (no deploys under runtime/)")
            } else {
                format!(
                    "deploy '{name}' not found; existing deploys: {}",
                    deploys.join(", ")
                )
            };
            return Err(Error::Config(msg));
        }
        let bytes = std::fs::read(&path)?;
        serde_json::from_slice(&bytes)
            .map_err(|e| Error::Config(format!("deserialize handle for '{name}': {e}")))
    }

    /// Scan `runtime/*/handle.json` for valid deploy names.
    #[must_use]
    pub(crate) fn list_deploys() -> Vec<String> {
        let root = Self::runtime_root();
        let Ok(entries) = std::fs::read_dir(&root) else {
            return Vec::new();
        };
        let mut names = Vec::new();
        for entry in entries.flatten() {
            let handle_path = entry.path().join("handle.json");
            if handle_path.exists() {
                if let Some(name) = entry.file_name().to_str() {
                    names.push(name.to_string());
                }
            }
        }
        names.sort();
        names
    }

    /// Remove the handle file (mark as torn down). Leaves the rest of
    /// `runtime/<name>/` (logs, reports) for post-mortem.
    ///
    /// # Errors
    /// `Error::Io` if the file cannot be removed.
    pub(crate) fn remove_handle_file(name: &str) -> std::io::Result<()> {
        let path = Self::handle_path(name);
        if path.exists() {
            std::fs::remove_file(&path)?;
        }
        Ok(())
    }

    // ── Test helpers: save/load with an explicit base dir ──────────

    /// Save to `<base>/<name>/handle.json` (test helper — production
    /// code uses `save()` which writes to `runtime/<name>/`).
    #[cfg(test)]
    fn save_in(&self, base: &Path) -> std::io::Result<PathBuf> {
        let dir = base.join(&self.name);
        std::fs::create_dir_all(&dir)?;
        let path = dir.join("handle.json");
        let json = serde_json::to_string_pretty(self).expect("serialize");
        std::fs::write(&path, json)?;
        Ok(path)
    }

    /// Load from `<base>/<name>/handle.json` (test helper).
    #[cfg(test)]
    fn load_in(name: &str, base: &Path) -> std::io::Result<Self> {
        let path = base.join(name).join("handle.json");
        let bytes = std::fs::read(&path)?;
        serde_json::from_slice(&bytes).map_err(std::io::Error::other)
    }

    /// List deploys under `<base>/` (test helper).
    #[cfg(test)]
    fn list_deploys_in(base: &Path) -> Vec<String> {
        let Ok(entries) = std::fs::read_dir(base) else {
            return Vec::new();
        };
        let mut names = Vec::new();
        for entry in entries.flatten() {
            if entry.path().join("handle.json").exists() {
                if let Some(name) = entry.file_name().to_str() {
                    names.push(name.to_string());
                }
            }
        }
        names.sort();
        names
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn test_handle(name: &str) -> ClusterHandle {
        ClusterHandle {
            name: name.to_string(),
            kind: DeployKind::Kv,
            store_id: 1,
            group_id: 1,
            leader_endpoint: "http://127.0.0.1:28001".into(),
            node_ids: vec![0, 1, 2],
            node_pids: vec![100, 101, 102],
            node_rpc_urls: vec!["http://127.0.0.1:28001".into()],
            node_mgmt_urls: vec!["http://127.0.0.1:29001".into()],
            workspace_dir: PathBuf::from("workspace"),
            mode: "mem".into(),
            tunables: HandleTunables {
                max_inflight: 32,
                metrics_interval: 5,
                peer_pool_size: 2,
                enable_nagle: false,
                quickack: false,
                event_write: false,
                send_queue_capacity: 4096,
            },
            console_url: None,
            console_pid: None,
            created_at: Utc::now(),
        }
    }

    fn unique_temp_dir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "crowdb-bench-handle-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn handle_save_load_round_trip() {
        let base = unique_temp_dir();
        let h = test_handle("roundtrip");
        h.save_in(&base).unwrap();
        let loaded = ClusterHandle::load_in("roundtrip", &base).unwrap();
        assert_eq!(loaded.name, h.name);
        assert_eq!(loaded.kind, h.kind);
        assert_eq!(loaded.node_pids, h.node_pids);
        assert_eq!(loaded.leader_endpoint, h.leader_endpoint);
        let _ = std::fs::remove_dir_all(&base);
    }

    #[test]
    fn handle_load_missing_name_lists_deploys() {
        let base = unique_temp_dir();
        let h = test_handle("existing");
        h.save_in(&base).unwrap();
        let deploys = ClusterHandle::list_deploys_in(&base);
        assert_eq!(deploys, vec!["existing"]);
        // load_in returns io::Error; the production load() wraps it in
        // Error::Config with the deploy list. Here we just verify the
        // file is missing.
        assert!(ClusterHandle::load_in("nonexistent", &base).is_err());
        let _ = std::fs::remove_dir_all(&base);
    }

    #[test]
    fn handle_save_on_existing_name_errors() {
        let h = test_handle("dup");
        // First save succeeds.
        h.save().unwrap();
        // Second save fails with "already exists".
        let err = h.save().unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("already exists"), "msg={msg}");
        // Cleanup.
        let _ = ClusterHandle::remove_handle_file("dup");
    }

    #[test]
    fn handle_list_deploys() {
        let base = unique_temp_dir();
        assert!(ClusterHandle::list_deploys_in(&base).is_empty());
        test_handle("a").save_in(&base).unwrap();
        test_handle("b").save_in(&base).unwrap();
        test_handle("c").save_in(&base).unwrap();
        let deploys = ClusterHandle::list_deploys_in(&base);
        assert_eq!(deploys, vec!["a", "b", "c"]);
        let _ = std::fs::remove_dir_all(&base);
    }

    #[test]
    fn deploy_kind_parse() {
        assert_eq!(DeployKind::parse("kv"), Some(DeployKind::Kv));
        assert_eq!(DeployKind::parse("rpc"), Some(DeployKind::Rpc));
        assert_eq!(DeployKind::parse("chunk"), Some(DeployKind::Chunk));
        assert_eq!(DeployKind::parse("storage"), Some(DeployKind::Storage));
        assert_eq!(DeployKind::parse("bad"), None);
    }
}
