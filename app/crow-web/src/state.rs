// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use crow_console_shared::error::{Error, Result};
use crow_console_shared::monitor::MonitorCache;
use crow_console_shared::{
    config::{ConsoleConfigEngine, ServerEntry, TomlFileEngine},
    ConsoleConfig,
};

/// Shared, mutable console state.
///
/// `config` carries the full `ConsoleConfig` (racks, nodes, servers)
/// behind a `RwLock`; mutations are persisted via `ConsoleConfig::save`
/// to `config_path` when present.
///
/// `openapi_cache` is a per-node TTL cache for the `OpenAPI` JSON proxy.
///
/// `diskdb_client` is lazily initialized on the first `/api/diskdb/*`
/// request (the service registry may not be ready at console startup).
#[derive(Clone)]
pub struct AppState {
    pub config: Arc<RwLock<ConsoleConfig>>,
    pub config_engine: Option<Arc<dyn ConsoleConfigEngine>>,
    pub runtime_root: Arc<PathBuf>,
    pub openapi_cache: Arc<std::sync::Mutex<HashMap<u64, (serde_json::Value, std::time::Instant)>>>,
    pub monitor_cache: Arc<MonitorCache>,
    pub runtime_pids: Arc<std::sync::Mutex<HashMap<String, u32>>>,
    pub diskdb_client: Arc<tokio::sync::RwLock<Option<crow_diskdb_client::DiskdbClient>>>,
    /// Cached crow-rpc transport reused across KV requests to avoid
    /// spawning 6+ threads per request. Shared by all `CrowkvClient`
    /// instances created in `kv.rs`.
    pub kv_rpc_transport: Arc<tokio::sync::RwLock<Option<Arc<crow_kv_client::KvRpcTransport>>>>,
    /// Rate-limiter for repeated gRPC failure warnings: maps
    /// `endpoint` → last-warned timestamp. Prevents flooding the
    /// console with identical "instance query failed" warnings every
    /// poll cycle when a diskdb instance is unreachable.
    pub warn_dedup: Arc<std::sync::Mutex<HashMap<String, std::time::Instant>>>,
}

impl Default for AppState {
    fn default() -> Self {
        Self::with_config(ConsoleConfig::default(), None)
    }
}

impl AppState {
    /// Build state from a list of pre-registered server URLs. Each URL
    /// becomes a synthetic `ServerEntry` with id `srv{i}`. Used by tests
    /// and by callers that don't need a persistent registry.
    #[must_use]
    pub fn new(default_servers: Vec<String>) -> Self {
        let mut cfg = ConsoleConfig::default();
        for (i, url) in default_servers.into_iter().enumerate() {
            let _ = cfg.add_server(ServerEntry::new(format!("srv{i}"), url));
        }
        Self::with_config(cfg, None)
    }

    /// Build state from an already-loaded `ConsoleConfig`. `path` is the
    /// on-disk location used by mutating handlers to persist changes;
    /// pass `None` for in-memory-only state (tests).
    #[must_use]
    pub fn with_config(config: ConsoleConfig, path: Option<PathBuf>) -> Self {
        let runtime_root = path
            .as_ref()
            .and_then(|path| path.parent().map(std::path::Path::to_path_buf))
            .unwrap_or_else(|| PathBuf::from("runtime-data"));
        let engine = path.map(|path| Arc::new(TomlFileEngine::new(path)) as Arc<dyn ConsoleConfigEngine>);
        Self::with_config_engine(config, engine, runtime_root)
    }

    #[must_use]
    pub fn with_config_engine(
        config: ConsoleConfig,
        engine: Option<Arc<dyn ConsoleConfigEngine>>,
        runtime_root: PathBuf,
    ) -> Self {
        Self {
            config: Arc::new(RwLock::new(config)),
            config_engine: engine,
            runtime_root: Arc::new(runtime_root),
            openapi_cache: Arc::new(std::sync::Mutex::new(HashMap::new())),
            monitor_cache: Arc::new(MonitorCache::new()),
            runtime_pids: Arc::new(std::sync::Mutex::new(HashMap::new())),
            diskdb_client: Arc::new(tokio::sync::RwLock::new(None)),
            kv_rpc_transport: Arc::new(tokio::sync::RwLock::new(None)),
            warn_dedup: Arc::new(std::sync::Mutex::new(HashMap::new())),
        }
    }

    /// Persist the current config to `config_path`, if one was provided.
    /// No-op for in-memory state.
    ///
    /// # Panics
    /// Panics if the `RwLock` is poisoned.
    ///
    /// # Errors
    /// Returns an error if config saving fails.
    pub fn persist(&self) -> crow_console_shared::error::Result<()> {
        if let Some(engine) = self.config_engine.as_ref() {
            let cfg = self.config.read().unwrap();
            cfg.save_with_engine(engine.as_ref())?;
        }
        Ok(())
    }

    /// Get the runtime PID for a node.
    ///
    /// # Panics
    /// Panics if the `Mutex` is poisoned.
    #[must_use]
    pub fn runtime_pid(&self, node_id: impl std::fmt::Display) -> Option<u32> {
        self.runtime_pids
            .lock()
            .unwrap()
            .get(&node_id.to_string())
            .copied()
    }

    /// Set the runtime PID for a node.
    ///
    /// # Panics
    /// Panics if the `Mutex` is poisoned.
    pub fn set_runtime_pid(&self, node_id: impl std::fmt::Display, pid: u32) {
        self.runtime_pids.lock().unwrap().insert(node_id.to_string(), pid);
    }

    /// Clear the runtime PID for a node.
    ///
    /// # Panics
    /// Panics if the `Mutex` is poisoned.
    pub fn clear_runtime_pid(&self, node_id: impl std::fmt::Display) {
        self.runtime_pids.lock().unwrap().remove(&node_id.to_string());
    }

    /// Snapshot of all tracked KV server PIDs, keyed by node id.
    /// Excludes DDB PIDs (keyed as `diskdb-{node_id}`).
    ///
    /// # Panics
    /// Panics if the `Mutex` is poisoned.
    #[must_use]
    pub fn kv_pid_snapshot(&self) -> std::collections::HashMap<u64, u32> {
        self.runtime_pids
            .lock()
            .unwrap()
            .iter()
            .filter_map(|(k, v)| {
                if k.starts_with("diskdb-") {
                    None
                } else {
                    k.parse::<u64>().ok().map(|n| (n, *v))
                }
            })
            .collect()
    }

    /// Get the runtime PID for a diskdb instance on a node (R77).
    /// Keyed separately from kv-server PIDs.
    ///
    /// # Panics
    /// Panics if the `Mutex` is poisoned.
    #[must_use]
    pub fn diskdb_runtime_pid(&self, node_id: impl std::fmt::Display) -> Option<u32> {
        self.runtime_pids
            .lock()
            .unwrap()
            .get(&format!("diskdb-{node_id}"))
            .copied()
    }

    /// Set the runtime PID for a diskdb instance on a node (R77).
    ///
    /// # Panics
    /// Panics if the `Mutex` is poisoned.
    pub fn set_diskdb_runtime_pid(&self, node_id: impl std::fmt::Display, pid: u32) {
        self.runtime_pids
            .lock()
            .unwrap()
            .insert(format!("diskdb-{node_id}"), pid);
    }

    /// Clear the runtime PID for a diskdb instance on a node (R77).
    ///
    /// # Panics
    /// Panics if the `Mutex` is poisoned.
    pub fn clear_diskdb_runtime_pid(&self, node_id: impl std::fmt::Display) {
        self.runtime_pids
            .lock()
            .unwrap()
            .remove(&format!("diskdb-{node_id}"));
    }

    #[must_use]
    pub fn node_workspace_dir(&self, node_id: impl std::fmt::Display) -> PathBuf {
        self.runtime_root.join(format!("N-{node_id}"))
    }

    /// Rate-limited warning: returns `true` if the caller should warn
    /// (at most once per `interval` per `key`), `false` if the same
    /// key was warned recently and should be suppressed.
    ///
    /// # Panics
    /// Panics if the `Mutex` is poisoned.
    #[must_use]
    pub fn should_warn(&self, key: &str, interval: std::time::Duration) -> bool {
        let mut map = self.warn_dedup.lock().unwrap();
        let now = std::time::Instant::now();
        if let Some(last) = map.get(key) {
            if now.duration_since(*last) < interval {
                return false;
            }
        }
        map.insert(key.to_string(), now);
        true
    }

    /// Remove all node workspace directories under `runtime_root`.
    /// Called by the internal reset endpoint after stopping servers.
    ///
    /// # Errors
    ///
    /// Returns an error if a directory cannot be removed.
    pub fn clear_workspaces(&self) -> Result<()> {
        if !self.runtime_root.exists() {
            return Ok(());
        }
        for entry in std::fs::read_dir(&*self.runtime_root).map_err(Error::Io)? {
            let entry = entry.map_err(Error::Io)?;
            let name = entry.file_name();
            if name.to_string_lossy().starts_with("N-") {
                std::fs::remove_dir_all(entry.path()).map_err(Error::Io)?;
            }
        }
        Ok(())
    }

    /// Prepares the workspace directory for a node.
    ///
    /// # Errors
    ///
    /// Returns an error if directory creation fails due to I/O errors.
    pub fn prepare_node_workspace(&self, node_id: impl std::fmt::Display) -> Result<PathBuf> {
        let base = self.node_workspace_dir(node_id);
        std::fs::create_dir_all(&base).map_err(Error::Io)?;
        std::fs::create_dir_all(base.join("bin")).map_err(Error::Io)?;
        std::fs::create_dir_all(base.join("log")).map_err(Error::Io)?;
        std::fs::create_dir_all(base.join("waldata")).map_err(Error::Io)?;
        std::fs::canonicalize(base).map_err(Error::Io)
    }

    /// Get or create a cached `KvRpcTransport`. The transport (and its
    /// crow-rpc server/client + I/O thread) is reused across KV requests
    /// to avoid spawning threads per request. A fresh `CrowkvClient` is
    /// still created per request (cheap — no threads), sharing this
    /// transport via `with_rpc_transport`.
    pub async fn kv_rpc_transport(&self) -> Arc<crow_kv_client::KvRpcTransport> {
        if let Some(t) = self.kv_rpc_transport.read().await.as_ref() {
            return Arc::clone(t);
        }
        let mut guard = self.kv_rpc_transport.write().await;
        if let Some(t) = guard.as_ref() {
            return Arc::clone(t);
        }
        let t = Arc::new(crow_kv_client::KvRpcTransport::new());
        *guard = Some(Arc::clone(&t));
        t
    }
}

/// Compile-time path to the vendored Swagger UI assets (committed under
/// `crow-console/web/swagger-ui`).
pub const SWAGGER_UI_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/swagger-ui");

/// Compile-time path to the React SPA build output. The `ui/`
/// project is within the `crow-web` crate; running
/// `npm run build` (or `make ui-build`) populates `dist/`.
pub const FRONTEND_DIST: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/ui/dist");

#[cfg(test)]
mod tests {
    use super::*;

    static CWD_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn tempdir(tag: &str) -> PathBuf {
        let base = std::env::temp_dir();
        let unique = format!(
            "crow-web-state-{tag}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let dir = base.join(unique);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn prepare_node_workspace_returns_absolute_path_for_relative_runtime_root() {
        let _guard = CWD_LOCK.lock().unwrap();
        let original_cwd = std::env::current_dir().unwrap();
        let root = tempdir("relative-runtime-root");
        std::env::set_current_dir(&root).unwrap();

        let state =
            AppState::with_config_engine(ConsoleConfig::default(), None, PathBuf::from("runtime-data"));
        let workspace = state.prepare_node_workspace("n1").unwrap();

        assert!(workspace.is_absolute());
        assert!(workspace.ends_with(PathBuf::from("runtime-data/N-n1")));
        assert!(workspace.join("bin").is_dir());
        assert!(workspace.join("log").is_dir());
        assert!(workspace.join("waldata").is_dir());

        std::env::set_current_dir(original_cwd).unwrap();
        let _ = std::fs::remove_dir_all(root);
    }
}
