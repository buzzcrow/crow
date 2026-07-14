// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use crowkv_console_shared::error::{Error, Result};
use crowkv_console_shared::monitor::MonitorCache;
use crowkv_console_shared::{
    config::{ConsoleConfigEngine, ServerEntry, TomlFileEngine},
    ConsoleConfig,
};

/// Shared, mutable console state.
///
/// `config` carries the full `ConsoleConfig` (racks, nodes, servers, bench
/// overrides) behind a `RwLock`; mutations are persisted via
/// `ConsoleConfig::save` to `config_path` when present.
///
/// `openapi_cache` is a per-node TTL cache for the `OpenAPI` JSON proxy.
#[derive(Clone)]
pub struct AppState {
    pub config: Arc<RwLock<ConsoleConfig>>,
    pub config_engine: Option<Arc<dyn ConsoleConfigEngine>>,
    pub runtime_root: Arc<PathBuf>,
    pub openapi_cache: Arc<std::sync::Mutex<HashMap<String, (serde_json::Value, std::time::Instant)>>>,
    pub monitor_cache: Arc<MonitorCache>,
    pub runtime_pids: Arc<std::sync::Mutex<HashMap<String, u32>>>,
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
    pub fn persist(&self) -> crowkv_console_shared::error::Result<()> {
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
    pub fn runtime_pid(&self, node_id: &str) -> Option<u32> {
        self.runtime_pids.lock().unwrap().get(node_id).copied()
    }

    /// Set the runtime PID for a node.
    ///
    /// # Panics
    /// Panics if the `Mutex` is poisoned.
    pub fn set_runtime_pid(&self, node_id: impl Into<String>, pid: u32) {
        self.runtime_pids.lock().unwrap().insert(node_id.into(), pid);
    }

    /// Clear the runtime PID for a node.
    ///
    /// # Panics
    /// Panics if the `Mutex` is poisoned.
    pub fn clear_runtime_pid(&self, node_id: &str) {
        self.runtime_pids.lock().unwrap().remove(node_id);
    }

    #[must_use]
    pub fn node_workspace_dir(&self, node_id: &str) -> PathBuf {
        self.runtime_root.join(format!("N-{node_id}"))
    }

    /// Prepares the workspace directory for a node.
    ///
    /// # Errors
    ///
    /// Returns an error if directory creation fails due to I/O errors.
    pub fn prepare_node_workspace(&self, node_id: &str) -> Result<PathBuf> {
        let base = self.node_workspace_dir(node_id);
        std::fs::create_dir_all(&base).map_err(Error::Io)?;
        std::fs::create_dir_all(base.join("bin")).map_err(Error::Io)?;
        std::fs::create_dir_all(base.join("log")).map_err(Error::Io)?;
        std::fs::create_dir_all(base.join("wal")).map_err(Error::Io)?;
        std::fs::canonicalize(base).map_err(Error::Io)
    }
}

/// Compile-time path to the vendored Swagger UI assets (committed under
/// `crowkv-console/web/swagger-ui`).
pub const SWAGGER_UI_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/swagger-ui");

/// Compile-time path to the React SPA build output. The `ui/`
/// project is within the `crowkv-web` crate; running
/// `npm run build` (or `make ui-build`) populates `dist/`.
pub const FRONTEND_DIST: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/ui/dist");

#[cfg(test)]
mod tests {
    use super::*;

    static CWD_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn tempdir(tag: &str) -> PathBuf {
        let base = std::env::temp_dir();
        let unique = format!(
            "crowkv-web-state-{tag}-{}-{}",
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
        assert!(workspace.join("wal").is_dir());

        std::env::set_current_dir(original_cwd).unwrap();
        let _ = std::fs::remove_dir_all(root);
    }
}
