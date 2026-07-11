use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use crowkv_console_shared::monitor::MonitorCache;
use crowkv_console_shared::{config::ServerEntry, ConsoleConfig};

/// Shared, mutable console state.
///
/// `config` carries the full `ConsoleConfig` (racks, nodes, servers, bench
/// overrides) behind a `RwLock`; mutations are persisted via
/// `ConsoleConfig::save` to `config_path` when present.
///
/// `openapi_cache` is a per-node TTL cache for the `OpenAPI` JSON proxy.
#[derive(Clone, Default)]
pub struct AppState {
    pub config: Arc<RwLock<ConsoleConfig>>,
    pub config_path: Option<Arc<PathBuf>>,
    pub openapi_cache: Arc<std::sync::Mutex<HashMap<String, (serde_json::Value, std::time::Instant)>>>,
    pub monitor_cache: Arc<MonitorCache>,
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
        Self {
            config: Arc::new(RwLock::new(config)),
            config_path: path.map(Arc::new),
            openapi_cache: Arc::new(std::sync::Mutex::new(HashMap::new())),
            monitor_cache: Arc::new(MonitorCache::new()),
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
        if let Some(path) = self.config_path.as_ref() {
            let cfg = self.config.read().unwrap();
            cfg.save(path.as_ref())?;
        }
        Ok(())
    }
}

/// Compile-time path to the vendored Swagger UI assets (committed under
/// `crowkv-console/web/swagger-ui`).
pub const SWAGGER_UI_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/swagger-ui");

/// Compile-time path to the React SPA build output. The `ui/`
/// project is within the `crowkv-web` crate; running
/// `npm run build` (or `make ui-build`) populates `dist/`.
pub const FRONTEND_DIST: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/ui/dist");
