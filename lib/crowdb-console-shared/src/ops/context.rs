// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! [`OpContext`] — shared connection state for all `ops` functions.

use std::sync::{Arc, RwLock};

use crowdb_kv_client::{ClientConfig, CrowdbKvClient, CrowdbSysmdClient};

use crate::config::{ConsoleConfig, NodeEntry, ServerEntry};
use crate::error::{Error, Result};

/// Shared context for all `ops` domain functions.
///
/// Holds:
/// - **`sysmd`** — a [`CrowdbSysmdClient`] for group-0 system metadata
///   (hardware hierarchy, KV-cluster topology, service registry).
/// - **`kv`** — a [`CrowdbKvClient`] for the KV data-plane (put/get/
///   delete/scan on user stores/groups).
/// - **`config`** — the local TOML [`ConsoleConfig`] (rack/node/server
///   entries, bootstrap state). Mutated under an `RwLock` and persisted
///   by the caller via the engine.
///
/// Both `sysmd` and `kv` wrap the same underlying `Arc<CrowdbKvClient>`
/// so the topology cache and connection pool are shared.
pub struct OpContext {
    sysmd: CrowdbSysmdClient,
    kv: Arc<CrowdbKvClient>,
    config: RwLock<ConsoleConfig>,
}

impl std::fmt::Debug for OpContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpContext")
            .field("sysmd", &"CrowdbSysmdClient")
            .field("kv", &"Arc<CrowdbKvClient>")
            .field("config", &self.config)
            .finish()
    }
}

impl OpContext {
    /// Build an `OpContext` from a group-0 endpoint (e.g.
    /// `127.0.0.1:28001`) and an initial [`ConsoleConfig`].
    ///
    /// The `group0_endpoint` is used to seed the topology cache for
    /// group 0 (store 0, group 0). The mgmt URLs of all group-0 hosting
    /// nodes are passed as topology-discovery seeds so the client can
    /// find a new leader when the seeded one is down.
    #[must_use]
    pub fn new(group0_endpoint: String, mgmt_seeds: Vec<String>, config: ConsoleConfig) -> Self {
        let mut seeds = mgmt_seeds;
        if !seeds.iter().any(|s| s == &group0_endpoint) {
            seeds.push(group0_endpoint.clone());
        }
        let kv = CrowdbKvClient::new(ClientConfig::new(seeds));
        kv.seed_leader(0, 0, group0_endpoint);
        let shared = Arc::new(kv);
        let sysmd = CrowdbSysmdClient::from_shared(Arc::clone(&shared));
        Self {
            sysmd,
            kv: shared,
            config: RwLock::new(config),
        }
    }

    /// Build an `OpContext` from a pre-built shared [`CrowdbKvClient`].
    ///
    /// Used by the web backend (`AppState::op_context`) to share the
    /// cached client (topology cache + connection pool) across requests
    /// rather than building a new one per request. `group0_endpoint`
    /// seeds the leader hint for store 0 / group 0; `mgmt_seeds` are
    /// additional topology-discovery seeds (already in the shared
    /// client's config — accepted for API symmetry with [`Self::new`]).
    #[must_use]
    pub fn with_shared_client(
        kv: Arc<CrowdbKvClient>,
        group0_endpoint: String,
        mgmt_seeds: &[String],
        config: ConsoleConfig,
    ) -> Self {
        let _ = mgmt_seeds; // seeds are already in the shared client's config
        kv.seed_leader(0, 0, group0_endpoint);
        let sysmd = CrowdbSysmdClient::from_shared(Arc::clone(&kv));
        Self {
            sysmd,
            kv,
            config: RwLock::new(config),
        }
    }

    /// Access the [`CrowdbSysmdClient`] for group-0 system metadata.
    #[must_use]
    pub fn sysmd(&self) -> &CrowdbSysmdClient {
        &self.sysmd
    }

    /// Access the [`CrowdbKvClient`] for the KV data-plane.
    #[must_use]
    pub fn kv(&self) -> &CrowdbKvClient {
        &self.kv
    }

    /// Re-seed the group-0 leader hint. Called after deploying servers
    /// (e.g. `local_deploy`) so subsequent sysdata writes target the
    /// correct endpoint.
    pub fn seed_group0_leader(&self, endpoint: String) {
        self.kv.seed_leader(0, 0, endpoint);
    }

    /// Access the shared [`Arc<CrowdbKvClient>`] for the KV data-plane.
    /// Used by the web backend to verify the client is shared (not
    /// duplicated) between `AppState` and `OpContext`.
    #[must_use]
    pub fn kv_arc(&self) -> &Arc<CrowdbKvClient> {
        &self.kv
    }

    /// Read-lock the [`ConsoleConfig`].
    ///
    /// # Panics
    /// Panics if the `RwLock` is poisoned.
    pub fn config(&self) -> std::sync::RwLockReadGuard<'_, ConsoleConfig> {
        self.config.read().unwrap()
    }

    /// Write-lock the [`ConsoleConfig`].
    ///
    /// # Panics
    /// Panics if the `RwLock` is poisoned.
    pub fn config_mut(&self) -> std::sync::RwLockWriteGuard<'_, ConsoleConfig> {
        self.config.write().unwrap()
    }

    /// Look up a [`NodeEntry`] by id from the local config.
    ///
    /// # Errors
    /// Returns [`Error::NotFound`] if the node does not exist.
    pub fn node_entry(&self, node_id: u64) -> Result<NodeEntry> {
        self.config()
            .node(node_id)
            .cloned()
            .ok_or_else(|| Error::NotFound {
                kind: "node".into(),
                id: node_id.to_string(),
            })
    }

    /// Look up the [`ServerEntry`] deployed on a node, from the local
    /// config.
    ///
    /// # Errors
    /// Returns [`Error::NotFound`] if no server is deployed on the node.
    pub fn server_for_node(&self, node_id: u64) -> Result<ServerEntry> {
        self.config()
            .server_for_node(node_id)
            .cloned()
            .ok_or_else(|| Error::NotFound {
                kind: "server".into(),
                id: node_id.to_string(),
            })
    }

    /// Resolve the HTTP management URL for a node's deployed
    /// `crowdb-kv-server`. This is the `ServerEntry.url` field (e.g.
    /// `http://127.0.0.1:9910`).
    ///
    /// # Errors
    /// Returns [`Error::NotFound`] if no server is deployed on the node.
    pub fn node_mgmt_url(&self, node_id: u64) -> Result<String> {
        Ok(self.server_for_node(node_id)?.url)
    }
}
