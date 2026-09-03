// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_kv::cluster::px_kv_store::PxKvStore;
use crowdb_kv::common::config::CrowDBConfig;
use crowdb_kv::kv::CrowdbTreeBackend;
use crowdb_kv::metrics::MetricsRegistry;
use crowdb_kv::wal::IoBackend;
use dashmap::DashMap;
use std::sync::Arc;
use std::sync::Mutex;

/// Parse the `--wal-backend` CLI value (`clap`'s `value_parser` already
/// restricts it to `["file", "mem-block", "block-device"]`) into the
/// WAL's [`IoBackend`].
#[must_use]
pub(crate) fn parse_wal_backend(s: &str) -> IoBackend {
    match s {
        "mem-block" => IoBackend::mem_block(),
        "block-device" => IoBackend::block_device(),
        _ => IoBackend::File,
    }
}

/// Parse the `--kv-backend` CLI value (`clap`'s `value_parser` already
/// restricts it to `["file", "block", "mem-block"]`) into the FFI's
/// [`CrowdbTreeBackend`].
#[must_use]
pub(crate) fn parse_crowtree_backend(s: &str) -> CrowdbTreeBackend {
    match s {
        "block" => CrowdbTreeBackend::Block,
        "mem-block" => CrowdbTreeBackend::MemBlock,
        _ => CrowdbTreeBackend::File,
    }
}

pub struct KvStoreRegistry {
    pub stores: DashMap<u64, Arc<PxKvStore>>,
    /// Unified cluster configuration (all sub-configs + flags + paths).
    pub config: CrowDBConfig,
    /// Parsed WAL I/O backend (derived from `config.wal_backend`).
    pub wal_backend: Arc<IoBackend>,
    /// Parsed crowdb-tree storage backend (derived from `config.crowtree_backend`).
    pub crowtree_backend: CrowdbTreeBackend,
    /// Port pool for KV server listeners, populated from `--ports` CLI arg.
    /// Used by `add_store` as a fallback before `persisted_port_for_store`.
    port_pool: Mutex<Vec<u16>>,
    /// Metrics registry shared by all stores. `None` when metrics disabled.
    pub metrics_registry: Option<Arc<Mutex<MetricsRegistry>>>,
    /// crowdb-rpc I/O worker count (from `--rpc-workers` CLI). Applied to
    /// each `PxKvStore` at construction. Default: 2.
    pub rpc_workers: u32,
}

impl Default for KvStoreRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl KvStoreRegistry {
    #[must_use]
    pub(crate) fn new() -> Self {
        Self::with_config(CrowDBConfig::default())
    }

    #[must_use]
    pub fn with_config(config: CrowDBConfig) -> Self {
        let wal_backend = Arc::new(parse_wal_backend(&config.wal_backend));
        let crowtree_backend = parse_crowtree_backend(&config.crowtree_backend);
        Self {
            stores: DashMap::new(),
            wal_backend,
            crowtree_backend,
            config,
            port_pool: Mutex::new(Vec::new()),
            metrics_registry: None,
            rpc_workers: 2,
        }
    }

    /// Builder-style setter for the metrics registry.
    #[must_use]
    pub fn with_metrics_registry(mut self, registry: Arc<Mutex<MetricsRegistry>>) -> Self {
        self.metrics_registry = Some(registry);
        self
    }

    /// Builder-style setter for the crowdb-rpc worker count.
    #[must_use]
    pub fn with_rpc_workers(mut self, workers: u32) -> Self {
        self.rpc_workers = workers;
        self
    }

    /// Set the port pool (from `--ports` CLI argument).
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned.
    pub fn set_port_pool(&self, ports: Vec<u16>) {
        *self.port_pool.lock().unwrap() = ports;
    }

    /// Try to allocate the next port from the pool. Returns `None` if the
    /// pool is empty or exhausted.
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned.
    #[must_use]
    pub fn next_port(&self) -> Option<u16> {
        let mut pool = self.port_pool.lock().unwrap();
        if pool.is_empty() {
            None
        } else {
            Some(pool.remove(0))
        }
    }

    /// Peek at the first port in the pool without removing it. Used to
    /// derive the RPC endpoint for store 0 in first-boot mode (before the
    /// store is created via `/system/init`).
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned.
    #[must_use]
    pub fn first_port(&self) -> Option<u16> {
        self.port_pool.lock().unwrap().first().copied()
    }

    pub fn add_store(&self, store_id: u64, store: Arc<PxKvStore>) {
        self.stores.insert(store_id, store);
    }

    #[must_use]
    pub fn get_store(&self, store_id: u64) -> Option<Arc<PxKvStore>> {
        self.stores.get(&store_id).map(|r| r.clone())
    }

    /// All hosted store IDs.
    #[must_use]
    pub(crate) fn store_ids(&self) -> Vec<u64> {
        self.stores.iter().map(|e| *e.key()).collect()
    }

    #[must_use]
    pub(crate) fn remove_store(&self, store_id: u64) -> Option<Arc<PxKvStore>> {
        self.stores.remove(&store_id).map(|(_, v)| v)
    }
}
