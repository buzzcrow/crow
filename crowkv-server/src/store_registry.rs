// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

use crowkv::cluster::px_kv_store::PxKvStore;
use crowkv::common::config::{AdmissionPolicy, PaxosConfig, PxElectionConfig};
use crowkv::kv::CrowtreeBackend;
use crowkv::metrics::MetricsRegistry;
use crowkv::wal::IoBackend;
use dashmap::DashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;

/// Parse the `--wal-backend` CLI value (`clap`'s `value_parser` already
/// restricts it to `["file", "mem-block", "block-device"]`) into the
/// WAL's [`IoBackend`].
#[must_use]
pub fn parse_wal_backend(s: &str) -> IoBackend {
    match s {
        "mem-block" => IoBackend::mem_block(),
        "block-device" => IoBackend::block_device(),
        _ => IoBackend::File,
    }
}

/// Parse the `--kv-backend` CLI value (`clap`'s `value_parser` already
/// restricts it to `["file", "block", "mem-block"]`) into the FFI's
/// [`CrowtreeBackend`].
#[must_use]
pub fn parse_crowtree_backend(s: &str) -> CrowtreeBackend {
    match s {
        "block" => CrowtreeBackend::Block,
        "mem-block" => CrowtreeBackend::MemBlock,
        _ => CrowtreeBackend::File,
    }
}

pub struct KvStoreRegistry {
    pub stores: DashMap<u64, Arc<PxKvStore>>,
    pub election_cfg: PxElectionConfig,
    pub wal_root: PathBuf,
    pub config_root: PathBuf,
    pub wal_backend: Arc<IoBackend>,
    /// Root directory for durable per-group crowtree files.
    pub data_root: PathBuf,
    /// Durable backend for the crowtree engine.
    pub crowtree_backend: CrowtreeBackend,
    /// Port pool for KV server listeners, populated from `--ports` CLI arg.
    /// Used by `add_store` as a fallback before `persisted_port_for_store`.
    port_pool: Mutex<Vec<u16>>,
    /// Metrics registry shared by all stores. `None` when metrics disabled.
    pub metrics_registry: Option<Arc<Mutex<MetricsRegistry>>>,
    /// Skip the durable `fdatasync` on every WAL write batch, for all
    /// groups created by this registry. See `--no-fsync` (R10 benchmark
    /// framework). Default `false`.
    pub wal_skip_fsync: bool,
    /// Max in-flight proposals for groups created via the management
    /// API. Set from `--max-inflight` CLI arg. Default: `PaxosConfig::DEFAULT`.
    pub max_inflight: usize,
    /// Number of admission queues per group. Default: 1.
    pub inflight_queues: usize,
    /// Admission policy when the inflight window is full. Default: `Reject`.
    pub inflight_admission: AdmissionPolicy,
}

impl Default for KvStoreRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl KvStoreRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::with_runtime(
            PxElectionConfig::DEFAULT,
            PathBuf::from("waldata"),
            PathBuf::from("conf"),
            Arc::new(IoBackend::detect()),
        )
    }

    #[must_use]
    pub fn with_election_config(election_cfg: PxElectionConfig) -> Self {
        Self::with_runtime(
            election_cfg,
            PathBuf::from("waldata"),
            PathBuf::from("conf"),
            Arc::new(IoBackend::detect()),
        )
    }

    #[must_use]
    pub fn with_runtime(
        election_cfg: PxElectionConfig,
        wal_root: PathBuf,
        config_root: PathBuf,
        wal_backend: Arc<IoBackend>,
    ) -> Self {
        let data_root = wal_root
            .parent()
            .map_or_else(|| PathBuf::from("ctdata"), |p| p.join("ctdata"));
        Self {
            stores: DashMap::new(),
            election_cfg,
            wal_root,
            config_root,
            wal_backend,
            data_root,
            crowtree_backend: CrowtreeBackend::File,
            port_pool: Mutex::new(Vec::new()),
            metrics_registry: None,
            wal_skip_fsync: false,
            max_inflight: PaxosConfig::DEFAULT.max_inflight_proposals,
            inflight_queues: PaxosConfig::DEFAULT.inflight_queues,
            inflight_admission: PaxosConfig::DEFAULT.inflight_admission,
        }
    }

    /// Builder-style setter for [`Self::data_root`],
    /// used by `main.rs` right after construction (mirrors
    /// [`Self::set_port_pool`]'s pattern of a post-construction CLI-driven
    /// override rather than a longer `with_runtime` parameter list).
    #[must_use]
    pub fn with_data_root(mut self, data_root: PathBuf) -> Self {
        self.data_root = data_root;
        self
    }

    /// Builder-style setter for [`Self::crowtree_backend`],
    /// used by `main.rs` right after construction alongside
    /// [`Self::with_data_root`].
    #[must_use]
    pub fn with_crowtree_backend(mut self, crowtree_backend: CrowtreeBackend) -> Self {
        self.crowtree_backend = crowtree_backend;
        self
    }

    /// Builder-style setter for [`Self::metrics_registry`].
    #[must_use]
    pub fn with_metrics_registry(mut self, registry: Arc<Mutex<MetricsRegistry>>) -> Self {
        self.metrics_registry = Some(registry);
        self
    }

    /// Builder-style setter for [`Self::wal_skip_fsync`].
    #[must_use]
    pub fn with_wal_skip_fsync(mut self, skip_fsync: bool) -> Self {
        self.wal_skip_fsync = skip_fsync;
        self
    }

    /// Builder-style setter for [`Self::max_inflight`].
    #[must_use]
    pub fn with_max_inflight(mut self, max_inflight: usize) -> Self {
        self.max_inflight = max_inflight;
        self
    }

    /// Builder-style setter for [`Self::inflight_queues`].
    #[must_use]
    pub fn with_inflight_queues(mut self, queues: usize) -> Self {
        self.inflight_queues = queues;
        self
    }

    /// Builder-style setter for [`Self::inflight_admission`].
    #[must_use]
    pub fn with_inflight_admission(mut self, policy: AdmissionPolicy) -> Self {
        self.inflight_admission = policy;
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

    pub fn add_store(&self, store_id: u64, store: Arc<PxKvStore>) {
        self.stores.insert(store_id, store);
    }

    #[must_use]
    pub fn get_store(&self, store_id: u64) -> Option<Arc<PxKvStore>> {
        self.stores.get(&store_id).map(|r| r.clone())
    }

    #[must_use]
    pub fn remove_store(&self, store_id: u64) -> Option<Arc<PxKvStore>> {
        self.stores.remove(&store_id).map(|(_, v)| v)
    }
}
