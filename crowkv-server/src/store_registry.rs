use crowkv::cluster::px_kv_store::PxKvStore;
use crowkv::common::config::PxElectionConfig;
use crowkv::wal::IoBackend;
use dashmap::DashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;

pub struct KvStoreRegistry {
    pub stores: DashMap<u64, Arc<PxKvStore>>,
    pub election_cfg: PxElectionConfig,
    pub wal_root: PathBuf,
    pub config_root: PathBuf,
    pub wal_backend: Arc<IoBackend>,
    /// Port pool for KV server listeners, populated from `--ports` CLI arg.
    /// Used by `add_store` as a fallback before `persisted_port_for_store`.
    port_pool: Mutex<Vec<u16>>,
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
            PathBuf::from("wal"),
            PathBuf::from("conf"),
            Arc::new(IoBackend::detect()),
        )
    }

    #[must_use]
    pub fn with_election_config(election_cfg: PxElectionConfig) -> Self {
        Self::with_runtime(
            election_cfg,
            PathBuf::from("wal"),
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
        Self {
            stores: DashMap::new(),
            election_cfg,
            wal_root,
            config_root,
            wal_backend,
            port_pool: Mutex::new(Vec::new()),
        }
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
