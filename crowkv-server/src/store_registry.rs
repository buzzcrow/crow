use crowkv::cluster::px_kv_store::PxKvStore;
use crowkv::common::config::PxElectionConfig;
use crowkv::wal::IoBackend;
use dashmap::DashMap;
use std::path::PathBuf;
use std::sync::Arc;

pub struct KvStoreRegistry {
    pub stores: DashMap<u64, Arc<PxKvStore>>,
    pub election_cfg: PxElectionConfig,
    pub wal_root: PathBuf,
    pub wal_backend: Arc<IoBackend>,
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
            Arc::new(IoBackend::detect()),
        )
    }

    #[must_use]
    pub fn with_election_config(election_cfg: PxElectionConfig) -> Self {
        Self::with_runtime(election_cfg, PathBuf::from("wal"), Arc::new(IoBackend::detect()))
    }

    #[must_use]
    pub fn with_runtime(
        election_cfg: PxElectionConfig,
        wal_root: PathBuf,
        wal_backend: Arc<IoBackend>,
    ) -> Self {
        Self {
            stores: DashMap::new(),
            election_cfg,
            wal_root,
            wal_backend,
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
