use crowkv::cluster::px_kv_store::PxKvStore;
use crowkv::common::config::PxElectionConfig;
use dashmap::DashMap;
use std::sync::Arc;

/// Registry of all `PxKvStore` instances managed by this server process.
///
/// Shared between the HTTP management API and the main lifecycle loop. Wrapped
/// in `Arc` by callers and accessed via `axum::extract::State`.
pub struct KvStoreRegistry {
    pub stores: DashMap<u64, Arc<PxKvStore>>,
    pub election_cfg: PxElectionConfig,
}

impl Default for KvStoreRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl KvStoreRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::with_election_config(PxElectionConfig::DEFAULT)
    }

    #[must_use]
    pub fn with_election_config(election_cfg: PxElectionConfig) -> Self {
        Self {
            stores: DashMap::new(),
            election_cfg,
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
