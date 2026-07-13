use crowkv::cluster::px_kv_store::PxKvStore;
use crowkv::common::config::PxElectionConfig;
use crowkv::kv::CrowtreeBackend;
use crowkv::wal::IoBackend;
use dashmap::DashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;

/// Which [`crowkv::kv::KVEngine`] backend new groups are created with.
///
/// Selected once at server startup via `--kv-engine` and stored on the
/// registry; both the CLI bootstrap path (`main.rs`) and the management-API
/// dynamic group-creation path (`mgmt_api.rs::add_group`) read it from there
/// so the two paths can never disagree on which engine a fresh group gets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KvEngineKind {
    /// In-memory, non-durable `InMemKV`. No longer the `--kv-engine` CLI
    /// default (flipped to `Crowtree` in `doc/todo-sm.md` Step 5) — kept as
    /// the explicit low-durability/test/dev choice, and as the placeholder
    /// value here before `KvStoreRegistry::with_kv_engine` applies the
    /// CLI-parsed choice.
    Memory,
    /// Durable crowtree file under the registry's `data_root`, one file per
    /// `(store_id, group_id)` — see `startup::store_crowtree_path`.
    Crowtree,
}

impl KvEngineKind {
    /// Parse the `--kv-engine` CLI value (`clap`'s `value_parser` already
    /// restricts it to `["memory", "crowtree"]`, so any other input is a
    /// caller bug, not a user-facing error path).
    #[must_use]
    pub fn parse(s: &str) -> Self {
        match s {
            "crowtree" => Self::Crowtree,
            _ => Self::Memory,
        }
    }
}

/// Parse the `--kv-backend` CLI value (`clap`'s `value_parser` already
/// restricts it to `["file", "block"]`) into the FFI's [`CrowtreeBackend`].
/// Only meaningful when `KvEngineKind::Crowtree` is selected.
#[must_use]
pub fn parse_crowtree_backend(s: &str) -> CrowtreeBackend {
    match s {
        "block" => CrowtreeBackend::Block,
        _ => CrowtreeBackend::File,
    }
}

pub struct KvStoreRegistry {
    pub stores: DashMap<u64, Arc<PxKvStore>>,
    pub election_cfg: PxElectionConfig,
    pub wal_root: PathBuf,
    pub config_root: PathBuf,
    pub wal_backend: Arc<IoBackend>,
    pub kv_engine: KvEngineKind,
    /// Root directory for durable per-group crowtree files. Only read when
    /// `kv_engine == KvEngineKind::Crowtree`.
    pub data_root: PathBuf,
    /// Durable backend for the crowtree engine (plan-tree #22). Only read
    /// when `kv_engine == KvEngineKind::Crowtree`.
    pub crowtree_backend: CrowtreeBackend,
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
        let data_root = wal_root
            .parent()
            .map_or_else(|| PathBuf::from("ctdata"), |p| p.join("ctdata"));
        Self {
            stores: DashMap::new(),
            election_cfg,
            wal_root,
            config_root,
            wal_backend,
            kv_engine: KvEngineKind::Memory,
            data_root,
            crowtree_backend: CrowtreeBackend::File,
            port_pool: Mutex::new(Vec::new()),
        }
    }

    /// Builder-style setter for [`Self::kv_engine`] / [`Self::data_root`],
    /// used by `main.rs` right after construction (mirrors
    /// [`Self::set_port_pool`]'s pattern of a post-construction CLI-driven
    /// override rather than a longer `with_runtime` parameter list).
    #[must_use]
    pub fn with_kv_engine(mut self, kv_engine: KvEngineKind, data_root: PathBuf) -> Self {
        self.kv_engine = kv_engine;
        self.data_root = data_root;
        self
    }

    /// Builder-style setter for [`Self::crowtree_backend`] (plan-tree #22),
    /// used by `main.rs` right after construction alongside
    /// [`Self::with_kv_engine`].
    #[must_use]
    pub fn with_crowtree_backend(mut self, crowtree_backend: CrowtreeBackend) -> Self {
        self.crowtree_backend = crowtree_backend;
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
