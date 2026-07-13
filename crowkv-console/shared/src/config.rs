//! Console configuration: persisted registry of `crowkv-server` instances.
//!
//! C2 status: file-backed `[[server]]` list; later phases extend with
//! racks, nodes, ssh creds. The struct is the single source of truth so
//! the storage format can evolve without touching call sites.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::error::{Error, Result};

use std::fmt;

pub trait ConsoleConfigEngine: Send + Sync {
    /// Load the console configuration from the engine's storage.
    ///
    /// # Errors
    /// Returns an error if loading fails (e.g., file not found, parse error).
    fn load(&self) -> Result<ConsoleConfig>;

    /// Save the console configuration to the engine's storage.
    ///
    /// # Errors
    /// Returns an error if saving fails (e.g., permission denied, write error).
    fn save(&self, config: &ConsoleConfig) -> Result<()>;
}

#[derive(Clone, PartialEq, Eq)]
pub struct TomlFileEngine {
    path: PathBuf,
}

impl fmt::Debug for TomlFileEngine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TomlFileEngine")
            .field("path", &self.path)
            .finish()
    }
}

impl TomlFileEngine {
    #[must_use]
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    #[must_use]
    pub fn default_path() -> Option<PathBuf> {
        Some(PathBuf::from("runtime-data/crowkv.db.toml"))
    }

    #[must_use]
    pub fn from_default_path() -> Option<Self> {
        Self::default_path().map(Self::new)
    }
}

impl ConsoleConfigEngine for TomlFileEngine {
    fn load(&self) -> Result<ConsoleConfig> {
        match std::fs::read_to_string(&self.path) {
            Ok(body) => ConsoleConfig::from_toml_str(&body, &self.path),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(ConsoleConfig::default()),
            Err(e) => Err(Error::Io(e)),
        }
    }

    fn save(&self, config: &ConsoleConfig) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent).map_err(Error::Io)?;
        }
        let body = config.to_toml_string()?;
        let tmp = self.path.with_extension("toml.tmp");
        std::fs::write(&tmp, body).map_err(Error::Io)?;
        std::fs::rename(&tmp, &self.path).map_err(Error::Io)?;
        Ok(())
    }
}

/// On-disk console config. New top-level fields land in later phases
/// (ssh defaults, etc.). Unknown fields are ignored on load and dropped
/// on save (`serde(default)` everywhere) to keep migrations easy.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsoleConfig {
    #[serde(default, rename = "rack")]
    pub racks: Vec<RackEntry>,
    #[serde(default, rename = "node")]
    pub nodes: Vec<NodeEntry>,
    #[serde(default, rename = "server")]
    pub servers: Vec<ServerEntry>,
    #[serde(default)]
    pub stores: Vec<StoreEntry>,
    #[serde(default)]
    pub groups: Vec<GroupEntry>,
    /// Optional `[bench]` section. The bench engine's built-in scenarios
    /// stay authoritative; entries here overlay them field-by-field.
    #[serde(default, skip_serializing_if = "BenchConfig::is_empty")]
    pub bench: BenchConfig,
}

/// `[bench]` section. Currently only carries user-defined / user-tweaked
/// stress scenarios (`[bench.stress.<name>]`); future knobs (default
/// reporting dir, max threads, etc.) belong here too.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BenchConfig {
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub stress: std::collections::BTreeMap<String, StressScenarioOverride>,
}

impl BenchConfig {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.stress.is_empty()
    }
}

/// User-supplied overlay for a stress scenario. Every field is optional;
/// missing fields fall through to the built-in defaults (or, for
/// brand-new names, to `BenchConfig::defaults`).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StressScenarioOverride {
    /// `"read" | "write" | "list" | "mix"`. Case-insensitive.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workload: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub threads: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connections: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_secs: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_space: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value_size: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RackEntry {
    pub id: String,
    #[serde(default)]
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeEntry {
    pub id: String,
    pub rack_id: String,
    /// Default `127.0.0.1` for local simulated nodes.
    pub host: String,
    /// SSH port. Defaults to 22.
    #[serde(default = "default_ssh_port")]
    pub ssh_port: u16,
    /// SSH user for lifecycle ops. Empty string disables SSH and falls
    /// back to local-fork lifecycle (C3 path) for tests.
    #[serde(default)]
    pub ssh_user: String,
    /// Optional explicit private-key path. `None` falls back to
    /// `~/.ssh/id_ed25519` then `~/.ssh/id_rsa`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ssh_key: Option<String>,
    /// Optional password for password auth. Mutually exclusive with
    /// `ssh_key`. Plaintext on disk — operators are expected to rely on
    /// key auth in practice.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ssh_password: Option<String>,
}

fn default_ssh_port() -> u16 {
    22
}

impl NodeEntry {
    /// `true` if this node is configured to use SSH for lifecycle ops.
    #[must_use]
    pub fn ssh_enabled(&self) -> bool {
        !self.ssh_user.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServerEntry {
    /// Console-side identifier; must be unique within the file.
    pub id: String,
    /// `crowkv-server` management base URL, e.g. `http://127.0.0.1:9910`.
    pub url: String,
    /// Owning node id; populated for console-deployed instances. `None`
    /// for plain "registered external server" entries from C2.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_id: Option<String>,
    /// gRPC base URL, e.g. `http://127.0.0.1:28001`. Populated for
    /// console-deployed instances.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grpc_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mgmt_port: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grpc_port: Option<u16>,
    #[serde(default)]
    pub auto_start: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub binary: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub election_profile: Option<String>,
    #[serde(default, skip_deserializing, skip_serializing_if = "Option::is_none")]
    pub pid: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoreEntry {
    pub store_id: u64,
    #[serde(default)]
    pub nodes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupEntry {
    pub store_id: u64,
    pub group_id: u64,
    #[serde(default)]
    pub replicas: Vec<ReplicaEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicaEntry {
    pub replica_id: u64,
    pub node_id: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
struct PersistedConsoleConfig {
    #[serde(default)]
    version: u32,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    rack: BTreeMap<String, PersistedRackEntry>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    node: BTreeMap<String, PersistedNodeEntry>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    crowkv_server: BTreeMap<String, PersistedServerEntry>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    store: BTreeMap<String, PersistedStoreEntry>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    group: BTreeMap<String, PersistedGroupEntry>,
    #[serde(default, skip_serializing_if = "BenchConfig::is_empty")]
    bench: BenchConfig,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
struct PersistedRackEntry {
    #[serde(default)]
    name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PersistedNodeEntry {
    rack_id: String,
    host: String,
    #[serde(default = "default_ssh_port")]
    ssh_port: u16,
    #[serde(default)]
    ssh_user: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    ssh_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    ssh_password: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PersistedServerEntry {
    node_id: Option<String>,
    url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    grpc_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    mgmt_port: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    grpc_port: Option<u16>,
    #[serde(default)]
    auto_start: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    binary: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    election_profile: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PersistedStoreEntry {
    store_id: u64,
    #[serde(default)]
    nodes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PersistedGroupEntry {
    store_id: u64,
    group_id: u64,
    #[serde(default)]
    replicas: Vec<ReplicaEntry>,
}

impl ServerEntry {
    /// Convenience constructor for a plain registered server (C2 style).
    #[must_use]
    pub fn new(id: impl Into<String>, url: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            url: url.into(),
            node_id: None,
            grpc_url: None,
            mgmt_port: None,
            grpc_port: None,
            auto_start: false,
            binary: None,
            election_profile: None,
            pid: None,
        }
    }
}

impl ConsoleConfig {
    /// Default config file path.
    ///
    /// Config is persisted to `runtime-data/crowkv.db.toml` in the project root.
    /// This file stores registered crowkv-server instances for the console.
    #[must_use]
    pub fn default_path() -> Option<PathBuf> {
        TomlFileEngine::default_path()
    }

    /// Load the config from `path`. A missing file yields a default
    /// (empty) config so first-run is friendly.
    ///
    /// # Errors
    /// Returns `Error::Io` for non-`NotFound` filesystem errors and
    /// `Error::Config` for TOML parse failures.
    pub fn load(path: &Path) -> Result<Self> {
        TomlFileEngine::new(path).load()
    }

    /// Save the config atomically (write to a tempfile, then rename).
    ///
    /// # Errors
    /// Filesystem and TOML serialization errors are propagated.
    pub fn save(&self, path: &Path) -> Result<()> {
        TomlFileEngine::new(path).save(self)
    }

    /// Load configuration using the provided engine.
    ///
    /// # Errors
    /// Returns an error if the engine's load fails.
    pub fn load_with_engine(engine: &dyn ConsoleConfigEngine) -> Result<Self> {
        engine.load()
    }

    /// Save configuration using the provided engine.
    ///
    /// # Errors
    /// Returns an error if the engine's save fails.
    pub fn save_with_engine(&self, engine: &dyn ConsoleConfigEngine) -> Result<()> {
        engine.save(self)
    }

    /// Add a server entry. Rejects duplicate `id` and duplicate `url`.
    ///
    /// # Errors
    /// Returns `Error::Conflict` on duplicate id; `Error::Validation` on
    /// duplicate url.
    pub fn add_server(&mut self, entry: ServerEntry) -> Result<()> {
        if self.servers.iter().any(|s| s.id == entry.id) {
            return Err(Error::Conflict {
                kind: "server".into(),
                id: entry.id,
            });
        }
        if self.servers.iter().any(|s| s.url == entry.url) {
            return Err(Error::Validation {
                field: "url".into(),
                message: format!("url {} already registered", entry.url),
            });
        }
        self.servers.push(entry);
        Ok(())
    }

    /// Remove a server entry by id.
    ///
    /// # Errors
    /// Returns `Error::NotFound` if no entry has that id.
    pub fn remove_server(&mut self, id: &str) -> Result<ServerEntry> {
        let pos = self
            .servers
            .iter()
            .position(|s| s.id == id)
            .ok_or_else(|| Error::NotFound {
                kind: "server".into(),
                id: id.to_string(),
            })?;
        Ok(self.servers.remove(pos))
    }

    /// All server URLs in registration order.
    #[must_use]
    pub fn server_urls(&self) -> Vec<String> {
        self.servers.iter().map(|s| s.url.clone()).collect()
    }

    pub fn record_store(&mut self, store_id: u64, mut nodes: Vec<String>) {
        nodes.sort();
        nodes.dedup();
        if let Some(store) = self.stores.iter_mut().find(|s| s.store_id == store_id) {
            store.nodes = nodes;
        } else {
            self.stores.push(StoreEntry { store_id, nodes });
        }
        self.stores.sort_by_key(|s| s.store_id);
    }

    pub fn ensure_store_node(&mut self, store_id: u64, node_id: &str) {
        if let Some(store) = self.stores.iter_mut().find(|s| s.store_id == store_id) {
            if !store.nodes.iter().any(|n| n == node_id) {
                store.nodes.push(node_id.to_string());
                store.nodes.sort();
            }
        } else {
            self.stores.push(StoreEntry {
                store_id,
                nodes: vec![node_id.to_string()],
            });
            self.stores.sort_by_key(|s| s.store_id);
        }
    }

    pub fn remove_store_record(&mut self, store_id: u64) {
        self.stores.retain(|s| s.store_id != store_id);
        self.groups.retain(|g| g.store_id != store_id);
    }

    pub fn record_group(&mut self, store_id: u64, group_id: u64, mut replicas: Vec<ReplicaEntry>) {
        replicas.sort_by_key(|r| r.replica_id);
        if let Some(group) = self
            .groups
            .iter_mut()
            .find(|g| g.store_id == store_id && g.group_id == group_id)
        {
            group.replicas = replicas;
        } else {
            self.groups.push(GroupEntry {
                store_id,
                group_id,
                replicas,
            });
        }
        self.groups.sort_by_key(|g| (g.store_id, g.group_id));
    }

    pub fn remove_group_record(&mut self, store_id: u64, group_id: u64) {
        self.groups
            .retain(|g| !(g.store_id == store_id && g.group_id == group_id));
    }

    pub fn add_group_replica(&mut self, store_id: u64, group_id: u64, replica: ReplicaEntry) {
        if let Some(group) = self
            .groups
            .iter_mut()
            .find(|g| g.store_id == store_id && g.group_id == group_id)
        {
            if let Some(existing) = group
                .replicas
                .iter_mut()
                .find(|r| r.replica_id == replica.replica_id)
            {
                *existing = replica;
            } else {
                group.replicas.push(replica);
                group.replicas.sort_by_key(|r| r.replica_id);
            }
        } else {
            self.groups.push(GroupEntry {
                store_id,
                group_id,
                replicas: vec![replica],
            });
            self.groups.sort_by_key(|g| (g.store_id, g.group_id));
        }
    }

    pub fn remove_group_replica(&mut self, store_id: u64, group_id: u64, replica_id: u64) {
        if let Some(group) = self
            .groups
            .iter_mut()
            .find(|g| g.store_id == store_id && g.group_id == group_id)
        {
            group.replicas.retain(|r| r.replica_id != replica_id);
        }
        self.groups
            .retain(|g| !(g.store_id == store_id && g.group_id == group_id && g.replicas.is_empty()));
    }

    #[must_use]
    pub fn group(&self, store_id: u64, group_id: u64) -> Option<&GroupEntry> {
        self.groups
            .iter()
            .find(|g| g.store_id == store_id && g.group_id == group_id)
    }

    pub fn purge_node_topology(&mut self, node_id: &str) {
        for store in &mut self.stores {
            store.nodes.retain(|n| n != node_id);
        }
        self.stores.retain(|s| !s.nodes.is_empty());
        for group in &mut self.groups {
            group.replicas.retain(|r| r.node_id != node_id);
        }
        self.groups.retain(|g| !g.replicas.is_empty());
    }

    /// Add a rack. Rejects duplicate id.
    ///
    /// # Errors
    /// `Error::Conflict` on duplicate id.
    pub fn add_rack(&mut self, entry: RackEntry) -> Result<()> {
        if self.racks.iter().any(|r| r.id == entry.id) {
            return Err(Error::Conflict {
                kind: "rack".into(),
                id: entry.id,
            });
        }
        self.racks.push(entry);
        Ok(())
    }

    /// Remove a rack by id.
    ///
    /// # Errors
    /// `Error::NotFound` if no rack with that id; `Error::Conflict` if any
    /// node still references the rack.
    pub fn remove_rack(&mut self, id: &str) -> Result<RackEntry> {
        if self.nodes.iter().any(|n| n.rack_id == id) {
            return Err(Error::Conflict {
                kind: "rack".into(),
                id: format!("{id}: rack still referenced by nodes"),
            });
        }
        let pos = self
            .racks
            .iter()
            .position(|r| r.id == id)
            .ok_or_else(|| Error::NotFound {
                kind: "rack".into(),
                id: id.to_string(),
            })?;
        Ok(self.racks.remove(pos))
    }

    /// Add a node. Rejects duplicate id and unknown rack.
    ///
    /// # Errors
    /// `Error::Conflict` on duplicate id; `Error::Validation` on unknown rack.
    pub fn add_node(&mut self, entry: NodeEntry) -> Result<()> {
        if self.nodes.iter().any(|n| n.id == entry.id) {
            return Err(Error::Conflict {
                kind: "node".into(),
                id: entry.id,
            });
        }
        if !self.racks.iter().any(|r| r.id == entry.rack_id) {
            return Err(Error::Validation {
                field: "rack_id".into(),
                message: format!("unknown rack {}", entry.rack_id),
            });
        }
        self.nodes.push(entry);
        Ok(())
    }

    /// Remove a node by id.
    ///
    /// # Errors
    /// `Error::NotFound` if no node; `Error::Conflict` if a server is
    /// still deployed to the node.
    pub fn remove_node(&mut self, id: &str) -> Result<NodeEntry> {
        if self.servers.iter().any(|s| s.node_id.as_deref() == Some(id)) {
            return Err(Error::Conflict {
                kind: "node".into(),
                id: format!("{id}: node still hosts a deployed server"),
            });
        }
        let pos = self
            .nodes
            .iter()
            .position(|n| n.id == id)
            .ok_or_else(|| Error::NotFound {
                kind: "node".into(),
                id: id.to_string(),
            })?;
        Ok(self.nodes.remove(pos))
    }

    /// Look up a node by id.
    #[must_use]
    pub fn node(&self, id: &str) -> Option<&NodeEntry> {
        self.nodes.iter().find(|n| n.id == id)
    }

    /// Look up a server entry by id.
    #[must_use]
    pub fn server(&self, id: &str) -> Option<&ServerEntry> {
        self.servers.iter().find(|s| s.id == id)
    }

    /// Look up the server deployed on a given node.
    #[must_use]
    pub fn server_for_node(&self, node_id: &str) -> Option<&ServerEntry> {
        self.servers
            .iter()
            .find(|s| s.node_id.as_deref() == Some(node_id))
    }

    /// Remove the server entry deployed on a given node.
    ///
    /// # Errors
    /// `Error::NotFound` if no server is deployed on this node.
    pub fn remove_server_for_node(&mut self, node_id: &str) -> Result<ServerEntry> {
        let pos = self
            .servers
            .iter()
            .position(|s| s.node_id.as_deref() == Some(node_id))
            .ok_or_else(|| Error::NotFound {
                kind: "server".into(),
                id: format!("no server on node {node_id}"),
            })?;
        Ok(self.servers.remove(pos))
    }

    /// Mutable look-up for in-place updates (e.g. `pid` after restart).
    #[must_use]
    pub fn server_mut(&mut self, id: &str) -> Option<&mut ServerEntry> {
        self.servers.iter_mut().find(|s| s.id == id)
    }

    fn to_persisted(&self) -> PersistedConsoleConfig {
        let rack = self
            .racks
            .iter()
            .map(|entry| {
                (
                    entry.id.clone(),
                    PersistedRackEntry {
                        name: entry.name.clone(),
                    },
                )
            })
            .collect();
        let node = self
            .nodes
            .iter()
            .map(|entry| {
                (
                    entry.id.clone(),
                    PersistedNodeEntry {
                        rack_id: entry.rack_id.clone(),
                        host: entry.host.clone(),
                        ssh_port: entry.ssh_port,
                        ssh_user: entry.ssh_user.clone(),
                        ssh_key: entry.ssh_key.clone(),
                        ssh_password: entry.ssh_password.clone(),
                    },
                )
            })
            .collect();
        let crowkv_server = self
            .servers
            .iter()
            .map(|entry| {
                (
                    entry.id.clone(),
                    PersistedServerEntry {
                        node_id: entry.node_id.clone(),
                        url: entry.url.clone(),
                        grpc_url: entry.grpc_url.clone(),
                        mgmt_port: entry.mgmt_port,
                        grpc_port: entry.grpc_port,
                        auto_start: entry.auto_start,
                        binary: entry.binary.clone(),
                        election_profile: entry.election_profile.clone(),
                    },
                )
            })
            .collect();
        let store = self
            .stores
            .iter()
            .map(|entry| {
                (
                    entry.store_id.to_string(),
                    PersistedStoreEntry {
                        store_id: entry.store_id,
                        nodes: entry.nodes.clone(),
                    },
                )
            })
            .collect();
        let group = self
            .groups
            .iter()
            .map(|entry| {
                (
                    format!("{}:{}", entry.store_id, entry.group_id),
                    PersistedGroupEntry {
                        store_id: entry.store_id,
                        group_id: entry.group_id,
                        replicas: entry.replicas.clone(),
                    },
                )
            })
            .collect();
        PersistedConsoleConfig {
            version: 2,
            rack,
            node,
            crowkv_server,
            store,
            group,
            bench: self.bench.clone(),
        }
    }

    fn from_persisted(persisted: PersistedConsoleConfig) -> Self {
        let mut racks: Vec<RackEntry> = persisted
            .rack
            .into_iter()
            .map(|(id, entry)| RackEntry { id, name: entry.name })
            .collect();
        racks.sort_by(|a, b| a.id.cmp(&b.id));
        let mut nodes: Vec<NodeEntry> = persisted
            .node
            .into_iter()
            .map(|(id, entry)| NodeEntry {
                id,
                rack_id: entry.rack_id,
                host: entry.host,
                ssh_port: entry.ssh_port,
                ssh_user: entry.ssh_user,
                ssh_key: entry.ssh_key,
                ssh_password: entry.ssh_password,
            })
            .collect();
        nodes.sort_by(|a, b| a.id.cmp(&b.id));
        let mut servers: Vec<ServerEntry> = persisted
            .crowkv_server
            .into_iter()
            .map(|(id, entry)| ServerEntry {
                id,
                url: entry.url,
                node_id: entry.node_id,
                grpc_url: entry.grpc_url,
                mgmt_port: entry.mgmt_port,
                grpc_port: entry.grpc_port,
                auto_start: entry.auto_start,
                binary: entry.binary,
                election_profile: entry.election_profile,
                pid: None,
            })
            .collect();
        servers.sort_by(|a, b| a.id.cmp(&b.id));
        let mut stores: Vec<StoreEntry> = persisted
            .store
            .into_values()
            .map(|entry| StoreEntry {
                store_id: entry.store_id,
                nodes: entry.nodes,
            })
            .collect();
        stores.sort_by_key(|s| s.store_id);
        let mut groups: Vec<GroupEntry> = persisted
            .group
            .into_values()
            .map(|entry| GroupEntry {
                store_id: entry.store_id,
                group_id: entry.group_id,
                replicas: entry.replicas,
            })
            .collect();
        groups.sort_by_key(|g| (g.store_id, g.group_id));
        Self {
            racks,
            nodes,
            servers,
            stores,
            groups,
            bench: persisted.bench,
        }
    }

    fn from_toml_str(body: &str, path: &Path) -> Result<Self> {
        let persisted: PersistedConsoleConfig =
            toml::from_str(body).map_err(|e| Error::Config(format!("{}: {e}", path.display())))?;
        Ok(Self::from_persisted(persisted))
    }

    fn to_toml_string(&self) -> Result<String> {
        toml::to_string_pretty(&self.to_persisted()).map_err(|e| Error::Config(format!("serialize: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::{ConsoleConfig, GroupEntry, ReplicaEntry, ServerEntry, StoreEntry, TomlFileEngine};

    #[test]
    fn round_trip_load_save() {
        let dir = tempdir();
        let path = dir.join("console.toml");

        let mut cfg = ConsoleConfig::default();
        let mut a = ServerEntry::new("a", "http://127.0.0.1:9910");
        a.node_id = Some("n1".into());
        a.grpc_url = Some("http://127.0.0.1:9921".into());
        a.mgmt_port = Some(9910);
        a.grpc_port = Some(9921);
        a.auto_start = true;
        a.election_profile = Some("test".into());
        a.pid = Some(12345);
        cfg.add_server(a).unwrap();
        cfg.add_server(ServerEntry::new("b", "http://127.0.0.1:9911"))
            .unwrap();
        cfg.stores.push(StoreEntry {
            store_id: 7,
            nodes: vec!["n1".into(), "n2".into()],
        });
        cfg.groups.push(GroupEntry {
            store_id: 7,
            group_id: 70,
            replicas: vec![
                ReplicaEntry {
                    replica_id: 700,
                    node_id: "n1".into(),
                },
                ReplicaEntry {
                    replica_id: 701,
                    node_id: "n2".into(),
                },
            ],
        });

        cfg.save(&path).unwrap();
        let loaded = ConsoleConfig::load(&path).unwrap();
        let mut expected = cfg.clone();
        expected.servers[0].pid = None;
        assert_eq!(expected, loaded);
    }

    #[test]
    fn pid_is_not_persisted_to_disk() {
        let dir = tempdir();
        let path = dir.join("console.toml");

        let mut cfg = ConsoleConfig::default();
        let mut entry = ServerEntry::new("a", "http://127.0.0.1:9910");
        entry.pid = Some(4242);
        cfg.add_server(entry).unwrap();

        cfg.save(&path).unwrap();
        let raw = std::fs::read_to_string(&path).unwrap();
        assert!(!raw.contains("pid"), "runtime pid must not be persisted: {raw}");
    }

    #[test]
    fn missing_file_yields_default() {
        let dir = tempdir();
        let path = dir.join("nope.toml");
        let cfg = ConsoleConfig::load(&path).unwrap();
        assert!(cfg.servers.is_empty());
    }

    #[test]
    fn toml_engine_round_trip() {
        let dir = tempdir();
        let path = dir.join("engine.toml");
        let engine = TomlFileEngine::new(path.clone());

        let mut cfg = ConsoleConfig::default();
        cfg.add_server(ServerEntry::new("a", "http://127.0.0.1:9910"))
            .unwrap();

        cfg.save_with_engine(&engine).unwrap();
        let loaded = ConsoleConfig::load_with_engine(&engine).unwrap();

        assert_eq!(cfg, loaded);
    }

    #[test]
    fn default_path_points_to_runtime_data() {
        assert_eq!(
            TomlFileEngine::default_path().unwrap(),
            std::path::PathBuf::from("runtime-data/crowkv.db.toml")
        );
        assert_eq!(
            ConsoleConfig::default_path().unwrap(),
            std::path::PathBuf::from("runtime-data/crowkv.db.toml")
        );
    }

    #[test]
    fn duplicate_id_rejected() {
        let mut cfg = ConsoleConfig::default();
        cfg.add_server(ServerEntry::new("a", "http://1")).unwrap();
        let err = cfg.add_server(ServerEntry::new("a", "http://2")).unwrap_err();
        assert!(matches!(err, crate::error::Error::Conflict { .. }));
    }

    #[test]
    fn duplicate_url_rejected() {
        let mut cfg = ConsoleConfig::default();
        cfg.add_server(ServerEntry::new("a", "http://1")).unwrap();
        let err = cfg.add_server(ServerEntry::new("b", "http://1")).unwrap_err();
        assert!(matches!(err, crate::error::Error::Validation { .. }));
    }

    #[test]
    fn remove_missing_is_not_found() {
        let mut cfg = ConsoleConfig::default();
        let err = cfg.remove_server("ghost").unwrap_err();
        assert!(matches!(err, crate::error::Error::NotFound { .. }));
    }

    fn tempdir() -> std::path::PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let base = std::env::temp_dir();
        let unique = format!(
            "crowkv-console-cfg-{}-{}-{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let dir = base.join(unique);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }
}
