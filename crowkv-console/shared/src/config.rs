//! Console configuration: persisted registry of `crowkv-server` instances.
//!
//! C2 status: file-backed `[[server]]` list; later phases extend with
//! racks, nodes, ssh creds. The struct is the single source of truth so
//! the storage format can evolve without touching call sites.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

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
    /// Last-known OS pid. Possibly stale; verify before issuing a stop.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pid: Option<u32>,
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
            pid: None,
        }
    }
}

impl ConsoleConfig {
    /// Default config file path.
    ///
    /// Config is persisted to `log/crowkv-console-db.toml` in the project root.
    /// This file stores registered crowkv-server instances for the console.
    #[must_use]
    pub fn default_path() -> Option<PathBuf> {
        Some(PathBuf::from("log/crowkv-console-db.toml"))
    }

    /// Load the config from `path`. A missing file yields a default
    /// (empty) config so first-run is friendly.
    ///
    /// # Errors
    /// Returns `Error::Io` for non-`NotFound` filesystem errors and
    /// `Error::Config` for TOML parse failures.
    pub fn load(path: &Path) -> Result<Self> {
        match std::fs::read_to_string(path) {
            Ok(s) => toml::from_str(&s).map_err(|e| Error::Config(format!("{}: {e}", path.display()))),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
            Err(e) => Err(Error::Io(e)),
        }
    }

    /// Save the config atomically (write to a tempfile, then rename).
    ///
    /// # Errors
    /// Filesystem and TOML serialization errors are propagated.
    pub fn save(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(Error::Io)?;
        }
        let body = toml::to_string_pretty(self).map_err(|e| Error::Config(format!("serialize: {e}")))?;
        let tmp = path.with_extension("toml.tmp");
        std::fs::write(&tmp, body).map_err(Error::Io)?;
        std::fs::rename(&tmp, path).map_err(Error::Io)?;
        Ok(())
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
        let pos = self.servers.iter().position(|s| s.id == id).ok_or_else(|| Error::NotFound {
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
        let pos = self.racks.iter().position(|r| r.id == id).ok_or_else(|| Error::NotFound {
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
        let pos = self.nodes.iter().position(|n| n.id == id).ok_or_else(|| Error::NotFound {
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
        self.servers.iter().find(|s| s.node_id.as_deref() == Some(node_id))
    }

    /// Remove the server entry deployed on a given node.
    ///
    /// # Errors
    /// `Error::NotFound` if no server is deployed on this node.
    pub fn remove_server_for_node(&mut self, node_id: &str) -> Result<ServerEntry> {
        let pos = self.servers.iter().position(|s| s.node_id.as_deref() == Some(node_id)).ok_or_else(|| Error::NotFound {
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
}

#[cfg(test)]
mod tests {
    use super::{ConsoleConfig, ServerEntry};

    #[test]
    fn round_trip_load_save() {
        let dir = tempdir();
        let path = dir.join("console.toml");

        let mut cfg = ConsoleConfig::default();
        cfg.add_server(ServerEntry::new("a", "http://127.0.0.1:9910")).unwrap();
        cfg.add_server(ServerEntry::new("b", "http://127.0.0.1:9911")).unwrap();

        cfg.save(&path).unwrap();
        let loaded = ConsoleConfig::load(&path).unwrap();
        assert_eq!(cfg, loaded);
    }

    #[test]
    fn missing_file_yields_default() {
        let dir = tempdir();
        let path = dir.join("nope.toml");
        let cfg = ConsoleConfig::load(&path).unwrap();
        assert!(cfg.servers.is_empty());
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
        let base = std::env::temp_dir();
        let unique = format!(
            "crowkv-console-cfg-{}-{}",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        );
        let dir = base.join(unique);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }
}
