// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Per-node configuration cache: single JSON file replacing per-group
//! `GroupConfigStore` files.
//!
//! `NodeConfig` is the durable, per-node snapshot of all stores and
//! groups hosted on this node, including their membership. On restart,
//! `node-config.json` is loaded to seed rebuilt groups so they do not
//! start as `quorum=1` singletons in the restore window.
//!
//! The file lives at `{config_root}/node-config.json`. All groups on
//! this node share one file, so a membership update for any group
//! triggers a read-modify-write of the single file.

use std::io;
use std::path::{Path, PathBuf};

use tokio::fs;
use tokio::io::AsyncWriteExt;

use serde::{Deserialize, Serialize};

use crate::cluster::group_config::{PxGroupConfig, PxGroupMember};

/// One group entry within a store entry in `node-config.json`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeGroupEntry {
    pub group_id: u64,
    pub replica_id: u64,
    pub members: Vec<PxGroupMember>,
    #[serde(default)]
    pub membership_epoch: u64,
    #[serde(default)]
    pub term: u64,
}

/// One store entry in `node-config.json`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeStoreEntry {
    pub store_id: u64,
    #[serde(default)]
    pub groups: Vec<NodeGroupEntry>,
}

/// The full per-node config cache, serialized as `conf/node-config.json`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeConfig {
    #[serde(default)]
    pub version: u32,
    #[serde(default)]
    pub stores: Vec<NodeStoreEntry>,
}

impl NodeConfig {
    /// Look up a store entry by `store_id`.
    #[must_use]
    pub fn store(&self, store_id: u64) -> Option<&NodeStoreEntry> {
        self.stores.iter().find(|s| s.store_id == store_id)
    }

    /// Look up a group entry by `(store_id, group_id)`.
    #[must_use]
    pub fn group(&self, store_id: u64, group_id: u64) -> Option<&NodeGroupEntry> {
        self.store(store_id)
            .and_then(|s| s.groups.iter().find(|g| g.group_id == group_id))
    }

    /// Upsert a group's config into the node config. Creates the store
    /// entry if it does not exist yet.
    ///
    /// # Panics
    /// Panics if the store entry cannot be found after creation (impossible
    /// by construction).
    pub fn upsert_group(&mut self, store_id: u64, config: &PxGroupConfig, replica_id: u64) {
        if !self.stores.iter().any(|s| s.store_id == store_id) {
            self.stores.push(NodeStoreEntry {
                store_id,
                groups: Vec::new(),
            });
        }
        let store_entry = self
            .stores
            .iter_mut()
            .find(|s| s.store_id == store_id)
            .expect("just pushed or existed");
        if let Some(existing) = store_entry
            .groups
            .iter_mut()
            .find(|g| g.group_id == config.group_id)
        {
            existing.members.clone_from(&config.members);
            existing.membership_epoch = config.membership_epoch;
            existing.term = config.term;
            existing.replica_id = replica_id;
        } else {
            store_entry.groups.push(NodeGroupEntry {
                group_id: config.group_id,
                replica_id,
                members: config.members.clone(),
                membership_epoch: config.membership_epoch,
                term: config.term,
            });
        }
    }

    /// Remove a group entry. Returns `true` if it was present.
    pub fn remove_group(&mut self, store_id: u64, group_id: u64) -> bool {
        if let Some(store_entry) = self.stores.iter_mut().find(|s| s.store_id == store_id) {
            let before = store_entry.groups.len();
            store_entry.groups.retain(|g| g.group_id != group_id);
            return store_entry.groups.len() != before;
        }
        false
    }

    /// Remove a store entry. Returns `true` if it was present.
    pub fn remove_store(&mut self, store_id: u64) -> bool {
        let before = self.stores.len();
        self.stores.retain(|s| s.store_id != store_id);
        self.stores.len() != before
    }
}

/// File-based store for the per-node config cache.
///
/// The config file lives at `{config_root}/node-config.json`. All groups
/// on this node share one file. Writes are atomic (tmp + fsync + rename).
#[derive(Clone, Debug)]
pub struct NodeConfigStore {
    config_path: PathBuf,
}

impl NodeConfigStore {
    /// Create a store rooted at `config_root`. The file path is
    /// `{config_root}/node-config.json`.
    #[must_use]
    pub fn new(config_root: impl AsRef<Path>) -> Self {
        let config_path = config_root.as_ref().join("node-config.json");
        Self { config_path }
    }

    /// Path to the config file (for diagnostics / tests).
    #[must_use]
    #[allow(dead_code)]
    pub(crate) fn path(&self) -> &Path {
        &self.config_path
    }

    /// Load the full node config, or `NodeConfig::default()` if no file
    /// exists.
    ///
    /// # Errors
    /// Returns IO error if the file exists but cannot be read. Returns
    /// `InvalidData` if the JSON is corrupt.
    pub async fn load(&self) -> io::Result<NodeConfig> {
        match fs::read(&self.config_path).await {
            Ok(data) => {
                let config: NodeConfig = serde_json::from_slice(&data)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                Ok(config)
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(NodeConfig::default()),
            Err(e) => Err(e),
        }
    }

    /// Load the config for a specific group, or `None` if the file or
    /// the group entry does not exist.
    ///
    /// # Errors
    /// Returns IO error if the file exists but cannot be read.
    pub async fn load_group(&self, store_id: u64, group_id: u64) -> io::Result<Option<PxGroupConfig>> {
        let config = self.load().await?;
        Ok(config.group(store_id, group_id).map(|g| PxGroupConfig {
            group_id: g.group_id,
            term: g.term,
            members: g.members.clone(),
            membership_epoch: g.membership_epoch,
        }))
    }

    /// Atomically persist (upsert) a group's config into the node config
    /// file. Reads the current file (or starts fresh), updates the
    /// group entry, and writes back atomically.
    ///
    /// # Errors
    /// Returns IO error if read, write, sync, or rename fails.
    pub async fn save_group(&self, store_id: u64, config: &PxGroupConfig, replica_id: u64) -> io::Result<()> {
        let mut node_config = self.load().await.unwrap_or_default();
        node_config.upsert_group(store_id, config, replica_id);
        self.write_atomic(&node_config).await
    }

    /// Remove a group from the node config file. Idempotent.
    ///
    /// # Errors
    /// Returns IO error if read or write fails.
    pub async fn remove_group(&self, store_id: u64, group_id: u64) -> io::Result<()> {
        let mut node_config = self.load().await.unwrap_or_default();
        node_config.remove_group(store_id, group_id);
        self.write_atomic(&node_config).await
    }

    /// Remove a store from the node config file. Idempotent.
    ///
    /// # Errors
    /// Returns IO error if read or write fails.
    #[allow(dead_code)]
    pub(crate) async fn remove_store(&self, store_id: u64) -> io::Result<()> {
        let mut node_config = self.load().await.unwrap_or_default();
        node_config.remove_store(store_id);
        self.write_atomic(&node_config).await
    }

    /// Atomically write the full node config to disk.
    async fn write_atomic(&self, config: &NodeConfig) -> io::Result<()> {
        if let Some(parent) = self.config_path.parent() {
            fs::create_dir_all(parent).await?;
        }

        let payload =
            serde_json::to_vec_pretty(config).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        let tmp_path = {
            let mut p = self.config_path.clone();
            p.set_extension("json.tmp");
            p
        };

        {
            let mut file = fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&tmp_path)
                .await?;
            file.write_all(&payload).await?;
            file.flush().await?;
            file.sync_all().await?;
        }

        fs::rename(&tmp_path, &self.config_path).await?;

        if let Some(parent) = self.config_path.parent() {
            if let Ok(dir) = fs::File::open(parent).await {
                let _ = dir.sync_all().await;
            }
        }

        Ok(())
    }
}
