// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#![allow(clippy::missing_errors_doc, clippy::cast_possible_truncation)]

//! [`HardwareClient`]: group-0 hardware hierarchy + per-disk-group maps.
//!
//! Wraps a [`CrowkvClient`] pinned to group 0 (store 0, group 0). All
//! keys are text-path keys (see `design-crow-protocol-key.md` §5.2); all values
//! are JSON-encoded proto `*Value` types from `crow-protocol`.
//!
//! Writes are blind puts (no CAS); values are small (< 1 KB). Status
//! setters do a read-modify-write to preserve other fields and bump
//! `status_changed_at_ms`.
//!
//! See `doc/design/kv/design-crow-kv-group0.md` §2.1, §3.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use tracing::warn;

use crow_protocol::common::{HwStatus, NodeValue, RackValue};
use crow_protocol::common_type::{DiskGroupId, NodeId, RackId};
use crow_protocol::diskdb::rpc::{DiskGroupValue, DiskValue};
use crow_protocol::key::{
    BindMapKey, DiskGroupKey, DiskGroupUsageKey, DiskKey, NodeKey, OwnerMapKey, RackKey, TextKey,
};
use crow_protocol::sysdata::{DiskGroupEntry, DiskdbOwnerEntry, KVGroupBindEntry};

use crate::client::{GetOutcome, ScanOutcome};
use crate::{CrowkvClient, Error, Result};

/// Group-0 store/group IDs (the system group).
const G0_STORE: u64 = 0;
const G0_GROUP: u64 = 0;

/// Client for the hardware hierarchy and per-disk-group maps in
/// group 0.
///
/// All methods target store 0, group 0. The wrapped `CrowkvClient`
/// must have its topology seeded with a group-0 leader endpoint
/// (via `seed_leader(0, 0, endpoint)` or `/topology` discovery).
pub struct HardwareClient {
    kv: Arc<CrowkvClient>,
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

// ── helpers ─────────────────────────────────────────────────────

async fn put_json<T: serde::Serialize>(kv: &CrowkvClient, key: &str, value: &T) -> Result<()> {
    let payload = serde_json::to_vec(value).map_err(|e| Error::SysdataDecode {
        key: key.to_string(),
        reason: e.to_string(),
    })?;
    kv.put(G0_STORE, G0_GROUP, key.as_bytes(), &payload, None)
        .await
        .map(|_| ())
}

async fn get_json<T: serde::de::DeserializeOwned>(kv: &CrowkvClient, key: &str) -> Result<Option<T>> {
    match kv
        .get(
            G0_STORE,
            G0_GROUP,
            key.as_bytes(),
            crate::ReadMode::Linearizable,
            None,
        )
        .await?
    {
        GetOutcome::Found { value, .. } => {
            let v: T = serde_json::from_slice(&value).map_err(|e| Error::SysdataDecode {
                key: key.to_string(),
                reason: e.to_string(),
            })?;
            Ok(Some(v))
        }
        GetOutcome::NotFound => Ok(None),
    }
}

pub(crate) async fn scan_prefix<T: serde::de::DeserializeOwned>(
    kv: &CrowkvClient,
    prefix: &str,
) -> Result<Vec<(String, T)>> {
    let mut out: Vec<(String, T)> = Vec::new();
    let mut start_after: Vec<u8> = Vec::new();
    loop {
        let ScanOutcome { items, truncated, .. } = kv
            .scan(
                G0_STORE,
                G0_GROUP,
                prefix.as_bytes(),
                &start_after,
                &[],
                0,
                crate::ReadMode::Linearizable,
                None,
                false,
                None,
            )
            .await?;
        for (k, v) in &items {
            let key_str = std::str::from_utf8(k)
                .map_err(|e| Error::SysdataDecode {
                    key: prefix.to_string(),
                    reason: e.to_string(),
                })?
                .to_string();
            let val: T = serde_json::from_slice(v).map_err(|e| Error::SysdataDecode {
                key: key_str.clone(),
                reason: e.to_string(),
            })?;
            out.push((key_str, val));
        }
        if !truncated || items.is_empty() {
            break;
        }
        // Use the last key as the next `start_after`.
        if let Some((last_key, _)) = items.last() {
            start_after = last_key.to_vec();
        } else {
            break;
        }
    }
    Ok(out)
}

// ── rack ────────────────────────────────────────────────────────

impl HardwareClient {
    /// Wrap a `CrowkvClient` for group-0 hardware access.
    #[must_use]
    pub fn new(kv: CrowkvClient) -> Self {
        Self { kv: Arc::new(kv) }
    }

    /// Wrap an already-shared `CrowkvClient` for group-0 hardware access.
    #[must_use]
    pub fn from_shared(kv: Arc<CrowkvClient>) -> Self {
        Self { kv }
    }

    /// Access the underlying `CrowkvClient`.
    #[must_use]
    pub fn kv(&self) -> &CrowkvClient {
        &self.kv
    }

    /// Add or replace a rack record.
    pub async fn add_rack(&self, rack_id: RackId, value: &RackValue) -> Result<()> {
        let key = RackKey { rack_id };
        put_json(&self.kv, &key.to_path(), value).await
    }

    /// Read a rack record.
    pub async fn get_rack(&self, rack_id: RackId) -> Result<Option<RackValue>> {
        let key = RackKey { rack_id };
        get_json(&self.kv, &key.to_path()).await
    }

    /// Remove a rack record.
    pub async fn remove_rack(&self, rack_id: RackId) -> Result<()> {
        let key = RackKey { rack_id };
        self.kv
            .delete(G0_STORE, G0_GROUP, key.to_path().as_bytes(), None)
            .await
            .map(|_| ())
    }

    /// List all rack records (prefix scan `/hw/rack/`).
    pub async fn list_racks(&self) -> Result<Vec<(RackId, RackValue)>> {
        let entries = scan_prefix::<RackValue>(&self.kv, &<RackKey as TextKey>::prefix_all()).await?;
        let mut out = Vec::with_capacity(entries.len());
        for (path, value) in entries {
            let k = RackKey::from_path(&path).map_err(|e| Error::SysdataKeyParse(e.to_string()))?;
            out.push((k.rack_id, value));
        }
        Ok(out)
    }

    /// Set a rack's status (read-modify-write, bumps
    /// `status_changed_at_ms` on the rack value — `RackValue` has no
    /// such field, so this is a blind put of the new status).
    pub async fn set_rack_status(&self, rack_id: RackId, status: HwStatus) -> Result<()> {
        let key = RackKey { rack_id };
        let mut value = get_json::<RackValue>(&self.kv, &key.to_path())
            .await?
            .unwrap_or_default();
        value.status = status as i32;
        put_json(&self.kv, &key.to_path(), &value).await
    }
}

// ── node ────────────────────────────────────────────────────────

impl HardwareClient {
    /// Add or replace a node record.
    pub async fn add_node(&self, rack_id: RackId, node_id: NodeId, value: &NodeValue) -> Result<()> {
        let key = NodeKey { rack_id, node_id };
        put_json(&self.kv, &key.to_path(), value).await
    }

    /// Read a node record.
    pub async fn get_node(&self, rack_id: RackId, node_id: NodeId) -> Result<Option<NodeValue>> {
        let key = NodeKey { rack_id, node_id };
        get_json(&self.kv, &key.to_path()).await
    }

    /// Remove a node record.
    pub async fn remove_node(&self, rack_id: RackId, node_id: NodeId) -> Result<()> {
        let key = NodeKey { rack_id, node_id };
        self.kv
            .delete(G0_STORE, G0_GROUP, key.to_path().as_bytes(), None)
            .await
            .map(|_| ())
    }

    /// List all node records (prefix scan `/hw/node/`).
    pub async fn list_nodes(&self) -> Result<Vec<(RackId, NodeId, NodeValue)>> {
        let entries = scan_prefix::<NodeValue>(&self.kv, &<NodeKey as TextKey>::prefix_all()).await?;
        let mut out = Vec::with_capacity(entries.len());
        for (path, value) in entries {
            let k = NodeKey::from_path(&path).map_err(|e| Error::SysdataKeyParse(e.to_string()))?;
            out.push((k.rack_id, k.node_id, value));
        }
        Ok(out)
    }

    /// List nodes in one rack (prefix scan `/hw/node/<rack_id>/`).
    pub async fn list_nodes_in_rack(&self, rack_id: RackId) -> Result<Vec<(NodeId, NodeValue)>> {
        let entries = scan_prefix::<NodeValue>(&self.kv, &NodeKey::text_prefix_for_rack(rack_id)).await?;
        let mut out = Vec::with_capacity(entries.len());
        for (path, value) in entries {
            let k = NodeKey::from_path(&path).map_err(|e| Error::SysdataKeyParse(e.to_string()))?;
            out.push((k.node_id, value));
        }
        Ok(out)
    }

    /// Set a node's status (read-modify-write, bumps
    /// `status_changed_at_ms`).
    pub async fn set_node_status(&self, rack_id: RackId, node_id: NodeId, status: HwStatus) -> Result<()> {
        let key = NodeKey { rack_id, node_id };
        let mut value = get_json::<NodeValue>(&self.kv, &key.to_path())
            .await?
            .unwrap_or_default();
        value.status = status as i32;
        value.status_changed_at_ms = now_ms();
        put_json(&self.kv, &key.to_path(), &value).await
    }
}

// ── disk-group ──────────────────────────────────────────────────

impl HardwareClient {
    /// Add or replace a disk-group record.
    pub async fn add_disk_group(
        &self,
        rack_id: RackId,
        node_id: NodeId,
        dg_id: DiskGroupId,
        value: &DiskGroupValue,
    ) -> Result<()> {
        let key = DiskGroupKey {
            rack_id,
            node_id,
            disk_group_id: dg_id,
        };
        put_json(&self.kv, &key.to_path(), value).await
    }

    /// Read a disk-group record. Returns a [`DiskGroupEntry`] with the
    /// key fields included.
    pub async fn get_disk_group(
        &self,
        rack_id: RackId,
        node_id: NodeId,
        dg_id: DiskGroupId,
    ) -> Result<Option<DiskGroupEntry>> {
        let key = DiskGroupKey {
            rack_id,
            node_id,
            disk_group_id: dg_id,
        };
        let value = get_json::<DiskGroupValue>(&self.kv, &key.to_path()).await?;
        Ok(value.map(|value| DiskGroupEntry {
            rack_id,
            node_id,
            dg_id,
            value,
        }))
    }

    /// Remove a disk-group record.
    pub async fn remove_disk_group(
        &self,
        rack_id: RackId,
        node_id: NodeId,
        dg_id: DiskGroupId,
    ) -> Result<()> {
        let key = DiskGroupKey {
            rack_id,
            node_id,
            disk_group_id: dg_id,
        };
        self.kv
            .delete(G0_STORE, G0_GROUP, key.to_path().as_bytes(), None)
            .await
            .map(|_| ())
    }

    /// List all disk-group records (prefix scan `/hw/dg/`).
    pub async fn list_disk_groups(&self) -> Result<Vec<DiskGroupEntry>> {
        let entries =
            scan_prefix::<DiskGroupValue>(&self.kv, &<DiskGroupKey as TextKey>::prefix_all()).await?;
        let mut out = Vec::with_capacity(entries.len());
        for (path, value) in entries {
            let k = DiskGroupKey::from_path(&path).map_err(|e| Error::SysdataKeyParse(e.to_string()))?;
            out.push(DiskGroupEntry {
                rack_id: k.rack_id,
                node_id: k.node_id,
                dg_id: k.disk_group_id,
                value,
            });
        }
        Ok(out)
    }

    /// List disk-groups on one node (prefix scan
    /// `/hw/dg/<rack_id>/<node_id>/`).
    pub async fn list_disk_groups_on_node(
        &self,
        rack_id: RackId,
        node_id: NodeId,
    ) -> Result<Vec<DiskGroupEntry>> {
        let entries =
            scan_prefix::<DiskGroupValue>(&self.kv, &DiskGroupKey::text_prefix_for_node(rack_id, node_id))
                .await?;
        let mut out = Vec::with_capacity(entries.len());
        for (path, value) in entries {
            let k = DiskGroupKey::from_path(&path).map_err(|e| Error::SysdataKeyParse(e.to_string()))?;
            out.push(DiskGroupEntry {
                rack_id: k.rack_id,
                node_id: k.node_id,
                dg_id: k.disk_group_id,
                value,
            });
        }
        Ok(out)
    }

    /// Set a disk-group's status (read-modify-write).
    pub async fn set_disk_group_status(
        &self,
        rack_id: RackId,
        node_id: NodeId,
        dg_id: DiskGroupId,
        status: HwStatus,
    ) -> Result<()> {
        let key = DiskGroupKey {
            rack_id,
            node_id,
            disk_group_id: dg_id,
        };
        let mut value = get_json::<DiskGroupValue>(&self.kv, &key.to_path())
            .await?
            .unwrap_or_default();
        value.status = status as i32;
        put_json(&self.kv, &key.to_path(), &value).await
    }
}

// ── disk ────────────────────────────────────────────────────────

impl HardwareClient {
    /// Add or replace a disk record.
    pub async fn add_disk(
        &self,
        rack_id: RackId,
        node_id: NodeId,
        dg_id: DiskGroupId,
        disk_id: &crow_protocol::common::DiskId,
        value: &DiskValue,
    ) -> Result<()> {
        let key = DiskKey {
            rack_id,
            node_id,
            disk_group_id: dg_id,
            disk_id: *disk_id,
        };
        put_json(&self.kv, &key.to_path(), value).await
    }

    /// Read a disk record.
    pub async fn get_disk(
        &self,
        rack_id: RackId,
        node_id: NodeId,
        dg_id: DiskGroupId,
        disk_id: &crow_protocol::common::DiskId,
    ) -> Result<Option<DiskValue>> {
        let key = DiskKey {
            rack_id,
            node_id,
            disk_group_id: dg_id,
            disk_id: *disk_id,
        };
        get_json(&self.kv, &key.to_path()).await
    }

    /// Remove a disk record.
    pub async fn remove_disk(
        &self,
        rack_id: RackId,
        node_id: NodeId,
        dg_id: DiskGroupId,
        disk_id: &crow_protocol::common::DiskId,
    ) -> Result<()> {
        let key = DiskKey {
            rack_id,
            node_id,
            disk_group_id: dg_id,
            disk_id: *disk_id,
        };
        self.kv
            .delete(G0_STORE, G0_GROUP, key.to_path().as_bytes(), None)
            .await
            .map(|_| ())
    }

    /// List all disks in a disk-group (prefix scan
    /// `/hw/disk/<rack_id>/<node_id>/<dg_id>/`).
    pub async fn list_disks_in_group(
        &self,
        rack_id: RackId,
        node_id: NodeId,
        dg_id: DiskGroupId,
    ) -> Result<Vec<(crow_protocol::common::DiskId, DiskValue)>> {
        let entries = scan_prefix::<DiskValue>(
            &self.kv,
            &DiskKey::text_prefix_for_disk_group(rack_id, node_id, dg_id),
        )
        .await?;
        let mut out = Vec::with_capacity(entries.len());
        for (path, value) in entries {
            let k = DiskKey::from_path(&path).map_err(|e| Error::SysdataKeyParse(e.to_string()))?;
            out.push((k.disk_id, value));
        }
        Ok(out)
    }

    /// Set a disk's status (read-modify-write).
    pub async fn set_disk_status(
        &self,
        rack_id: RackId,
        node_id: NodeId,
        dg_id: DiskGroupId,
        disk_id: &crow_protocol::common::DiskId,
        status: HwStatus,
    ) -> Result<()> {
        let key = DiskKey {
            rack_id,
            node_id,
            disk_group_id: dg_id,
            disk_id: *disk_id,
        };
        let mut value = get_json::<DiskValue>(&self.kv, &key.to_path())
            .await?
            .unwrap_or_default();
        value.status = status as i32;
        put_json(&self.kv, &key.to_path(), &value).await
    }
}

// ── ownership map ───────────────────────────────────────────────

impl HardwareClient {
    /// Set the ownership-map entry for a disk-group (which diskdb
    /// instance owns it + lease expiry).
    pub async fn set_owner(
        &self,
        rack_id: RackId,
        node_id: NodeId,
        dg_id: DiskGroupId,
        instance_id: u64,
        lease_expiry_ms: u64,
    ) -> Result<()> {
        let key = OwnerMapKey {
            rack_id,
            node_id,
            disk_group_id: dg_id,
        };
        let value = crow_protocol::common::OwnerMapValue {
            instance_id,
            lease_expiry_ms,
        };
        put_json(&self.kv, &key.to_path(), &value).await
    }

    /// Read the ownership-map entry for a disk-group.
    pub async fn get_owner(
        &self,
        rack_id: RackId,
        node_id: NodeId,
        dg_id: DiskGroupId,
    ) -> Result<Option<DiskdbOwnerEntry>> {
        let key = OwnerMapKey {
            rack_id,
            node_id,
            disk_group_id: dg_id,
        };
        let value = get_json::<crow_protocol::common::OwnerMapValue>(&self.kv, &key.to_path()).await?;
        Ok(value.map(|v| DiskdbOwnerEntry {
            rack_id,
            node_id,
            dg_id,
            instance_id: v.instance_id,
            lease_expiry_ms: v.lease_expiry_ms,
        }))
    }

    /// List all ownership-map entries (prefix scan `/hw/dg_owner/`).
    pub async fn list_owners(&self) -> Result<Vec<DiskdbOwnerEntry>> {
        let entries = scan_prefix::<crow_protocol::common::OwnerMapValue>(
            &self.kv,
            &<OwnerMapKey as TextKey>::prefix_all(),
        )
        .await?;
        let mut out = Vec::with_capacity(entries.len());
        for (path, value) in entries {
            let k = OwnerMapKey::from_path(&path).map_err(|e| Error::SysdataKeyParse(e.to_string()))?;
            out.push(DiskdbOwnerEntry {
                rack_id: k.rack_id,
                node_id: k.node_id,
                dg_id: k.disk_group_id,
                instance_id: value.instance_id,
                lease_expiry_ms: value.lease_expiry_ms,
            });
        }
        Ok(out)
    }

    /// Remove the ownership-map entry for a disk-group.
    pub async fn remove_owner(&self, rack_id: RackId, node_id: NodeId, dg_id: DiskGroupId) -> Result<()> {
        let key = OwnerMapKey {
            rack_id,
            node_id,
            disk_group_id: dg_id,
        };
        self.kv
            .delete(G0_STORE, G0_GROUP, key.to_path().as_bytes(), None)
            .await
            .map(|_| ())
    }
}

// ── bind map ────────────────────────────────────────────────────

impl HardwareClient {
    /// Set the bind-map entry for a disk-group (which paxos data
    /// group its zone records live on).
    pub async fn set_bind(
        &self,
        rack_id: RackId,
        node_id: NodeId,
        dg_id: DiskGroupId,
        store_id: u64,
        group_id: u64,
    ) -> Result<()> {
        let key = BindMapKey {
            rack_id,
            node_id,
            disk_group_id: dg_id,
        };
        let value = crow_protocol::common::BindMapValue { store_id, group_id };
        put_json(&self.kv, &key.to_path(), &value).await
    }

    /// Read the bind-map entry for a disk-group.
    pub async fn get_bind(
        &self,
        rack_id: RackId,
        node_id: NodeId,
        dg_id: DiskGroupId,
    ) -> Result<Option<KVGroupBindEntry>> {
        let key = BindMapKey {
            rack_id,
            node_id,
            disk_group_id: dg_id,
        };
        let value = get_json::<crow_protocol::common::BindMapValue>(&self.kv, &key.to_path()).await?;
        Ok(value.map(|v| KVGroupBindEntry {
            rack_id,
            node_id,
            dg_id,
            store_id: v.store_id,
            group_id: v.group_id,
        }))
    }

    /// List all bind-map entries (prefix scan `/hw/dg_bind/`).
    pub async fn list_binds(&self) -> Result<Vec<KVGroupBindEntry>> {
        let entries = scan_prefix::<crow_protocol::common::BindMapValue>(
            &self.kv,
            &<BindMapKey as TextKey>::prefix_all(),
        )
        .await?;
        let mut out = Vec::with_capacity(entries.len());
        for (path, value) in entries {
            let k = BindMapKey::from_path(&path).map_err(|e| Error::SysdataKeyParse(e.to_string()))?;
            out.push(KVGroupBindEntry {
                rack_id: k.rack_id,
                node_id: k.node_id,
                dg_id: k.disk_group_id,
                store_id: value.store_id,
                group_id: value.group_id,
            });
        }
        Ok(out)
    }

    /// Remove the bind-map entry for a disk-group.
    pub async fn remove_bind(&self, rack_id: RackId, node_id: NodeId, dg_id: DiskGroupId) -> Result<()> {
        let key = BindMapKey {
            rack_id,
            node_id,
            disk_group_id: dg_id,
        };
        self.kv
            .delete(G0_STORE, G0_GROUP, key.to_path().as_bytes(), None)
            .await
            .map(|_| ())
    }
}

// ── disk-group usage ─────────────────────────────────────────────

impl HardwareClient {
    /// Remove the disk-group usage summary record.
    pub async fn remove_disk_group_usage(&self, dg_id: DiskGroupId) -> Result<()> {
        let key = DiskGroupUsageKey { disk_group_id: dg_id };
        self.kv
            .delete(G0_STORE, G0_GROUP, key.to_path().as_bytes(), None)
            .await
            .map(|_| ())
    }
}

// ── cascading remove ─────────────────────────────────────────────

impl HardwareClient {
    /// Remove a disk record + any derived records below it.
    /// Disks have no derived records in group 0 (zone/busy/free records
    /// live on the disk-group's bind, not group 0), so this is equivalent
    /// to `remove_disk`.
    pub async fn remove_disk_cascade(
        &self,
        rack_id: RackId,
        node_id: NodeId,
        dg_id: DiskGroupId,
        disk_id: &crow_protocol::common::DiskId,
    ) -> Result<()> {
        self.remove_disk(rack_id, node_id, dg_id, disk_id).await
    }

    /// Remove a disk-group record + all derived records: child disks,
    /// owner map entry, bind map entry, usage summary.
    pub async fn remove_disk_group_cascade(
        &self,
        rack_id: RackId,
        node_id: NodeId,
        dg_id: DiskGroupId,
    ) -> Result<()> {
        // Remove child disks.
        let disks = self.list_disks_in_group(rack_id, node_id, dg_id).await?;
        for (disk_id, _) in &disks {
            if let Err(e) = self.remove_disk(rack_id, node_id, dg_id, disk_id).await {
                warn!(error = %e, dg_id, "cascade: remove_disk failed; continuing");
            }
        }
        // Remove owner map entry.
        if let Err(e) = self.remove_owner(rack_id, node_id, dg_id).await {
            warn!(error = %e, dg_id, "cascade: remove_owner failed; continuing");
        }
        // Remove bind map entry.
        if let Err(e) = self.remove_bind(rack_id, node_id, dg_id).await {
            warn!(error = %e, dg_id, "cascade: remove_bind failed; continuing");
        }
        // Remove usage summary.
        if let Err(e) = self.remove_disk_group_usage(dg_id).await {
            warn!(error = %e, dg_id, "cascade: remove_disk_group_usage failed; continuing");
        }
        // Remove the disk-group record itself.
        self.remove_disk_group(rack_id, node_id, dg_id).await
    }

    /// Remove a node record + all derived records: child disk-groups
    /// (with their disks, owner, bind, usage), then the node itself.
    pub async fn remove_node_cascade(&self, rack_id: RackId, node_id: NodeId) -> Result<()> {
        let dgs = self.list_disk_groups_on_node(rack_id, node_id).await?;
        for dg in &dgs {
            if let Err(e) = self.remove_disk_group_cascade(rack_id, node_id, dg.dg_id).await {
                warn!(error = %e, node_id, dg_id = dg.dg_id, "cascade: remove_disk_group_cascade failed; continuing");
            }
        }
        self.remove_node(rack_id, node_id).await
    }

    /// Remove a rack record + all derived records: child nodes (with
    /// their disk-groups, disks, owner, bind, usage), then the rack itself.
    pub async fn remove_rack_cascade(&self, rack_id: RackId) -> Result<()> {
        let nodes = self.list_nodes_in_rack(rack_id).await?;
        for (node_id, _) in &nodes {
            if let Err(e) = self.remove_node_cascade(rack_id, *node_id).await {
                warn!(error = %e, rack_id, node_id, "cascade: remove_node_cascade failed; continuing");
            }
        }
        self.remove_rack(rack_id).await
    }
}

// Suppress unused warning for `now_ms` (used by status setters).
#[allow(dead_code)]
fn _now_ms_used() -> u64 {
    now_ms()
}
