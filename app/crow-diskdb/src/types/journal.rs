// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Journal records, zone snapshot record, key-layout helpers, and CRC
//! integrity.
//!
//! The journal is the source of truth for zone state; the in-memory
//! bitmap is derived and rebuilt on restart by replaying the journal
//! (design doc §3.4, §7). Each allocate/free appends a small record to
//! the paxos data group's journal via a blind `Put`; the full
//! `ZoneRecord` is a compacted snapshot written periodically.

use serde::{Deserialize, Serialize};

use super::ids::{DiskGroupId, DiskUuid, NodeId};
use super::zone_state::ZoneState;

// ── Journal records ─────────────────────────────────────────────

/// Record appended on each allocate. ≤ 32 bytes bincode-serialized.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BusyRecord {
    pub zone_offset: u64,
    pub size: u32,
    pub tag: u64,
}

/// Record appended on each free. Same shape as `BusyRecord`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FreeRecord {
    pub zone_offset: u64,
    pub size: u32,
    pub tag: u64,
}

// ── Zone snapshot record ────────────────────────────────────────

/// The compacted snapshot of a zone, written periodically by snapshot
/// compaction (not on every allocate). Stored directly at the snapshot
/// key — no wrapper.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ZoneRecord {
    pub disk_uuid: DiskUuid,
    pub zone_index: u32,
    /// Absolute byte offset of this zone on the disk.
    pub disk_offset: u64,
    /// Capacity of this zone in bytes (last zone may be smaller).
    pub zone_size_bytes: u64,
    /// Current allocation position in block units.
    pub allocate_pos: u32,
    /// Compacted bitmap bytes.
    pub usage_bitmap: Vec<u8>,
    pub zone_state: ZoneState,
    /// Max journal slot included in this compaction.
    pub snapshot_slot: u64,
    /// CRC32 over the record with this field zeroed.
    pub checksum: u32,
}

impl ZoneRecord {
    /// Compute and set the checksum (CRC32 over bincode-serialized
    /// record with the checksum field zeroed).
    ///
    /// # Panics
    /// Panics if bincode serialization fails (cannot fail for this struct).
    pub fn compute_checksum(&mut self) {
        self.checksum = 0;
        let bytes = bincode::serialize(self).expect("serialize ZoneRecord for checksum");
        self.checksum = crc32fast::hash(&bytes);
    }

    /// Verify the checksum matches.
    ///
    /// # Panics
    /// Panics if bincode serialization fails (cannot fail for this struct).
    #[must_use]
    pub fn verify_checksum(&self) -> bool {
        let mut copy = self.clone();
        copy.checksum = 0;
        let bytes = bincode::serialize(&copy).expect("serialize ZoneRecord for verify");
        crc32fast::hash(&bytes) == self.checksum
    }

    /// Serialize to bytes (bincode).
    ///
    /// # Panics
    /// Panics if bincode serialization fails (cannot fail for this struct).
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        bincode::serialize(self).expect("serialize ZoneRecord")
    }

    /// Deserialize from bytes (bincode).
    ///
    /// # Errors
    /// Returns `Err` if the bytes cannot be deserialized into a `ZoneRecord`.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        bincode::deserialize(bytes).map_err(|e| e.to_string())
    }
}

// ── Journal key layout (data group, prefix-scan replay by slot) ─

/// `/diskdb/journal/{node_id}-{dg_id}/{disk_uuid}/z{zone_idx:04}/busy/{slot}`
#[must_use]
pub fn journal_key_busy(
    node_id: NodeId,
    dg_id: DiskGroupId,
    disk_uuid: &DiskUuid,
    zone_idx: u32,
    slot: u64,
) -> String {
    format!(
        "/diskdb/journal/{node_id}-{dg_id}/{}/z{zone_idx:04}/busy/{slot:020}",
        disk_uuid.to_key_component()
    )
}

/// `/diskdb/journal/{node_id}-{dg_id}/{disk_uuid}/z{zone_idx:04}/free/{slot}`
#[must_use]
pub fn journal_key_free(
    node_id: NodeId,
    dg_id: DiskGroupId,
    disk_uuid: &DiskUuid,
    zone_idx: u32,
    slot: u64,
) -> String {
    format!(
        "/diskdb/journal/{node_id}-{dg_id}/{}/z{zone_idx:04}/free/{slot:020}",
        disk_uuid.to_key_component()
    )
}

/// `/diskdb/journal/{node_id}-{dg_id}/{disk_uuid}/z{zone_idx:04}/snapshot`
#[must_use]
pub fn journal_key_snapshot(
    node_id: NodeId,
    dg_id: DiskGroupId,
    disk_uuid: &DiskUuid,
    zone_idx: u32,
) -> String {
    format!(
        "/diskdb/journal/{node_id}-{dg_id}/{}/z{zone_idx:04}/snapshot",
        disk_uuid.to_key_component()
    )
}

/// `/diskdb/journal/{node_id}-{dg_id}/{disk_uuid}/z{zone_idx:04}/`
#[must_use]
pub fn journal_prefix_zone(
    node_id: NodeId,
    dg_id: DiskGroupId,
    disk_uuid: &DiskUuid,
    zone_idx: u32,
) -> String {
    format!(
        "/diskdb/journal/{node_id}-{dg_id}/{}/z{zone_idx:04}/",
        disk_uuid.to_key_component()
    )
}

/// `/diskdb/journal/{node_id}-{dg_id}/{disk_uuid}/`
#[must_use]
pub fn journal_prefix_disk(node_id: NodeId, dg_id: DiskGroupId, disk_uuid: &DiskUuid) -> String {
    format!(
        "/diskdb/journal/{node_id}-{dg_id}/{}/",
        disk_uuid.to_key_component()
    )
}

/// `/diskdb/journal/{node_id}-{dg_id}/`
#[must_use]
pub fn journal_prefix_dg(node_id: NodeId, dg_id: DiskGroupId) -> String {
    format!("/diskdb/journal/{node_id}-{dg_id}/")
}

// ── Group-0 sysdata keys (matching design doc §5) ───────────────

/// `/diskdb/node/{node_id}/meta`
#[must_use]
pub fn sysdata_key_node(node_id: NodeId) -> String {
    format!("/diskdb/node/{node_id}/meta")
}

/// `/diskdb/node/{node_id}/dg/{dg_id}/meta`
#[must_use]
pub fn sysdata_key_disk_group(node_id: NodeId, dg_id: DiskGroupId) -> String {
    format!("/diskdb/node/{node_id}/dg/{dg_id}/meta")
}

/// `/diskdb/node/{node_id}/disk/{disk_uuid}/meta`
#[must_use]
pub fn sysdata_key_disk(node_id: NodeId, disk_uuid: &DiskUuid) -> String {
    format!(
        "/diskdb/node/{node_id}/disk/{}/meta",
        disk_uuid.to_key_component()
    )
}

/// `/diskdb/map/owner/{node_id}-{dg_id}`
#[must_use]
pub fn sysdata_key_owner(node_id: NodeId, dg_id: DiskGroupId) -> String {
    format!("/diskdb/map/owner/{node_id}-{dg_id}")
}

/// `/diskdb/map/bind/{node_id}-{dg_id}`
#[must_use]
pub fn sysdata_key_bind(node_id: NodeId, dg_id: DiskGroupId) -> String {
    format!("/diskdb/map/bind/{node_id}-{dg_id}")
}

/// `/diskdb/instance/{instance_id}`
#[must_use]
pub fn sysdata_key_instance(instance_id: &str) -> String {
    format!("/diskdb/instance/{instance_id}")
}
