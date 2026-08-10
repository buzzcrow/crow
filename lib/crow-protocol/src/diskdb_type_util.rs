// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Utility functions and extension traits for diskdb proto types.
//!
//! These are domain methods on types defined in the proto files
//! (`DiskId`, `HwStatus`, `ZoneAllocationState`, `ZoneValue`). They
//! live here (not in the proto) because they encode business logic,
//! not wire format.

use crate::common::{DiskId, HwStatus};
use crate::diskdb::rpc::{ZoneAllocationState, ZoneValue};

// ── DiskId helpers ──────────────────────────────────────────────

/// Convenience constructor for `DiskId`.
#[must_use]
pub fn disk_id(high: u64, low: u64) -> DiskId {
    DiskId { high, low }
}

/// Extension trait for `DiskId` display helpers.
pub trait DiskIdExt {
    /// Display format: `{high:016x}-{low:016x}`.
    fn to_display_string(&self) -> String;
}

impl DiskIdExt for DiskId {
    fn to_display_string(&self) -> String {
        format!("{:016x}-{:016x}", self.high, self.low)
    }
}

// ── HwStatus helpers ────────────────────────────────────────────

/// Extension trait for `HwStatus` domain logic.
pub trait HwStatusExt {
    /// Whether allocations are allowed at this status.
    fn allows_allocate(&self) -> bool;

    /// Whether frees are allowed at this status.
    fn allows_free(&self) -> bool;
}

impl HwStatusExt for HwStatus {
    fn allows_allocate(&self) -> bool {
        matches!(self, HwStatus::Up)
    }

    fn allows_free(&self) -> bool {
        matches!(self, HwStatus::Up | HwStatus::Maintenance | HwStatus::Suspect)
    }
}

/// Compute the effective status for a disk: the most restrictive
/// (highest-priority) of node, group, and disk status.
#[must_use]
pub fn effective_status(node: HwStatus, group: HwStatus, disk: HwStatus) -> HwStatus {
    node.max(group).max(disk)
}

// ── ZoneAllocationState helpers ─────────────────────────────────

/// Extension trait for `ZoneAllocationState` atomic interop.
pub trait ZoneAllocationStateExt {
    /// Decode a `u8` (e.g. from an `AtomicU8` load) into a state.
    /// Unknown values map to `Full` (defensive — corruption/bug).
    #[must_use]
    fn from_u8(v: u8) -> Self;
}

impl ZoneAllocationStateExt for ZoneAllocationState {
    fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::ZoneAllocActive,
            1 => Self::ZoneAllocAvailable,
            // 2 and unknown both map to Full; explicit arm documents the known value.
            #[allow(clippy::match_same_arms)]
            2 => Self::ZoneAllocFull,
            _ => Self::ZoneAllocFull,
        }
    }
}

// ── ZoneValue CRC integrity ─────────────────────────────────────

/// Extension trait for `ZoneValue` CRC32 integrity.
pub trait ZoneValueExt {
    /// Compute and set the checksum (CRC32 over bincode-serialized
    /// value, appended as 4 little-endian bytes to `usage_bitmap`).
    ///
    /// # Panics
    /// Panics if bincode serialization fails (cannot fail for this type).
    fn compute_checksum(&mut self);

    /// Verify the checksum matches.
    ///
    /// # Panics
    /// Panics if bincode serialization fails (cannot fail for this type).
    #[must_use]
    fn verify_checksum(&self) -> bool;

    /// Serialize to bytes (bincode).
    ///
    /// # Panics
    /// Panics if bincode serialization fails (cannot fail for this type).
    #[must_use]
    fn to_bytes(&self) -> Vec<u8>;

    /// Deserialize from bytes (bincode).
    ///
    /// # Errors
    /// Returns `Err` if the bytes cannot be deserialized into a `ZoneValue`.
    fn from_bytes(bytes: &[u8]) -> Result<ZoneValue, String>;
}

impl ZoneValueExt for ZoneValue {
    fn compute_checksum(&mut self) {
        let bytes = bincode::serialize(self).expect("serialize ZoneValue for checksum");
        let crc = crc32fast::hash(&bytes);
        self.usage_bitmap.extend_from_slice(&crc.to_le_bytes());
    }

    fn verify_checksum(&self) -> bool {
        if self.usage_bitmap.len() < 4 {
            return false;
        }
        let payload_len = self.usage_bitmap.len() - 4;
        let stored_crc = u32::from_le_bytes(self.usage_bitmap[payload_len..].try_into().expect("4 bytes"));
        let mut copy = self.clone();
        copy.usage_bitmap.truncate(payload_len);
        let bytes = bincode::serialize(&copy).expect("serialize ZoneValue for verify");
        crc32fast::hash(&bytes) == stored_crc
    }

    fn to_bytes(&self) -> Vec<u8> {
        bincode::serialize(self).expect("serialize ZoneValue")
    }

    fn from_bytes(bytes: &[u8]) -> Result<ZoneValue, String> {
        bincode::deserialize(bytes).map_err(|e| e.to_string())
    }
}
