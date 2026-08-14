// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Utility functions and extension traits for diskdb proto types.
//!
//! These are domain methods on types defined in the proto files
//! (`DiskId`, `HwStatus`, `ZoneAllocationState`, `ZoneValue`). They
//! live here (not in the proto) because they encode business logic,
//! not wire format.

use crate::common::{DiskId, HwStatus};
use crate::diskdb::rpc::{RecoveryScanProgressValue, ZoneAllocationState, ZoneValue};

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

    /// Parse a display-format string (`{high:016x}-{low:016x}`) into a
    /// `DiskId`. Also accepts a bare 32-char hex string (no dash).
    ///
    /// # Errors
    /// Returns an error string if the input is malformed.
    fn from_display_string(s: &str) -> Result<DiskId, String>;
}

impl DiskIdExt for DiskId {
    fn to_display_string(&self) -> String {
        format!("{:016x}-{:016x}", self.high, self.low)
    }

    fn from_display_string(s: &str) -> Result<DiskId, String> {
        let (high_hex, low_hex) = if let Some((h, l)) = s.split_once('-') {
            (h, l)
        } else if s.len() == 32 && s.is_ascii() {
            // ASCII-only guarantees byte 16 is a char boundary.
            let (h, l) = (&s[..16], &s[16..]);
            (h, l)
        } else {
            return Err(format!(
                "invalid DiskId string: expected 32 hex chars or high-low pair, got {s}"
            ));
        };
        let high = u64::from_str_radix(high_hex, 16).map_err(|e| format!("invalid high: {e}"))?;
        let low = u64::from_str_radix(low_hex, 16).map_err(|e| format!("invalid low: {e}"))?;
        Ok(DiskId { high, low })
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
///
/// The CRC32 is computed over `usage_bitmap` + `compact_ts` and stored
/// in the dedicated `crc32` field (proto field 7). The baseline
/// `ZoneValue` written during disk-add init has an empty bitmap,
/// `compact_ts = 0`, `snapshot_slot = 0`. A corrupted `compact_ts`
/// would break the watermark logic, so it is integrity-protected
/// alongside the bitmap.
pub trait ZoneValueExt {
    /// Compute and set `crc32` from `usage_bitmap` + `compact_ts`.
    fn compute_checksum(&mut self);

    /// Verify `crc32` matches `usage_bitmap` + `compact_ts`.
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
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&self.usage_bitmap);
        hasher.update(&self.compact_ts.to_le_bytes());
        self.crc32 = hasher.finalize();
    }

    fn verify_checksum(&self) -> bool {
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&self.usage_bitmap);
        hasher.update(&self.compact_ts.to_le_bytes());
        self.crc32 == hasher.finalize()
    }

    fn to_bytes(&self) -> Vec<u8> {
        bincode::serialize(self).expect("serialize ZoneValue")
    }

    fn from_bytes(bytes: &[u8]) -> Result<ZoneValue, String> {
        bincode::deserialize(bytes).map_err(|e| e.to_string())
    }
}

// ── RecoveryScanProgressValue serialization ─────────────────────

/// Extension trait for `RecoveryScanProgressValue` bincode
/// serialization (same wire format as other diskdb data-group
/// values).
pub trait RecoveryScanProgressValueExt {
    /// Serialize to bytes (bincode).
    ///
    /// # Panics
    /// Panics if bincode serialization fails (cannot fail for this type).
    #[must_use]
    fn to_bytes(&self) -> Vec<u8>;

    /// Deserialize from bytes (bincode).
    ///
    /// # Errors
    /// Returns `Err` if the bytes cannot be deserialized.
    fn from_bytes(bytes: &[u8]) -> Result<RecoveryScanProgressValue, String>;
}

impl RecoveryScanProgressValueExt for RecoveryScanProgressValue {
    fn to_bytes(&self) -> Vec<u8> {
        bincode::serialize(self).expect("serialize RecoveryScanProgressValue")
    }

    fn from_bytes(bytes: &[u8]) -> Result<RecoveryScanProgressValue, String> {
        bincode::deserialize(bytes).map_err(|e| e.to_string())
    }
}
