// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! chunkdb key types (group-0 sysdata).
//!
//! These keys identify the chunkdb instance range binding table and
//! range migration state stored in group 0 under `/chunkdb/...` text-
//! path keys. They implement [`TextKey`] only (no [`BinaryKey`] —
//! these are group-0 only).
//!
//! See `doc/design/kv/design-crow-kv-group0.md` §3.1 for the key
//! layout and `doc/working/design-r99-dynamic-range-binding.md` §1.

use super::encoding::{
    check_path_exact, decode_path_u64, encode_path_header, encode_path_u64, KeyError, TextKey,
};

// ── ChunkdbRangeBindingKey ──────────────────────────────────────

/// Key for a chunkdb instance range binding entry.
/// Text path: `/chunkdb/range_bind/<sub_range_index>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ChunkdbRangeBindingKey {
    pub sub_range_index: u32,
}

impl TextKey for ChunkdbRangeBindingKey {
    const PATH_MAGIC: &'static str = "/chunkdb";
    const PATH_TYPE: &'static str = "range_bind";

    fn encode_to_path(&self, out: &mut String) {
        encode_path_header(out, Self::PATH_MAGIC, Self::PATH_TYPE);
        encode_path_u64(out, u64::from(self.sub_range_index));
    }

    fn decode_path(parts: &[&str]) -> Result<Self, KeyError> {
        if parts.is_empty() {
            return Err(KeyError::ShortInput);
        }
        let sub_range_index = u32::try_from(decode_path_u64(parts[0])?).map_err(|_| KeyError::ShortInput)?;
        check_path_exact(parts, 1)?;
        Ok(Self { sub_range_index })
    }
}

impl ChunkdbRangeBindingKey {
    /// Text prefix for scanning all range bindings:
    /// `/chunkdb/range_bind/`.
    #[must_use]
    pub fn text_prefix_all() -> String {
        Self::prefix_all()
    }
}

// ── ChunkdbRangeMigrationKey ────────────────────────────────────

/// Key for a chunkdb range migration state entry.
/// Text path: `/chunkdb/range_mig/<sub_range_index>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ChunkdbRangeMigrationKey {
    pub sub_range_index: u32,
}

impl TextKey for ChunkdbRangeMigrationKey {
    const PATH_MAGIC: &'static str = "/chunkdb";
    const PATH_TYPE: &'static str = "range_mig";

    fn encode_to_path(&self, out: &mut String) {
        encode_path_header(out, Self::PATH_MAGIC, Self::PATH_TYPE);
        encode_path_u64(out, u64::from(self.sub_range_index));
    }

    fn decode_path(parts: &[&str]) -> Result<Self, KeyError> {
        if parts.is_empty() {
            return Err(KeyError::ShortInput);
        }
        let sub_range_index = u32::try_from(decode_path_u64(parts[0])?).map_err(|_| KeyError::ShortInput)?;
        check_path_exact(parts, 1)?;
        Ok(Self { sub_range_index })
    }
}

impl ChunkdbRangeMigrationKey {
    /// Text prefix for scanning all range migration states:
    /// `/chunkdb/range_mig/`.
    #[must_use]
    pub fn text_prefix_all() -> String {
        Self::prefix_all()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn range_binding_key_round_trip() {
        let key = ChunkdbRangeBindingKey { sub_range_index: 512 };
        let path = key.to_path();
        assert_eq!(path, "/chunkdb/range_bind/512");
        let decoded = ChunkdbRangeBindingKey::from_path(&path).unwrap();
        assert_eq!(decoded, key);
    }

    #[test]
    fn range_binding_key_prefix_all() {
        assert_eq!(ChunkdbRangeBindingKey::text_prefix_all(), "/chunkdb/range_bind/");
    }

    #[test]
    fn range_migration_key_round_trip() {
        let key = ChunkdbRangeMigrationKey { sub_range_index: 256 };
        let path = key.to_path();
        assert_eq!(path, "/chunkdb/range_mig/256");
        let decoded = ChunkdbRangeMigrationKey::from_path(&path).unwrap();
        assert_eq!(decoded, key);
    }

    #[test]
    fn range_migration_key_prefix_all() {
        assert_eq!(ChunkdbRangeMigrationKey::text_prefix_all(), "/chunkdb/range_mig/");
    }

    #[test]
    fn range_binding_key_rejects_overflow() {
        // u32::MAX + 1 = 4294967296 — overflows u32.
        let path = "/chunkdb/range_bind/4294967296";
        assert!(ChunkdbRangeBindingKey::from_path(path).is_err());
    }
}
