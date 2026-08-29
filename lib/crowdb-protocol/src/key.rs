// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Key encoding for crowdb-kv.
//!
//! Defines the [`BinaryKey`] and [`TextKey`] traits and all key kinds
//! stored in crowdb-kv. A key kind is a flat struct with hierarchy
//! fields; two encoding traits map the same struct to bytes:
//!
//! - [`BinaryKey`] — `magic_byte | type_tag:u16 BE | fields BE`,
//!   prost-encoded protobuf values. Used by diskdb data groups
//!   (high-volume, machine-only).
//! - [`TextKey`] — `/magic/type/<field1>/<field2>/...` slash-delimited
//!   path, JSON-encoded values. Used by group 0 (small, human-
//!   inspected, scan-friendly).
//!
//! See `doc/design/protocol/design-crowdb-protocol-key.md` for the full design.
//!
//! Binary key layouts are frozen once shipped. New key kinds are added
//! with a new type tag; existing layouts are never changed.

pub mod chunkdb;
pub mod common;
pub mod diskdb;
pub mod encoding;
pub mod kv_cluster;

#[cfg(test)]
mod key_tests;

pub use chunkdb::{ChunkdbRangeBindingKey, ChunkdbRangeMigrationKey};
pub use common::{NodeKey, RackKey};
pub use diskdb::{
    BindMapKey, BusyBlockKey, DiskGroupKey, DiskGroupUsageKey, DiskKey, FreeBlockKey, InstanceKey,
    OwnerMapKey, RecoveryScanProgressKey, ZoneKey, DISKDB_WATCH_PREFIXES,
};
pub use encoding::{BinaryKey, KeyError, TextKey, CROWDB_KEY_MAGIC};
pub use kv_cluster::{KvGroupKey, KvReplicaKey, KvStoreKey};
