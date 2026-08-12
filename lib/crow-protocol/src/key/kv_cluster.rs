// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! KV-cluster topology key types (group-0 sysdata).
//!
//! These keys identify the persistent records of the KV cluster's
//! structure (stores, groups, replicas) stored in group 0 under
//! `/kv/...` text-path keys. They implement [`TextKey`] only (no
//! [`BinaryKey`] — these are group-0 only).
//!
//! See `doc/design/kv/design-crow-kv-group0.md` §3.1 for the key
//! layout.

use super::encoding::{
    check_path_exact, decode_path_u64, encode_path_header, encode_path_u64, KeyError, TextKey,
};
use crate::common_type::{GroupId, ReplicaId, StoreId};

// ── KvStoreKey ──────────────────────────────────────────────────

/// Key for a KV-cluster store record.
/// Text path: `/kv/store/<store_id>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct KvStoreKey {
    pub store_id: StoreId,
}

impl TextKey for KvStoreKey {
    const PATH_MAGIC: &'static str = "/kv";
    const PATH_TYPE: &'static str = "store";

    fn encode_to_path(&self, out: &mut String) {
        encode_path_header(out, Self::PATH_MAGIC, Self::PATH_TYPE);
        encode_path_u64(out, self.store_id);
    }

    fn decode_path(parts: &[&str]) -> Result<Self, KeyError> {
        if parts.is_empty() {
            return Err(KeyError::ShortInput);
        }
        let store_id = decode_path_u64(parts[0])?;
        check_path_exact(parts, 1)?;
        Ok(Self { store_id })
    }
}

impl KvStoreKey {
    /// Text prefix for scanning all stores: `/kv/store/`.
    #[must_use]
    pub fn text_prefix_all() -> String {
        Self::prefix_all()
    }
}

// ── KvGroupKey ──────────────────────────────────────────────────

/// Key for a KV-cluster group record.
/// Text path: `/kv/group/<store_id>/<group_id>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct KvGroupKey {
    pub store_id: StoreId,
    pub group_id: GroupId,
}

impl TextKey for KvGroupKey {
    const PATH_MAGIC: &'static str = "/kv";
    const PATH_TYPE: &'static str = "group";

    fn encode_to_path(&self, out: &mut String) {
        encode_path_header(out, Self::PATH_MAGIC, Self::PATH_TYPE);
        encode_path_u64(out, self.store_id);
        encode_path_u64(out, self.group_id);
    }

    fn decode_path(parts: &[&str]) -> Result<Self, KeyError> {
        if parts.len() < 2 {
            return Err(KeyError::ShortInput);
        }
        let store_id = decode_path_u64(parts[0])?;
        let group_id = decode_path_u64(parts[1])?;
        check_path_exact(parts, 2)?;
        Ok(Self { store_id, group_id })
    }
}

impl KvGroupKey {
    /// Text prefix for scanning all groups in a store:
    /// `/kv/group/<store_id>/`.
    #[must_use]
    pub fn text_prefix_for_store(store_id: StoreId) -> String {
        format!("/kv/group/{store_id}/")
    }
}

// ── KvReplicaKey ────────────────────────────────────────────────

/// Key for a KV-cluster replica record.
/// Text path: `/kv/replica/<store_id>/<group_id>/<replica_id>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct KvReplicaKey {
    pub store_id: StoreId,
    pub group_id: GroupId,
    pub replica_id: ReplicaId,
}

impl TextKey for KvReplicaKey {
    const PATH_MAGIC: &'static str = "/kv";
    const PATH_TYPE: &'static str = "replica";

    fn encode_to_path(&self, out: &mut String) {
        encode_path_header(out, Self::PATH_MAGIC, Self::PATH_TYPE);
        encode_path_u64(out, self.store_id);
        encode_path_u64(out, self.group_id);
        encode_path_u64(out, self.replica_id);
    }

    fn decode_path(parts: &[&str]) -> Result<Self, KeyError> {
        if parts.len() < 3 {
            return Err(KeyError::ShortInput);
        }
        let store_id = decode_path_u64(parts[0])?;
        let group_id = decode_path_u64(parts[1])?;
        let replica_id = decode_path_u64(parts[2])?;
        check_path_exact(parts, 3)?;
        Ok(Self {
            store_id,
            group_id,
            replica_id,
        })
    }
}

impl KvReplicaKey {
    /// Text prefix for scanning all replicas in a store:
    /// `/kv/replica/<store_id>/`.
    #[must_use]
    pub fn text_prefix_for_store(store_id: StoreId) -> String {
        format!("/kv/replica/{store_id}/")
    }

    /// Text prefix for scanning all replicas in a group:
    /// `/kv/replica/<store_id>/<group_id>/`.
    #[must_use]
    pub fn text_prefix_for_group(store_id: StoreId, group_id: GroupId) -> String {
        format!("/kv/replica/{store_id}/{group_id}/")
    }
}
