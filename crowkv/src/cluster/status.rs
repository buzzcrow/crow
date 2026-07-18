// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Hierarchical point-in-time status of the cluster, used by the management APIs.
//!
//! Each layer (`PxKvStore` → `PxGroup` → `PxLocalReplica` / `PxRemoteReplica`)
//! exposes `status()` returning these structs. Status-specific fields
//! (`status`, `messages`) are defaulted; `#[serde(skip_serializing_if)]`
//! suppresses empty lists in topology output.

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::common::metrics::MetricsSnapshot;

/// Severity of a layer's runtime status. Serializes as a lowercase
/// string (`"ok"`, `"degraded"`, `"unhealthy"`) so the JSON wire shape
/// is identical to the previous `String`-typed fields.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum StatusLevel {
    #[default]
    Ok,
    Degraded,
    Unhealthy,
}

impl StatusLevel {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Degraded => "degraded",
            Self::Unhealthy => "unhealthy",
        }
    }

    #[must_use]
    pub fn worst(a: Self, b: Self) -> Self {
        use StatusLevel::{Degraded, Ok, Unhealthy};
        match (a, b) {
            (Unhealthy, _) | (_, Unhealthy) => Unhealthy,
            (Degraded, _) | (_, Degraded) => Degraded,
            (Ok, Ok) => Ok,
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, ToSchema)]
pub struct StoreStatus {
    pub store_id: u64,
    pub listen_addr: Option<String>,
    pub status: StatusLevel,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub messages: Vec<String>,
    pub groups: Vec<GroupStatus>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, ToSchema)]
pub struct GroupStatus {
    pub group_id: u64,
    pub leader_id: u64,
    pub local_replica_id: u64,
    pub force_classic: bool,
    pub status: StatusLevel,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub messages: Vec<String>,
    pub local_replica: ReplicaStatus,
    pub remotes: Vec<RemoteStatus>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, ToSchema)]
pub struct ReplicaStatus {
    pub id: u64,
    pub role: String,
    pub voting: bool,
    pub status: StatusLevel,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub messages: Vec<String>,
    pub kv_store: KvStoreStatus,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, ToSchema)]
pub struct KvStoreStatus {
    /// O(1) read of the in-memory map length. Cheap; safe to call from
    /// `/topology` per-request.
    pub key_count: u64,
    /// [`crate::kv::KVEngine::is_healthy`] as of this call. `true` for
    /// `InMemKV` always; for a `CrowtreeEngine`, `false` once a durable I/O
    /// fault has latched (`Crowtree::io_failed`).
    #[serde(default = "default_true")]
    pub engine_healthy: bool,
    /// [`crate::kv::CrowtreeEngine::stats`] as of this call, or `None` for
    /// `InMemKV` (no comparable internals). Populated by downcasting
    /// `PxLearner::engine()` via [`crate::kv::KVEngine::as_any`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub crowtree_stats: Option<CrowtreeStatsView>,
}

fn default_true() -> bool {
    true
}

/// Wire-serializable mirror of [`crate::kv::CrowtreeStats`] (that type lives
/// in `crowtree_ffi` and isn't `Serialize`), for `/topology`/`/api/health`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct CrowtreeStatsView {
    pub last_applied_slot: u64,
    pub contiguous_slot: u64,
    pub gc_watermark: u64,
    pub snapshot_pages_written: u64,
    pub snapshot_segments_written: u64,
    pub buffer_pool_hits: u64,
    pub buffer_pool_misses: u64,
    pub buffer_pool_evictions: u64,
    pub buffer_pool_writebacks: u64,
    pub buffer_pool_resident: u32,
    pub buffer_pool_dirty: u32,
    pub buffer_pool_used: u32,
    pub buffer_pool_num_frames: u32,
    pub mt_upsert_total: u64,
    pub mt_get_total: u64,
    pub mt_get_hit_total: u64,
    pub flush_drain_total: u64,
    pub flush_entries_total: u64,
    pub l1_get_total: u64,
    pub l1_get_hit_total: u64,
}

impl From<crate::kv::CrowtreeStats> for CrowtreeStatsView {
    fn from(s: crate::kv::CrowtreeStats) -> Self {
        Self {
            last_applied_slot: s.last_applied_slot,
            contiguous_slot: s.contiguous_slot,
            gc_watermark: s.gc_watermark,
            snapshot_pages_written: s.snapshot_pages_written,
            snapshot_segments_written: s.snapshot_segments_written,
            buffer_pool_hits: s.buffer_pool_hits,
            buffer_pool_misses: s.buffer_pool_misses,
            buffer_pool_evictions: s.buffer_pool_evictions,
            buffer_pool_writebacks: s.buffer_pool_writebacks,
            buffer_pool_resident: s.buffer_pool_resident,
            buffer_pool_dirty: s.buffer_pool_dirty,
            buffer_pool_used: s.buffer_pool_used,
            buffer_pool_num_frames: s.buffer_pool_num_frames,
            mt_upsert_total: s.mt_upsert_total,
            mt_get_total: s.mt_get_total,
            mt_get_hit_total: s.mt_get_hit_total,
            flush_drain_total: s.flush_drain_total,
            flush_entries_total: s.flush_entries_total,
            l1_get_total: s.l1_get_total,
            l1_get_hit_total: s.l1_get_hit_total,
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, ToSchema)]
pub struct RemoteStatus {
    pub id: u64,
    pub endpoint: String,
    pub voting: bool,
    pub status: StatusLevel,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub messages: Vec<String>,
    pub metrics: MetricsSnapshot,
}
