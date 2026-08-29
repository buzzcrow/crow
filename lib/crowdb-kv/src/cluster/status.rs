// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Hierarchical point-in-time status of the cluster, used by the management APIs.
//!
//! The wire types (`StoreStatus`, `GroupStatus`, `ReplicaStatus`, etc.)
//! live in `crowdb_protocol::mgmt` — the single home for cross-component
//! protocol types (`design-crowdb-kv-group0.md` §2.4). This module re-
//! exports them and hosts the two conversions that must stay local to
//! `crowdb-kv`:
//!
//! - `From<ElectionMetricsSnapshot> for ElectionStateView` —
//!   `ElectionMetricsSnapshot` is local to `crowdb-kv`, so the orphan
//!   rule permits the impl here.
//! - `crowdb_tree_stats_to_view` — a free function converting
//!   `CrowdbTreeStats` (from `crowdb-tree-ffi`) to `CrowdbTreeStatsView`
//!   (from `crowdb-protocol`). Both are foreign, so a `From` impl would
//!   violate the orphan rule; a free function avoids it.

pub use crowdb_protocol::mgmt::{
    CrowdbTreeStatsView, GroupStatus, InflightStatus, KvStoreStatus, RemoteStatus, ReplicaStatus, StatusLevel,
    StoreStatus,
};
pub(crate) use crowdb_protocol::mgmt::{ElectionStateView, ReadStateView};

use crate::common::metrics::ElectionMetricsSnapshot;
use crate::kv::CrowdbTreeStats;

/// Convert the local `ElectionMetricsSnapshot` (mutex-guarded gauges +
/// atomic counters) into the wire-serializable `ElectionStateView`.
impl From<ElectionMetricsSnapshot> for ElectionStateView {
    fn from(s: ElectionMetricsSnapshot) -> Self {
        Self {
            election_count: s.election_count,
            current_term: s.current_term,
            last_heartbeat_age_ms: s.last_heartbeat_age_ms,
            lease_remaining_ms: s.lease_remaining_ms,
            bulk_phase1_in_flight_slots: s.bulk_phase1_in_flight_slots,
            step_downs_higher_term: s.step_downs_higher_term,
            step_downs_lease_unrenewable: s.step_downs_lease_unrenewable,
            step_downs_admin: s.step_downs_admin,
        }
    }
}

/// Convert `CrowdbTreeStats` (from `crowdb-tree-ffi`, not `Serialize`) into
/// the wire-serializable `CrowdbTreeStatsView` (from `crowdb-protocol`).
/// A free function because both types are foreign — a `From` impl would
/// violate the orphan rule.
#[must_use]
pub fn crowdb_tree_stats_to_view(s: CrowdbTreeStats) -> CrowdbTreeStatsView {
    CrowdbTreeStatsView {
        last_applied_slot: s.last_applied_slot,
        contiguous_slot: s.contiguous_slot,
        gc_watermark: s.gc_watermark,
        snapshot_pages_written: s.snapshot_pages_written,
        snapshot_pages_total: s.snapshot_pages_total,
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
        snapshot_total: s.snapshot_total,
        l1_get_total: s.l1_get_total,
        l1_get_hit_total: s.l1_get_hit_total,
    }
}
