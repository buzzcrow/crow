// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::error::check;
use crate::sys;
use crate::tree::Crowdbtree;
use crate::CtError;

/// Result of a cadence-driven [`Crowdbtree::compact_sparse_blocks`] pass
/// (R129). Snapshot folding drops eligible tombstones during every snapshot;
/// this struct reports the block-level relocation and deletion outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MergeGcStats {
    pub blocks_selected: u64,
    pub pages_relocated: u64,
    pub bytes_relocated: u64,
    pub blocks_deleted: u64,
}

/// Point-in-time diagnostics snapshot; see [`Crowdbtree::stats`]. Every field
/// is O(1) on the C++ side (an already-tracked atomic counter or
/// `BufferPool::stats`), so this is safe to poll periodically.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Stats {
    pub last_applied_slot: u64,
    pub contiguous_slot: u64,
    pub gc_watermark: u64,
    pub io_failed: bool,
    pub snapshot_pages_written: u64,
    pub snapshot_pages_total: u64,
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
    pub snapshot_total: u64,
    pub l1_get_total: u64,
    pub l1_get_hit_total: u64,
}

impl Crowdbtree {
    /// Cadence-driven block compaction (R129). Selects sparse source blocks,
    /// relocates their live extents through a snapshot, and deletes blocks
    /// unreachable from any retained anchor. Non-block stores and disabled
    /// configurations return an empty stats result with no snapshot write.
    pub fn compact_sparse_blocks(&self) -> Result<MergeGcStats, CtError> {
        let mut stats = sys::ct_merge_gc_stats::default();
        check(unsafe { sys::ct_compact_sparse_blocks(self.as_ptr(), &mut stats) })?;
        Ok(MergeGcStats {
            blocks_selected: stats.blocks_selected,
            pages_relocated: stats.pages_relocated,
            bytes_relocated: stats.bytes_relocated,
            blocks_deleted: stats.blocks_deleted,
        })
    }

    /// Batched diagnostics snapshot. O(1) -- safe to poll periodically for
    /// metrics/console display.
    pub fn stats(&self) -> Stats {
        let mut raw = sys::ct_stats::default();
        unsafe { sys::ct_get_stats(self.as_ptr(), &mut raw) };
        Stats {
            last_applied_slot: raw.last_applied_slot,
            contiguous_slot: raw.contiguous_slot,
            gc_watermark: raw.gc_watermark,
            io_failed: raw.io_failed != 0,
            snapshot_pages_written: raw.snapshot_pages_written,
            snapshot_pages_total: raw.snapshot_pages_total,
            snapshot_segments_written: raw.snapshot_segments_written,
            buffer_pool_hits: raw.buffer_pool_hits,
            buffer_pool_misses: raw.buffer_pool_misses,
            buffer_pool_evictions: raw.buffer_pool_evictions,
            buffer_pool_writebacks: raw.buffer_pool_writebacks,
            buffer_pool_resident: raw.buffer_pool_resident,
            buffer_pool_dirty: raw.buffer_pool_dirty,
            buffer_pool_used: raw.buffer_pool_used,
            buffer_pool_num_frames: raw.buffer_pool_num_frames,
            mt_upsert_total: raw.mt_upsert_total,
            mt_get_total: raw.mt_get_total,
            mt_get_hit_total: raw.mt_get_hit_total,
            flush_drain_total: raw.flush_drain_total,
            flush_entries_total: raw.flush_entries_total,
            snapshot_total: raw.snapshot_total,
            l1_get_total: raw.l1_get_total,
            l1_get_hit_total: raw.l1_get_hit_total,
        }
    }
}
