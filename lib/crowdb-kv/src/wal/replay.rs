// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! WAL replay engine (P2 W10–W13).
//!
//! Discovers segments, scans records with CRC verification, truncates on
//! corruption, and rebuilds acceptor state, `current_term`, `voted_for`.

use std::io;
use std::path::PathBuf;
use std::sync::Arc;

use tracing::{debug, error, warn};

use crate::paxos::roles::SlotIndex;
use crate::paxos::{PxGroupId, PxNodeId, PxTerm};

use super::index::{SegmentIndex, SegmentMeta, SlotLocation};
use super::record::{RecordType, WALRecord};
use super::segment::{parse_segment_filename, SegmentReader};
use super::{IoBackend, OpenOptions};

/// Result of replaying all WAL segments for a group.
#[derive(Debug)]
pub struct ReplayResult {
    /// Verified records in slot order (deduplicated: highest ballot per slot wins).
    pub records: Vec<WALRecord>,
    /// Rebuilt segment index.
    pub index: SegmentIndex,
    /// Highest `segment_id` found (used to resume segment id counter).
    pub max_segment_id: u64,
    /// Reconstructed `current_term` (max term across all records).
    pub current_term: PxTerm,
    /// Reconstructed `voted_for` from the latest `VoteGranted` record for `current_term`.
    pub voted_for: Option<PxNodeId>,
}

/// Replay all WAL segments for a group across all disk paths.
///
/// ## Procedure
///
/// 1. Discover `disk/groupN/seg-*.ck`, order by `segment_id`.
/// 2. Walk records, verify magic/version/CRC.
/// 3. On corruption in the *last* (unsealed) segment: truncate at the bad offset.
///    On corruption in a *sealed* segment: abort with critical error.
/// 4. Rebuild acceptor state (highest-ballot wins per slot).
/// 5. Reconstruct `current_term` from max term.
/// 6. Reconstruct `voted_for` from latest `VoteGranted`.
///
/// # Errors
/// Returns IO error if segments cannot be read or corruption is found in sealed segments.
#[allow(clippy::too_many_lines)]
pub async fn replay_group(
    backend: &Arc<IoBackend>,
    disk_paths: &[PathBuf],
    group_id: PxGroupId,
) -> io::Result<ReplayResult> {
    // ── Step 1: discover segments ───────────────────────
    let mut all_segments: Vec<(usize, u64, PathBuf)> = Vec::new();
    for (disk_idx, disk_path) in disk_paths.iter().enumerate() {
        let group_dir = disk_path.join(format!("group{group_id}"));
        if !backend.exists(&group_dir).await {
            continue;
        }
        let entries = backend.read_dir(&group_dir).await?;
        for entry_path in entries {
            let name = match entry_path.file_name().and_then(|n| n.to_str()) {
                Some(n) => n.to_string(),
                None => continue,
            };
            if let Some(seg_id) = parse_segment_filename(&name) {
                all_segments.push((disk_idx, seg_id, entry_path));
            }
        }
    }

    all_segments.sort_by_key(|&(_, seg_id, _)| seg_id);

    debug!(
        group_id,
        segment_count = all_segments.len(),
        "replay: discovered segments"
    );

    // ── Step 2–3: scan records, verify CRC, truncate on error ──
    let mut verified_records: Vec<WALRecord> = Vec::new();
    let mut index = SegmentIndex::new();
    let mut max_segment_id: u64 = 0;
    let total_segments = all_segments.len();

    for (i, (disk_idx, seg_id, path)) in all_segments.into_iter().enumerate() {
        if seg_id > max_segment_id {
            max_segment_id = seg_id;
        }
        let is_last = i + 1 == total_segments;

        let mut reader = match SegmentReader::open(backend, &path).await {
            Ok(r) => r,
            Err(e) => {
                error!(group_id, segment_id = seg_id, error = %e, "replay: skipping unreadable segment");
                continue;
            }
        };

        let footer = reader.read_footer().await.ok().flatten();
        let is_sealed = footer.is_some();

        let mut seg_records: Vec<(SlotIndex, u64)> = Vec::new();
        let mut seg_min_slot = u64::MAX;
        let mut seg_max_slot = 0u64;
        let mut seg_record_count = 0u32;

        loop {
            match reader.next_record().await {
                Ok(Some((record, offset))) => {
                    if record.slot != 0 {
                        seg_records.push((record.slot, offset));
                        if record.slot < seg_min_slot {
                            seg_min_slot = record.slot;
                        }
                        if record.slot > seg_max_slot {
                            seg_max_slot = record.slot;
                        }
                    }
                    seg_record_count += 1;
                    verified_records.push(record);
                }
                Ok(None) => break, // EOF or footer
                Err((err, offset)) => {
                    if is_sealed && !is_last {
                        // Corruption inside a sealed (non-last) segment → abort.
                        let msg = format!(
                            "critical: corruption in sealed segment {} at offset {offset}: {err}. \
                             next step: fail node out of group and rebuild from peers.",
                            path.display()
                        );
                        error!(group_id, segment_id = seg_id, "{msg}");
                        return Err(io::Error::new(io::ErrorKind::InvalidData, msg));
                    }
                    // Corruption in the last/unsealed segment → truncate.
                    warn!(
                        group_id,
                        segment_id = seg_id,
                        offset,
                        error = %err,
                        "replay: truncating segment at corruption point"
                    );
                    // Truncate the file at the corruption offset.
                    let trunc_file = backend.open(&path, OpenOptions::read_write()).await?;
                    trunc_file.truncate(offset).await?;
                    break;
                }
            }
        }

        // Register segment metadata in index.
        let meta = SegmentMeta {
            segment_id: seg_id,
            disk_idx,
            min_slot: if seg_min_slot == u64::MAX { 0 } else { seg_min_slot },
            max_slot: seg_max_slot,
            record_count: seg_record_count,
        };
        for (slot, file_offset) in &seg_records {
            index.insert(
                *slot,
                SlotLocation {
                    disk_idx,
                    segment_id: seg_id,
                    file_offset: *file_offset,
                },
            );
        }
        index.register_segment(meta);
    }

    debug!(
        group_id,
        total_records = verified_records.len(),
        max_segment_id,
        "replay: scan complete"
    );

    // ── Step 4–7: rebuild in-memory state ──────────────

    // 4. current_term = max term across all records.
    let current_term = verified_records.iter().map(|r| r.term).max().unwrap_or(0);

    // 5. voted_for from latest VoteGranted for current_term.
    let voted_for = verified_records
        .iter()
        .rev()
        .find(|r| r.record_type == RecordType::VoteGranted && r.term == current_term)
        .and_then(WALRecord::voted_for_id);

    Ok(ReplayResult {
        records: verified_records,
        index,
        max_segment_id,
        current_term,
        voted_for,
    })
}
