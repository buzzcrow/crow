// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! `DdbDiskGroup` — per-disk-group manager: owns the disks, the RCU
//! allocatable-disk context, and the round-robin cursor.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use crow_protocol::common::{DiskId, HwStatus};
use crow_protocol::DiskGroupId;

use crate::model::disk::DdbDisk;
use crate::model::zone::{AllocatedRange, DdbZone};

/// RCU-published set of allocatable disks within the named
/// disk-group, replaced via `Arc` swap on add/remove/status-change.
pub type AllocateDiskContext = Vec<Arc<DdbDisk>>;

/// Result of a successful allocation: `(disk, zone, range)`.
pub type AllocClaim = (Arc<DdbDisk>, Arc<DdbZone>, AllocatedRange);

/// A disk-group manager — one per owned disk-group.
pub struct DdbDiskGroup {
    pub disk_group_id: DiskGroupId,
    pub node_id: u64,
    pub rack_id: u64,
    pub status: RwLock<HwStatus>,
    /// `(store_id, group_id)` for the bound paxos data group.
    pub bind: RwLock<(u64, u64)>,
    pub disks: RwLock<Vec<Arc<DdbDisk>>>,
    /// O(1) disk-id → disk lookup for the free path.
    disk_index: RwLock<HashMap<DiskId, Arc<DdbDisk>>>,
    /// RCU context of allocatable disks within this disk-group.
    allocating_disks: RwLock<Arc<AllocateDiskContext>>,
    /// Round-robin cursor over `allocating_disks`.
    pos_v_disk_ctx: AtomicU64,
}

impl DdbDiskGroup {
    pub fn new(disk_group_id: DiskGroupId, node_id: u64, rack_id: u64) -> Self {
        Self {
            disk_group_id,
            node_id,
            rack_id,
            status: RwLock::new(HwStatus::Up),
            bind: RwLock::new((0, 0)),
            disks: RwLock::new(Vec::new()),
            disk_index: RwLock::new(HashMap::new()),
            allocating_disks: RwLock::new(Arc::new(Vec::new())),
            pos_v_disk_ctx: AtomicU64::new(0),
        }
    }

    /// Add a disk to this disk-group. Rebuilds the allocatable disk set.
    pub fn add_disk(&self, disk: Arc<DdbDisk>) {
        {
            let mut idx = self.disk_index.write().unwrap();
            idx.insert(disk.disk_id, Arc::clone(&disk));
        }
        self.disks.write().unwrap().push(disk);
        self.rebuild_allocating_disks();
    }

    /// Rebuild the RCU-published allocatable disk set.
    pub fn rebuild_allocating_disks(&self) {
        let disks = self.disks.read().unwrap();
        let new_ctx: Vec<Arc<DdbDisk>> = disks.iter().filter(|d| d.allocatable()).cloned().collect();
        *self.allocating_disks.write().unwrap() = Arc::new(new_ctx);
    }

    /// Whether this disk-group can accept allocations.
    pub fn allocatable(&self) -> bool {
        *self.status.read().unwrap() == HwStatus::Up
    }

    /// Allocate a single block — round-robin over allocatable disks
    /// within this disk-group, skipping `exclude_disks`.
    ///
    /// Returns `NoSpace` if no disk can satisfy the request.
    pub fn allocate_block(
        &self,
        unit_count: u32,
        exclude_disks: &[DiskId],
        cas_retry_limit: u32,
        zone_rotate_count: u32,
    ) -> Result<AllocClaim, AllocError> {
        if !self.allocatable() {
            return Err(AllocError::NoSpace);
        }
        let ctx = Arc::clone(&self.allocating_disks.read().unwrap());
        if ctx.is_empty() {
            return Err(AllocError::NoSpace);
        }
        let ctx_len = ctx.len();
        #[allow(clippy::cast_possible_truncation)]
        let start = self.pos_v_disk_ctx.fetch_add(1, Ordering::Relaxed) as usize % ctx_len;
        for i in 0..ctx_len {
            let disk = &ctx[(start + i) % ctx_len];
            if exclude_disks.contains(&disk.disk_id) {
                continue;
            }
            if let Some((zone, range)) = disk.disk_allocate(unit_count, cas_retry_limit, zone_rotate_count) {
                return Ok((Arc::clone(disk), zone, range));
            }
        }
        Err(AllocError::NoSpace)
    }

    /// Allocate `count` blocks of `unit_count` units each, spreading
    /// across disks (anti-affinity via `exclude_disks`).
    ///
    /// Tries round-robin first; if not all `count` claimed, retries
    /// remaining with a full scan.
    pub fn allocate_blocks(
        &self,
        unit_count: u32,
        count: u32,
        exclude_disks: &[DiskId],
        cas_retry_limit: u32,
        zone_rotate_count: u32,
    ) -> Result<Vec<AllocClaim>, AllocError> {
        let mut results: Vec<AllocClaim> = Vec::new();
        let mut used_disks: Vec<DiskId> = exclude_disks.to_vec();

        // First pass: round-robin.
        for _ in 0..count {
            match self.allocate_block(unit_count, &used_disks, cas_retry_limit, zone_rotate_count) {
                Ok((disk, zone, range)) => {
                    used_disks.push(disk.disk_id);
                    results.push((disk, zone, range));
                }
                Err(AllocError::NoSpace) => break,
            }
        }

        if results.len() == count as usize {
            return Ok(results);
        }

        // Second pass: full scan (random start, skip excluded + used).
        let ctx = Arc::clone(&self.allocating_disks.read().unwrap());
        while results.len() < count as usize {
            let mut claimed = false;
            #[allow(clippy::cast_possible_truncation)]
            let start = rand::random_range(0..ctx.len().max(1));
            for i in 0..ctx.len() {
                let disk = &ctx[(start + i) % ctx.len()];
                if used_disks.contains(&disk.disk_id) {
                    continue;
                }
                if let Some((zone, range)) =
                    disk.disk_allocate(unit_count, cas_retry_limit, zone_rotate_count)
                {
                    used_disks.push(disk.disk_id);
                    results.push((Arc::clone(disk), zone, range));
                    claimed = true;
                    if results.len() >= count as usize {
                        break;
                    }
                }
            }
            if !claimed {
                break;
            }
        }

        if results.len() == count as usize {
            Ok(results)
        } else {
            Err(AllocError::NoSpace)
        }
    }

    /// Free a block by `(disk_id, zone_index, unit_offset, unit_count)`.
    pub fn free_block(&self, disk_id: &DiskId, zone_index: u32, unit_offset: u64, unit_count: u32) -> bool {
        let disk = {
            let idx = self.disk_index.read().unwrap();
            idx.get(disk_id).cloned()
        };
        match disk {
            Some(d) => d.free(zone_index, unit_offset, unit_count),
            None => false,
        }
    }
}

/// Allocation errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AllocError {
    /// No disk/zone can satisfy the request.
    NoSpace,
}
