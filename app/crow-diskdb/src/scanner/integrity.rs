// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Record integrity verification. Re-checks CRC on the `ZoneValue`
//! snapshot, detects records that `read_zone_records` silently skips
//! (undecodable keys/values), and validates `owner_chunk` well-
//! formedness on each `BusyBlockValue`. Full liveness cross-check
//! against caller registries is deferred.

use std::sync::Arc;

use crow_protocol::common::{ChunkId, DiskId};
use crow_protocol::diskdb::rpc::BusyBlockValue;
use crow_protocol::key::{BinaryKey, BusyBlockKey, FreeBlockKey};
use crow_protocol::ZoneValueExt;

use crate::ddb_kv_client::{Bind, DdbKvClient};
use crate::model::zone::DdbZone;
use crate::scanner::integrity::IntegrityFinding::{CorruptJournalRecord, CorruptSnapshot, OwnerMismatch};

/// One integrity finding.
#[derive(Debug, Clone)]
pub enum IntegrityFinding {
    /// `ZoneValue` snapshot failed CRC verification.
    CorruptSnapshot { disk_id: DiskId, zone_index: u32 },
    /// A journal record (busy or free) failed to decode.
    CorruptJournalRecord {
        disk_id: DiskId,
        zone_index: u32,
        key: Vec<u8>,
    },
    /// A `BusyBlockValue` has a zero `owner_chunk` (all three fields 0).
    OwnerMismatch {
        disk_id: DiskId,
        zone_index: u32,
        unit_offset: u64,
    },
}

/// Result of one integrity-scan cycle.
#[derive(Debug, Clone, Default)]
pub struct IntegrityScanResult {
    pub corrupt_snapshots: u64,
    pub corrupt_records: u64,
    pub owner_mismatches: u64,
    /// Per-finding details (capped to avoid unbounded growth).
    pub details: Vec<IntegrityFinding>,
}

/// Cap on `details`.
const DETAILS_CAP: usize = 256;

/// Scan all zones in one disk-group for record corruption + owner
/// mismatches. Skips active zones + zones whose lock is held (same
/// discipline as the ghost scan).
pub async fn scan_integrity(
    kv: &DdbKvClient,
    bind: Bind,
    disk_id: DiskId,
    zones: &[(u32, u32, Arc<DdbZone>)],
    active_zones: &[Arc<DdbZone>],
    detect_owner_mismatch: bool,
) -> IntegrityScanResult {
    let mut result = IntegrityScanResult::default();
    for &(zone_idx, _unit_capacity, ref live_zone) in zones {
        if active_zones.iter().any(|az| Arc::ptr_eq(az, live_zone)) {
            continue;
        }
        if live_zone.zone_lock.try_read().is_err() {
            continue;
        }

        // Read raw records (the scan re-uses the same prefix scans as
        // `read_zone_records` but checks every item, catching the
        // ones that `read_zone_records` silently skips).
        let (zone_value, corrupt_busy, corrupt_free) = scan_zone_records(kv, bind, disk_id, zone_idx).await;

        // CRC check on the snapshot.
        if let Some(ref zv) = zone_value {
            if !zv.verify_checksum() {
                result.corrupt_snapshots += 1;
                push_detail(
                    &mut result.details,
                    CorruptSnapshot {
                        disk_id,
                        zone_index: zone_idx,
                    },
                );
            }
        }

        // Corrupt journal records.
        for key in corrupt_busy.iter().chain(corrupt_free.iter()) {
            result.corrupt_records += 1;
            push_detail(
                &mut result.details,
                CorruptJournalRecord {
                    disk_id,
                    zone_index: zone_idx,
                    key: key.clone(),
                },
            );
        }

        // Owner mismatch check (optional).
        if detect_owner_mismatch {
            let records = kv
                .read_zone_records(bind, &disk_id, zone_idx)
                .await
                .unwrap_or_default();
            for busy in &records.busy {
                if is_zero_owner(&busy.value) {
                    result.owner_mismatches += 1;
                    push_detail(
                        &mut result.details,
                        OwnerMismatch {
                            disk_id,
                            zone_index: zone_idx,
                            unit_offset: busy.key.unit_offset,
                        },
                    );
                }
            }
        }
    }
    result
}

/// Check if a `BusyBlockValue` has a zero `owner_chunk` (all three
/// fields 0). A real allocation always has a non-zero owner.
#[cfg_attr(not(feature = "test-util"), allow(dead_code))]
pub fn is_zero_owner(bv: &BusyBlockValue) -> bool {
    match &bv.owner_chunk {
        None => true,
        Some(c) => is_zero_chunk(c),
    }
}

/// Check if a `ChunkId` is all-zeros.
#[cfg_attr(not(feature = "test-util"), allow(dead_code))]
pub fn is_zero_chunk(c: &ChunkId) -> bool {
    c.high == 0 && c.low == 0
}

/// Scan one zone's records, returning `(zone_value, corrupt_busy_keys,
/// corrupt_free_keys)`. A record is "corrupt" if its key or value
/// fails to decode — these are the records that
/// `read_zone_records` silently skips.
async fn scan_zone_records(
    kv: &DdbKvClient,
    bind: Bind,
    disk_id: DiskId,
    zone_index: u32,
) -> (
    Option<crow_protocol::diskdb::rpc::ZoneValue>,
    Vec<Vec<u8>>,
    Vec<Vec<u8>>,
) {
    let (store_id, group_id) = bind;
    let mut zone_value: Option<crow_protocol::diskdb::rpc::ZoneValue> = None;
    let mut corrupt_busy: Vec<Vec<u8>> = Vec::new();
    let mut corrupt_free: Vec<Vec<u8>> = Vec::new();

    // ZoneValue.
    let zone_key = crow_protocol::key::ZoneKey { disk_id, zone_index };
    let zone_bytes = zone_key.to_bytes();
    if let Ok(crow_kv_client::GetOutcome::Found { value, .. }) = kv
        .kv()
        .get(
            store_id,
            group_id,
            &zone_bytes,
            crow_kv_client::ReadMode::Linearizable,
            None,
        )
        .await
    {
        // Try bincode decode (CRC is checked separately below).
        if let Ok(zv) = bincode::deserialize::<crow_protocol::diskdb::rpc::ZoneValue>(&value) {
            zone_value = Some(zv);
        } else {
            // Bincode failure — synthesize a corrupt snapshot so
            // the CRC check below reports it.
            zone_value = Some(crow_protocol::diskdb::rpc::ZoneValue {
                usage_bitmap: Vec::new(),
                snapshot_slot: 0,
                crc32: 1, // intentionally wrong so verify_checksum fails
                compact_ts: 0,
            });
        }
    }

    // Busy records.
    let busy_prefix = BusyBlockKey::prefix_for_zone(&disk_id, zone_index);
    if let Ok(scan) = kv
        .kv()
        .scan(
            store_id,
            group_id,
            &busy_prefix,
            &[],
            &[],
            0,
            crow_kv_client::ReadMode::Linearizable,
            None,
            false,
            None,
        )
        .await
    {
        for (key, value) in &scan.items {
            let key_ok = BusyBlockKey::from_bytes(key).is_ok();
            let val_ok = bincode::deserialize::<BusyBlockValue>(value).is_ok();
            if !key_ok || !val_ok {
                corrupt_busy.push(key.to_vec());
            }
        }
    }

    // Free records.
    let free_prefix = FreeBlockKey::prefix_for_zone(&disk_id, zone_index);
    if let Ok(scan) = kv
        .kv()
        .scan(
            store_id,
            group_id,
            &free_prefix,
            &[],
            &[],
            0,
            crow_kv_client::ReadMode::Linearizable,
            None,
            false,
            None,
        )
        .await
    {
        for (key, value) in &scan.items {
            let key_ok = FreeBlockKey::from_bytes(key).is_ok();
            let val_ok = bincode::deserialize::<crow_protocol::diskdb::rpc::FreeBlockValue>(value).is_ok();
            if !key_ok || !val_ok {
                corrupt_free.push(key.to_vec());
            }
        }
    }

    (zone_value, corrupt_busy, corrupt_free)
}

/// Push a detail entry, respecting the cap.
fn push_detail(details: &mut Vec<IntegrityFinding>, finding: IntegrityFinding) {
    if details.len() < DETAILS_CAP {
        details.push(finding);
    }
}
