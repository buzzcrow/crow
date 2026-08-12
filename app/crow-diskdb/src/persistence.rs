// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! [`DataGroupClient`] — wraps [`CrowkvClient`] for put/delete/scan on
//! the disk-group's bound paxos data group.
//!
//! Uses `(store_id, group_id)` from `Node.bind` (set by the sync
//! loop). Keys are the binary keys from `lib/crow-protocol/src/key/
//! diskdb.rs` (`BusyBlockKey` / `FreeBlockKey` / `ZoneKey`), encoded
//! via `BinaryKey::encode`. Values are bincode-serialized proto types.
//!
//! See `doc/working/design-diskdb-server.md` §4.4.

use std::sync::Arc;

use bytes::Bytes;
use crow_kv_client::{BatchOp, CrowkvClient, ReadMode, Result};
use crow_protocol::common::{ChunkId, DiskId};
use crow_protocol::diskdb::rpc::{BlockState, BusyBlockValue, FreeBlockValue, Segment, ZoneValue};
use crow_protocol::key::{BinaryKey, BusyBlockKey, FreeBlockKey, ZoneKey};

use crate::node::{AllocClaim, AllocError, Node};

/// `(store_id, group_id)` identifying a bound paxos data group.
pub type Bind = (u64, u64);

/// A busy-block record read from the data group.
#[derive(Debug, Clone)]
pub struct BusyRecord {
    pub key: BusyBlockKey,
    pub value: BusyBlockValue,
}

/// A free-block record read from the data group.
#[derive(Debug, Clone)]
pub struct FreeRecord {
    pub key: FreeBlockKey,
    pub value: FreeBlockValue,
}

/// All records for one zone (used by R73 recovery).
#[derive(Debug, Clone, Default)]
pub struct ZoneRecords {
    pub zone_value: Option<ZoneValue>,
    pub busy: Vec<BusyRecord>,
    pub free: Vec<FreeRecord>,
}

/// Client for the disk-group's bound paxos data group.
///
/// All methods take `(store_id, group_id)` from `Node.bind`. The
/// wrapped `CrowkvClient` must have its topology seeded with the
/// data-group leader endpoint.
pub struct DataGroupClient {
    kv: Arc<CrowkvClient>,
}

impl DataGroupClient {
    /// Wrap a `CrowkvClient` for data-group access.
    #[must_use]
    pub fn new(kv: CrowkvClient) -> Self {
        Self { kv: Arc::new(kv) }
    }

    /// Wrap an already-shared `CrowkvClient`.
    #[must_use]
    pub fn from_shared(kv: Arc<CrowkvClient>) -> Self {
        Self { kv }
    }

    /// Access the underlying `CrowkvClient`.
    #[must_use]
    pub fn kv(&self) -> &CrowkvClient {
        &self.kv
    }

    /// Persist a single busy-block record: `put` to `BusyBlockKey`.
    ///
    /// Per §3.4/§7, this also deletes any prior `FreeBlockKey` for the
    /// same offset (re-allocation of a freed block). Done as one
    /// `batch_write` for atomicity.
    pub async fn persist_busy(
        &self,
        bind: Bind,
        disk_id: &DiskId,
        zone_index: u32,
        unit_offset: u64,
        value: &BusyBlockValue,
    ) -> Result<()> {
        let busy_key = BusyBlockKey {
            disk_id: *disk_id,
            zone_index,
            unit_offset,
        };
        let free_key = FreeBlockKey {
            disk_id: *disk_id,
            zone_index,
            unit_offset,
        };
        let busy_bytes = bincode::serialize(value).expect("serialize BusyBlockValue");
        let ops = vec![
            BatchOp::Put {
                key: Bytes::from(busy_key.to_bytes()),
                value: Bytes::from(busy_bytes),
            },
            BatchOp::Delete {
                key: Bytes::from(free_key.to_bytes()),
            },
        ];
        let (store_id, group_id) = bind;
        self.kv.batch_write(store_id, group_id, &ops).await.map(|_| ())
    }

    /// Persist a batch of busy-block records in one `batch_write`
    /// (multi-block allocate; one round-trip per data group).
    pub async fn persist_busy_batch(
        &self,
        bind: Bind,
        records: &[(DiskId, u32, u64, BusyBlockValue)],
    ) -> Result<()> {
        let mut ops = Vec::with_capacity(records.len() * 2);
        for (disk_id, zone_index, unit_offset, value) in records {
            let busy_key = BusyBlockKey {
                disk_id: *disk_id,
                zone_index: *zone_index,
                unit_offset: *unit_offset,
            };
            let free_key = FreeBlockKey {
                disk_id: *disk_id,
                zone_index: *zone_index,
                unit_offset: *unit_offset,
            };
            let busy_bytes = bincode::serialize(value).expect("serialize BusyBlockValue");
            ops.push(BatchOp::Put {
                key: Bytes::from(busy_key.to_bytes()),
                value: Bytes::from(busy_bytes),
            });
            ops.push(BatchOp::Delete {
                key: Bytes::from(free_key.to_bytes()),
            });
        }
        let (store_id, group_id) = bind;
        self.kv.batch_write(store_id, group_id, &ops).await.map(|_| ())
    }

    /// Persist a single free: one `batch_write` that deletes the
    /// `BusyBlockKey` and puts the `FreeBlockValue` at `FreeBlockKey`.
    pub async fn persist_free(
        &self,
        bind: Bind,
        disk_id: &DiskId,
        zone_index: u32,
        unit_offset: u64,
        value: &FreeBlockValue,
    ) -> Result<()> {
        let busy_key = BusyBlockKey {
            disk_id: *disk_id,
            zone_index,
            unit_offset,
        };
        let free_key = FreeBlockKey {
            disk_id: *disk_id,
            zone_index,
            unit_offset,
        };
        let free_bytes = bincode::serialize(value).expect("serialize FreeBlockValue");
        let ops = vec![
            BatchOp::Delete {
                key: Bytes::from(busy_key.to_bytes()),
            },
            BatchOp::Put {
                key: Bytes::from(free_key.to_bytes()),
                value: Bytes::from(free_bytes),
            },
        ];
        let (store_id, group_id) = bind;
        self.kv.batch_write(store_id, group_id, &ops).await.map(|_| ())
    }

    /// Persist a batch of free records in one `batch_write` (one
    /// round-trip per data group). Reused by R79's size-threshold
    /// batch.
    pub async fn persist_free_batch(
        &self,
        bind: Bind,
        records: &[(DiskId, u32, u64, FreeBlockValue)],
    ) -> Result<()> {
        let mut ops = Vec::with_capacity(records.len() * 2);
        for (disk_id, zone_index, unit_offset, value) in records {
            let busy_key = BusyBlockKey {
                disk_id: *disk_id,
                zone_index: *zone_index,
                unit_offset: *unit_offset,
            };
            let free_key = FreeBlockKey {
                disk_id: *disk_id,
                zone_index: *zone_index,
                unit_offset: *unit_offset,
            };
            let free_bytes = bincode::serialize(value).expect("serialize FreeBlockValue");
            ops.push(BatchOp::Delete {
                key: Bytes::from(busy_key.to_bytes()),
            });
            ops.push(BatchOp::Put {
                key: Bytes::from(free_key.to_bytes()),
                value: Bytes::from(free_bytes),
            });
        }
        let (store_id, group_id) = bind;
        self.kv.batch_write(store_id, group_id, &ops).await.map(|_| ())
    }

    /// Put a `ZoneValue` snapshot at `ZoneKey`.
    pub async fn put_zone(
        &self,
        bind: Bind,
        disk_id: &DiskId,
        zone_index: u32,
        value: &ZoneValue,
    ) -> Result<()> {
        let key = ZoneKey {
            disk_id: *disk_id,
            zone_index,
        };
        let bytes = bincode::serialize(value).expect("serialize ZoneValue");
        let (store_id, group_id) = bind;
        self.kv
            .put(store_id, group_id, &key.to_bytes(), &bytes, None)
            .await
            .map(|_| ())
    }

    /// Read all records for one zone: `ZoneValue` + all `BusyBlockKey`
    /// and `FreeBlockKey` entries. Used by R73 recovery.
    ///
    /// Does three prefix scans (zone, busy, free) and decodes the
    /// values.
    pub async fn read_zone_records(
        &self,
        bind: Bind,
        disk_id: &DiskId,
        zone_index: u32,
    ) -> Result<ZoneRecords> {
        let (store_id, group_id) = bind;
        let mut records = ZoneRecords::default();

        // 1. ZoneValue
        let zone_key = ZoneKey {
            disk_id: *disk_id,
            zone_index,
        };
        let zone_bytes = zone_key.to_bytes();
        let zone_scan = self
            .kv
            .scan(
                store_id,
                group_id,
                &zone_bytes,
                &[],
                &zone_bytes,
                1,
                ReadMode::Linearizable,
                None,
                false,
                None,
            )
            .await?;
        for (key, value) in &zone_scan.items {
            if key.as_ref() == zone_bytes.as_slice() {
                records.zone_value = Some(bincode::deserialize::<ZoneValue>(value).map_err(|e| {
                    crow_kv_client::Error::SysdataDecode {
                        key: format!("{zone_bytes:02x?}"),
                        reason: e.to_string(),
                    }
                })?);
            }
        }

        // 2. BusyBlock records
        let busy_prefix = BusyBlockKey::prefix_for_zone(disk_id, zone_index);
        let busy_scan = self
            .kv
            .scan(
                store_id,
                group_id,
                &busy_prefix,
                &[],
                &busy_prefix,
                0, // unlimited
                ReadMode::Linearizable,
                None,
                false,
                None,
            )
            .await?;
        for (key, value) in &busy_scan.items {
            if let Ok(bk) = BusyBlockKey::from_bytes(key) {
                if let Ok(bv) = bincode::deserialize::<BusyBlockValue>(value) {
                    records.busy.push(BusyRecord { key: bk, value: bv });
                }
            }
        }

        // 3. FreeBlock records
        let free_prefix = FreeBlockKey::prefix_for_zone(disk_id, zone_index);
        let free_scan = self
            .kv
            .scan(
                store_id,
                group_id,
                &free_prefix,
                &[],
                &free_prefix,
                0, // unlimited
                ReadMode::Linearizable,
                None,
                false,
                None,
            )
            .await?;
        for (key, value) in &free_scan.items {
            if let Ok(fk) = FreeBlockKey::from_bytes(key) {
                if let Ok(fv) = bincode::deserialize::<FreeBlockValue>(value) {
                    records.free.push(FreeRecord { key: fk, value: fv });
                }
            }
        }

        Ok(records)
    }

    /// Delete a batch of free records by key (R73 compaction).
    pub async fn delete_free_records_batch(&self, bind: Bind, keys: &[Vec<u8>]) -> Result<()> {
        let ops: Vec<BatchOp> = keys
            .iter()
            .map(|k| BatchOp::Delete {
                key: Bytes::from(k.clone()),
            })
            .collect();
        let (store_id, group_id) = bind;
        self.kv.batch_write(store_id, group_id, &ops).await.map(|_| ())
    }
}

// ── Two-phase async allocation ──────────────────────────────────

/// Two-phase allocate a single block.
///
/// Phase 1 (sync): bitmap CAS via `node.allocate_block`.
/// Phase 2 (async): persist `BusyBlockValue` via `DataGroupClient`.
///
/// On Phase 2 failure, rolls back the bitmap bits (Phase 1 undo) and
/// returns the error. See §4.5.
///
/// # Errors
/// Returns `AllocError::NoSpace` if no disk/zone can satisfy the
/// request, or a KV client error if the persist fails.
#[allow(clippy::too_many_arguments)]
pub async fn allocate_block(
    node: &Arc<Node>,
    unit_count: u32,
    owner_chunk: &ChunkId,
    unit_size: u32,
    kv: &DataGroupClient,
    cas_retry_limit: u32,
    zone_rotate_count: u32,
) -> std::result::Result<Segment, AllocError> {
    // Phase 1: bitmap CAS.
    let (disk, zone, range) = node.allocate_block(unit_count, &[], cas_retry_limit, zone_rotate_count)?;

    // Phase 2: persist BusyBlockValue.
    let value = BusyBlockValue {
        unit_count: range.unit_count,
        owner_chunk: Some(*owner_chunk),
        unit_size,
        state: BlockState::Ok as i32,
    };
    let bind = *node.bind.read().unwrap();
    if let Err(e) = kv
        .persist_busy(bind, &disk.disk_id, zone.zone_index, range.unit_offset, &value)
        .await
    {
        // Rollback Phase 1.
        let _ = zone.free(range.unit_offset, range.unit_count);
        tracing::warn!("allocate persist failed, rolled back bitmap: {e}");
        return Err(AllocError::NoSpace);
    }

    Ok(Segment {
        disk_id: Some(disk.disk_id),
        zone_index: zone.zone_index,
        unit_offset: range.unit_offset,
        unit_count: range.unit_count,
        owner_chunk: Some(*owner_chunk),
    })
}

/// Two-phase allocate multiple blocks (one `batch_write` per data
/// group). See §4.5.
///
/// # Errors
/// Returns `AllocError::NoSpace` if not all `count` blocks can be
/// placed, or a KV client error if the batch persist fails.
#[allow(clippy::too_many_arguments)]
pub async fn allocate_blocks(
    node: &Arc<Node>,
    unit_count: u32,
    count: u32,
    exclude_disks: &[DiskId],
    owner_chunk: &ChunkId,
    unit_size: u32,
    kv: &DataGroupClient,
    cas_retry_limit: u32,
    zone_rotate_count: u32,
) -> std::result::Result<Vec<Segment>, AllocError> {
    // Phase 1: bitmap CAS for all blocks.
    let claims: Vec<AllocClaim> = node.allocate_blocks(
        unit_count,
        count,
        exclude_disks,
        cas_retry_limit,
        zone_rotate_count,
    )?;

    // Phase 2: persist all in one batch_write.
    let records: Vec<(DiskId, u32, u64, BusyBlockValue)> = claims
        .iter()
        .map(|(disk, zone, range)| {
            (
                disk.disk_id,
                zone.zone_index,
                range.unit_offset,
                BusyBlockValue {
                    unit_count: range.unit_count,
                    owner_chunk: Some(*owner_chunk),
                    unit_size,
                    state: BlockState::Ok as i32,
                },
            )
        })
        .collect();

    let bind = *node.bind.read().unwrap();
    if let Err(e) = kv.persist_busy_batch(bind, &records).await {
        // Rollback ALL Phase 1 claims.
        for (_, zone, range) in &claims {
            let _ = zone.free(range.unit_offset, range.unit_count);
        }
        tracing::warn!("allocate_blocks persist failed, rolled back {count} claims: {e}");
        return Err(AllocError::NoSpace);
    }

    let segments: Vec<Segment> = claims
        .iter()
        .map(|(disk, zone, range)| Segment {
            disk_id: Some(disk.disk_id),
            zone_index: zone.zone_index,
            unit_offset: range.unit_offset,
            unit_count: range.unit_count,
            owner_chunk: Some(*owner_chunk),
        })
        .collect();
    Ok(segments)
}

// ── Immediate free ──────────────────────────────────────────────

/// Free a single block. v1: synchronous (no batch, no timer).
///
/// Phase 1: clear bitmap locally (per-bit CAS).
/// Phase 2: persist `FreeBlockValue` (delete `BusyBlockKey` + put
/// `FreeBlockKey` in one `batch_write`).
///
/// See §4.6. No KV read on free in v1 — `owner_chunk` comes from the
/// `Segment`.
///
/// # Errors
/// Returns a KV client error if the persist fails. The bitmap clear
/// already happened locally; the §12 ghost-allocation scanner
/// reconciles on restart.
pub async fn free_block(node: &Arc<Node>, segment: &Segment, kv: &DataGroupClient) -> Result<()> {
    let disk_id = segment
        .disk_id
        .ok_or_else(|| crow_kv_client::Error::SysdataDecode {
            key: "segment.disk_id".to_string(),
            reason: "missing disk_id in Segment".to_string(),
        })?;

    // Phase 1: clear bitmap locally.
    if !node.free_block(
        &disk_id,
        segment.zone_index,
        segment.unit_offset,
        segment.unit_count,
    ) {
        return Err(crow_kv_client::Error::SysdataDecode {
            key: "free_block".to_string(),
            reason: format!(
                "bitmap clear failed for disk {disk_id:?} zone {} offset {}",
                segment.zone_index, segment.unit_offset
            ),
        });
    }

    // Phase 2: persist FreeBlockValue.
    let value = FreeBlockValue {
        unit_count: segment.unit_count,
        previous_owner: segment.owner_chunk,
    };
    let bind = *node.bind.read().unwrap();
    kv.persist_free(bind, &disk_id, segment.zone_index, segment.unit_offset, &value)
        .await
}

/// Free multiple blocks (one `batch_write` per data group). See §4.6.
///
/// # Errors
/// Returns a KV client error if any persist fails. Bitmap clears
/// already happened locally.
pub async fn free_blocks(node: &Arc<Node>, segments: &[Segment], kv: &DataGroupClient) -> Result<()> {
    // Phase 1: clear all bitmaps locally.
    for seg in segments {
        let disk_id = seg.disk_id.ok_or_else(|| crow_kv_client::Error::SysdataDecode {
            key: "segment.disk_id".to_string(),
            reason: "missing disk_id in Segment".to_string(),
        })?;
        if !node.free_block(&disk_id, seg.zone_index, seg.unit_offset, seg.unit_count) {
            tracing::warn!(
                "bitmap clear failed for disk {disk_id:?} zone {} offset {} — ghost scanner will reconcile",
                seg.zone_index,
                seg.unit_offset
            );
        }
    }

    // Phase 2: persist all in one batch_write.
    let records: Vec<(DiskId, u32, u64, FreeBlockValue)> = segments
        .iter()
        .filter_map(|seg| {
            seg.disk_id.map(|disk_id| {
                (
                    disk_id,
                    seg.zone_index,
                    seg.unit_offset,
                    FreeBlockValue {
                        unit_count: seg.unit_count,
                        previous_owner: seg.owner_chunk,
                    },
                )
            })
        })
        .collect();

    if records.is_empty() {
        return Ok(());
    }

    let bind = *node.bind.read().unwrap();
    kv.persist_free_batch(bind, &records).await
}
