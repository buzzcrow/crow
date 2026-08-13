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
use crow_kv_client::{BatchOp, CrowkvClient, GetOutcome, JournalOp, ReadMode, Result};
use crow_protocol::common::{ChunkId, DiskId};
use crow_protocol::diskdb::rpc::{BlockState, BusyBlockValue, FreeBlockValue, Segment, ZoneValue};
use crow_protocol::key::{BinaryKey, BusyBlockKey, FreeBlockKey, ZoneKey};
use crow_protocol::ZoneValueExt;

use crate::domain::disk_group::{AllocClaim, AllocError, DdbDiskGroup};

/// `(store_id, group_id)` identifying a bound paxos data group.
pub type Bind = (u64, u64);

/// Errors from the free path when `validate_owner_on_free` is enabled.
#[derive(Debug)]
pub enum FreeError {
    /// KV client error during validation or persist.
    Kv(crow_kv_client::Error),
    /// Block is not busy (no `BusyBlockKey` exists) — double-free or
    /// never allocated.
    NotBusy {
        disk_id: DiskId,
        zone_index: u32,
        unit_offset: u64,
    },
    /// `owner_chunk` in the `BusyBlockValue` does not match the
    /// `Segment`'s `owner_chunk` — ownership mismatch.
    OwnerMismatch {
        disk_id: DiskId,
        zone_index: u32,
        unit_offset: u64,
        expected: ChunkId,
        actual: ChunkId,
    },
}

impl std::fmt::Display for FreeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Kv(e) => write!(f, "kv error: {e}"),
            Self::NotBusy {
                disk_id,
                zone_index,
                unit_offset,
            } => write!(
                f,
                "block not busy: disk {disk_id:?} zone {zone_index} offset {unit_offset}"
            ),
            Self::OwnerMismatch {
                disk_id,
                zone_index,
                unit_offset,
                expected,
                actual,
            } => write!(
                f,
                "owner mismatch: disk {disk_id:?} zone {zone_index} offset {unit_offset}, expected {expected:?} actual {actual:?}"
            ),
        }
    }
}

impl std::error::Error for FreeError {}

impl From<crow_kv_client::Error> for FreeError {
    fn from(e: crow_kv_client::Error) -> Self {
        Self::Kv(e)
    }
}

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

    /// Point-lookup a `BusyBlockValue` at `BusyBlockKey`. Returns
    /// `Ok(None)` if the key does not exist (block is not busy).
    /// Used by the `validate_owner_on_free` path.
    pub async fn get_busy(
        &self,
        bind: Bind,
        disk_id: &DiskId,
        zone_index: u32,
        unit_offset: u64,
    ) -> Result<Option<BusyBlockValue>> {
        let key = BusyBlockKey {
            disk_id: *disk_id,
            zone_index,
            unit_offset,
        };
        let (store_id, group_id) = bind;
        let outcome = self
            .kv
            .get(store_id, group_id, &key.to_bytes(), ReadMode::Linearizable, None)
            .await?;
        match outcome {
            GetOutcome::Found { value, .. } => {
                let bv = bincode::deserialize::<BusyBlockValue>(&value).map_err(|e| {
                    crow_kv_client::Error::SysdataDecode {
                        key: format!("{:02x?}", key.to_bytes()),
                        reason: e.to_string(),
                    }
                })?;
                Ok(Some(bv))
            }
            GetOutcome::NotFound => Ok(None),
        }
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

        // 1. ZoneValue (point lookup via get)
        let zone_key = ZoneKey {
            disk_id: *disk_id,
            zone_index,
        };
        let zone_bytes = zone_key.to_bytes();
        let zone_get = self
            .kv
            .get(store_id, group_id, &zone_bytes, ReadMode::Linearizable, None)
            .await?;
        if let GetOutcome::Found { value, .. } = zone_get {
            records.zone_value = Some(bincode::deserialize::<ZoneValue>(&value).map_err(|e| {
                crow_kv_client::Error::SysdataDecode {
                    key: format!("{zone_bytes:02x?}"),
                    reason: e.to_string(),
                }
            })?);
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
                &[],
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
                &[],
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

    /// Point-lookup the `ZoneValue` snapshot at `ZoneKey` only (1
    /// round-trip). Used by R73 recovery strategy 2 step a (load
    /// snapshot) — avoids the 2 wasted scans of `read_zone_records`
    /// when only the snapshot is needed. Returns `Ok(None)` if no
    /// `ZoneValue` exists (fresh zone).
    pub async fn get_zone_value(
        &self,
        bind: Bind,
        disk_id: &DiskId,
        zone_index: u32,
    ) -> Result<Option<ZoneValue>> {
        let key = ZoneKey {
            disk_id: *disk_id,
            zone_index,
        };
        let (store_id, group_id) = bind;
        let outcome = self
            .kv
            .get(store_id, group_id, &key.to_bytes(), ReadMode::Linearizable, None)
            .await?;
        match outcome {
            GetOutcome::Found { value, .. } => {
                let zv = ZoneValue::from_bytes(&value).map_err(|e| crow_kv_client::Error::SysdataDecode {
                    key: format!("{:02x?}", key.to_bytes()),
                    reason: e,
                })?;
                Ok(Some(zv))
            }
            GetOutcome::NotFound => Ok(None),
        }
    }

    /// Get the data group's current applied frontier (for compaction's
    /// `snapshot_slot`). Uses a linearizable `scan` with an empty
    /// prefix and reads `read_slot` from the response — the read
    /// barrier resolves to `contiguous_applied`, which is the slot to
    /// anchor a new snapshot at.
    pub async fn get_applied_slot(&self, bind: Bind) -> Result<u64> {
        let (store_id, group_id) = bind;
        let outcome = self
            .kv
            .scan(
                store_id,
                group_id,
                &[],
                &[],
                &[],
                1,
                ReadMode::Linearizable,
                None,
                true,
                None,
            )
            .await?;
        Ok(outcome.read_slot)
    }

    /// Slot-ordered journal scan over `BusyBlockKey` ops for one zone
    /// (R73 recovery strategy 2 step c, scan 1). Wraps
    /// `CrowkvClient::journal_scan` with `BusyBlockKey::prefix_for_zone`.
    /// Returns ops in slot order; the caller replays them to rebuild
    /// the bitmap.
    pub async fn journal_scan_busy(
        &self,
        bind: Bind,
        min_slot: u64,
        max_slot: u64,
        disk_id: &DiskId,
        zone_index: u32,
    ) -> Result<Vec<JournalOp>> {
        let prefix = BusyBlockKey::prefix_for_zone(disk_id, zone_index);
        let (store_id, group_id) = bind;
        let outcome = self
            .kv
            .journal_scan(
                store_id,
                group_id,
                min_slot,
                max_slot,
                &prefix,
                0,
                0,
                ReadMode::Linearizable,
                None,
            )
            .await?;
        Ok(outcome.ops)
    }

    /// Slot-ordered journal scan over `FreeBlockKey` ops for one zone
    /// (R73 recovery strategy 2 step c, scan 2). Wraps
    /// `CrowkvClient::journal_scan` with `FreeBlockKey::prefix_for_zone`.
    pub async fn journal_scan_free(
        &self,
        bind: Bind,
        min_slot: u64,
        max_slot: u64,
        disk_id: &DiskId,
        zone_index: u32,
    ) -> Result<Vec<JournalOp>> {
        let prefix = FreeBlockKey::prefix_for_zone(disk_id, zone_index);
        let (store_id, group_id) = bind;
        let outcome = self
            .kv
            .journal_scan(
                store_id,
                group_id,
                min_slot,
                max_slot,
                &prefix,
                0,
                0,
                ReadMode::Linearizable,
                None,
            )
            .await?;
        Ok(outcome.ops)
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
    node: &Arc<DdbDiskGroup>,
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
    node: &Arc<DdbDiskGroup>,
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
/// Phase 0 (optional): when `validate_owner_on_free` is `true`, read
/// the `BusyBlockValue` from the data group and validate `owner_chunk`
/// before touching the bitmap. Rejects on `NotBusy` or `OwnerMismatch`.
/// When `false` (default), no KV read — `owner_chunk` comes from the
/// `Segment` (§14).
/// Phase 1: clear bitmap locally (per-bit CAS).
/// Phase 2: persist `FreeBlockValue` (delete `BusyBlockKey` + put
/// `FreeBlockKey` in one `batch_write`).
///
/// See §4.6.
///
/// # Errors
/// Returns `FreeError::NotBusy` / `FreeError::OwnerMismatch` on
/// validation failure (no bitmap clear happens). Returns
/// `FreeError::Kv` if the persist fails — the bitmap clear already
/// happened locally; the §12 ghost-allocation scanner reconciles on
/// restart.
pub async fn free_block(
    node: &Arc<DdbDiskGroup>,
    segment: &Segment,
    kv: &DataGroupClient,
    validate_owner_on_free: bool,
) -> std::result::Result<(), FreeError> {
    let disk_id = segment.disk_id.ok_or_else(|| {
        FreeError::Kv(crow_kv_client::Error::SysdataDecode {
            key: "segment.disk_id".to_string(),
            reason: "missing disk_id in Segment".to_string(),
        })
    })?;
    let bind = *node.bind.read().unwrap();

    // Phase 0: validate ownership (optional, one paxos round-trip).
    if validate_owner_on_free {
        let busy = kv
            .get_busy(bind, &disk_id, segment.zone_index, segment.unit_offset)
            .await?;
        match busy {
            None => {
                return Err(FreeError::NotBusy {
                    disk_id,
                    zone_index: segment.zone_index,
                    unit_offset: segment.unit_offset,
                });
            }
            Some(bv) => {
                if bv.owner_chunk != segment.owner_chunk {
                    return Err(FreeError::OwnerMismatch {
                        disk_id,
                        zone_index: segment.zone_index,
                        unit_offset: segment.unit_offset,
                        expected: segment.owner_chunk.unwrap_or_default(),
                        actual: bv.owner_chunk.unwrap_or_default(),
                    });
                }
            }
        }
    }

    // Phase 1: clear bitmap locally.
    if !node.free_block(
        &disk_id,
        segment.zone_index,
        segment.unit_offset,
        segment.unit_count,
    ) {
        return Err(FreeError::Kv(crow_kv_client::Error::SysdataDecode {
            key: "free_block".to_string(),
            reason: format!(
                "bitmap clear failed for disk {disk_id:?} zone {} offset {}",
                segment.zone_index, segment.unit_offset
            ),
        }));
    }

    // Phase 2: persist FreeBlockValue.
    let value = FreeBlockValue {
        unit_count: segment.unit_count,
        previous_owner: segment.owner_chunk,
    };
    kv.persist_free(bind, &disk_id, segment.zone_index, segment.unit_offset, &value)
        .await
        .map_err(FreeError::from)
}

/// Free multiple blocks (one `batch_write` per data group). See §4.6.
///
/// When `validate_owner_on_free` is `true`, all segments are validated
/// first (all-or-nothing) — if any segment fails validation, no bitmap
/// is cleared and the error is returned.
///
/// # Errors
/// Returns `FreeError::NotBusy` / `FreeError::OwnerMismatch` on
/// validation failure (no bitmap clear happens). Returns
/// `FreeError::Kv` if the persist fails — bitmap clears already
/// happened locally.
pub async fn free_blocks(
    node: &Arc<DdbDiskGroup>,
    segments: &[Segment],
    kv: &DataGroupClient,
    validate_owner_on_free: bool,
) -> std::result::Result<(), FreeError> {
    let bind = *node.bind.read().unwrap();

    // Phase 0: validate ownership for all segments (all-or-nothing).
    if validate_owner_on_free {
        for seg in segments {
            let disk_id = seg.disk_id.ok_or_else(|| {
                FreeError::Kv(crow_kv_client::Error::SysdataDecode {
                    key: "segment.disk_id".to_string(),
                    reason: "missing disk_id in Segment".to_string(),
                })
            })?;
            let busy = kv
                .get_busy(bind, &disk_id, seg.zone_index, seg.unit_offset)
                .await?;
            match busy {
                None => {
                    return Err(FreeError::NotBusy {
                        disk_id,
                        zone_index: seg.zone_index,
                        unit_offset: seg.unit_offset,
                    });
                }
                Some(bv) => {
                    if bv.owner_chunk != seg.owner_chunk {
                        return Err(FreeError::OwnerMismatch {
                            disk_id,
                            zone_index: seg.zone_index,
                            unit_offset: seg.unit_offset,
                            expected: seg.owner_chunk.unwrap_or_default(),
                            actual: bv.owner_chunk.unwrap_or_default(),
                        });
                    }
                }
            }
        }
    }

    // Phase 1: clear all bitmaps locally.
    for seg in segments {
        if let Some(disk_id) = seg.disk_id {
            if !node.free_block(&disk_id, seg.zone_index, seg.unit_offset, seg.unit_count) {
                tracing::warn!(
                    "bitmap clear failed for disk {disk_id:?} zone {} offset {} — ghost scanner will reconcile",
                    seg.zone_index,
                    seg.unit_offset
                );
            }
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

    kv.persist_free_batch(bind, &records)
        .await
        .map_err(FreeError::from)
}
