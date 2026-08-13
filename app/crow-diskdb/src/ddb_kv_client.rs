// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! [`DdbKvClient`] — wraps [`CrowkvClient`] for put/delete/scan on
//! the disk-group's bound paxos data group.
//!
//! Uses `(store_id, group_id)` from `DdbDiskGroup.bind` (set by the
//! keep-alive loop). Keys are the binary keys from `lib/crow-protocol/
//! src/key/diskdb.rs` (`BusyBlockKey` / `FreeBlockKey` / `ZoneKey`),
//! encoded via `BinaryKey::encode`. Values are bincode-serialized proto
//! types.

use std::sync::Arc;

use bytes::Bytes;
use crow_kv_client::{BatchOp, CrowkvClient, GetOutcome, JournalOp, ReadMode, Result};
use crow_protocol::common::DiskId;
use crow_protocol::diskdb::rpc::{BusyBlockValue, FreeBlockValue, ZoneValue};
use crow_protocol::key::{BinaryKey, BusyBlockKey, FreeBlockKey, ZoneKey};
use crow_protocol::ZoneValueExt;

use crate::model::records::{BusyRecord, FreeRecord, ZoneRecords};

/// `(store_id, group_id)` identifying a bound paxos data group.
pub type Bind = (u64, u64);

/// Client for the disk-group's bound paxos data group.
///
/// All methods take `(store_id, group_id)` from `DdbDiskGroup.bind`. The
/// wrapped `CrowkvClient` must have its topology seeded with the
/// data-group leader endpoint.
pub struct DdbKvClient {
    kv: Arc<CrowkvClient>,
}

impl DdbKvClient {
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
