// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#![allow(clippy::cast_possible_truncation)]
#![allow(dead_code)]

use std::collections::BTreeMap;

use parking_lot::RwLock;

use crowkv::kv::{Batch, BatchOp, Cell, KVEngine, KVFuture, Op};

/// In-memory, single-version engine backed by an ordered `BTreeMap` under a
/// single `RwLock`. The write lock held for the duration of `apply` makes the
/// batch atomic to readers; ordered iteration gives `scan` / `iter_all` for
/// free. No persistence — test-only, not selectable via the server CLI.
/// Used by unit/integration tests and behavior validation.
#[derive(Default)]
pub struct InMemKV {
    map: RwLock<BTreeMap<Vec<u8>, (u64, Cell)>>,
}

impl InMemKV {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Full ordered stream including tombstones. Test-only utility.
    #[must_use]
    pub fn iter_all(&self) -> Vec<(Vec<u8>, u64, Cell)> {
        self.map
            .read()
            .iter()
            .map(|(k, (slot, cell))| (k.clone(), *slot, cell.clone()))
            .collect()
    }
}

impl KVEngine for InMemKV {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn apply(&self, slot: u64, batch: &Batch) -> KVFuture<Result<(), String>> {
        // Collapse intra-batch duplicates first: the last occurrence of a key
        // in batch order wins, so the per-key slot check below is made once
        // against the pre-batch state rather than once per occurrence (which
        // would let the first op claim the slot and skip the rest).
        let mut collapsed: BTreeMap<&[u8], &Op> = BTreeMap::new();
        for BatchOp { key, op } in &batch.ops {
            collapsed.insert(key.as_slice(), op);
        }
        let mut map = self.map.write();
        for (key, op) in collapsed {
            if let Some((resolved, _)) = map.get(key) {
                if slot <= *resolved {
                    continue;
                }
            }
            let cell = match op {
                Op::Put(v) => Cell::Value(v.clone()),
                Op::Delete => Cell::Tombstone,
            };
            map.insert(key.to_vec(), (slot, cell));
        }
        // No I/O path -- an in-memory apply can never fail.
        KVFuture::ready(Ok(()))
    }

    fn get(&self, key: &[u8]) -> KVFuture<Option<(u64, Vec<u8>)>> {
        let map = self.map.read();
        let result = match map.get(key) {
            Some((slot, Cell::Value(v))) => Some((*slot, v.clone())),
            _ => None,
        };
        KVFuture::ready(result)
    }

    fn scan(
        &self,
        prefix: &[u8],
        start_after: &[u8],
        limit: usize,
    ) -> KVFuture<(Vec<(Vec<u8>, u64, Vec<u8>)>, bool)> {
        let map = self.map.read();
        let mut items = Vec::new();
        let mut truncated = false;
        // BTreeMap is key-ordered; iterate from the lower bound and stop
        // once keys no longer share the prefix. When `start_after` is
        // empty, start from the prefix itself (avoids scanning
        // non-matching keys before the prefix range).
        let lower = if start_after.is_empty() {
            prefix.to_vec()
        } else {
            start_after.to_vec()
        };
        for (key, (slot, cell)) in map.range(lower..) {
            if !key.starts_with(prefix) {
                break;
            }
            if !start_after.is_empty() && key.as_slice() <= start_after {
                continue;
            }
            let Cell::Value(v) = cell else { continue };
            // Reaching the cap with another live match pending means the
            // result is truncated.
            if limit != 0 && items.len() >= limit {
                truncated = true;
                break;
            }
            items.push((key.clone(), *slot, v.clone()));
        }
        KVFuture::ready((items, truncated))
    }

    fn live_key_count(&self) -> usize {
        self.map
            .read()
            .values()
            .filter(|(_, cell)| matches!(cell, Cell::Value(_)))
            .count()
    }

    fn clear(&self) {
        self.map.write().clear();
    }

    fn snapshot_export(&self) -> Result<(u64, Vec<u8>), String> {
        let map = self.map.read();
        // `at_slot`: the highest slot for which this engine holds any
        // evidence of an apply. May under-report by a NoOp-only trailing
        // range (a repair-filled slot with an empty batch never reaches
        // `apply` at all -- see `PxLearner::apply_entry`), which is safe
        // per `KVEngine::resume_from_slot`'s contract: the joining replica
        // just re-fetches and re-learns a few extra (idempotent) slots.
        let at_slot = map.values().map(|(slot, _)| *slot).max().unwrap_or(0);
        let mut out = Vec::new();
        out.extend_from_slice(&MEM_SNAP_MAGIC.to_le_bytes());
        out.extend_from_slice(&MEM_SNAP_VERSION.to_le_bytes());
        out.extend_from_slice(&at_slot.to_le_bytes());
        out.extend_from_slice(&(map.len() as u64).to_le_bytes());
        for (key, (slot, cell)) in map.iter() {
            out.extend_from_slice(&(key.len() as u32).to_le_bytes());
            out.extend_from_slice(key);
            out.extend_from_slice(&slot.to_le_bytes());
            let (tombstone, value): (u8, &[u8]) = match cell {
                Cell::Tombstone => (1, &[]),
                Cell::Value(v) => (0, v.as_slice()),
            };
            out.push(tombstone);
            out.extend_from_slice(&(value.len() as u32).to_le_bytes());
            out.extend_from_slice(value);
        }
        Ok((at_slot, out))
    }

    fn snapshot_import(&self, stream: &[u8]) -> Result<u64, String> {
        let mut pos = 0usize;
        let read_u32 = |pos: &mut usize| -> Result<u32, String> {
            let bytes = stream
                .get(*pos..*pos + 4)
                .ok_or_else(|| "InMemKV snapshot import: truncated u32".to_string())?;
            *pos += 4;
            Ok(u32::from_le_bytes(bytes.try_into().unwrap()))
        };
        let read_u64 = |pos: &mut usize| -> Result<u64, String> {
            let bytes = stream
                .get(*pos..*pos + 8)
                .ok_or_else(|| "InMemKV snapshot import: truncated u64".to_string())?;
            *pos += 8;
            Ok(u64::from_le_bytes(bytes.try_into().unwrap()))
        };
        let magic = read_u32(&mut pos)?;
        if magic != MEM_SNAP_MAGIC {
            return Err(format!("InMemKV snapshot import: bad magic {magic:#x}"));
        }
        let version = read_u32(&mut pos)?;
        if version != MEM_SNAP_VERSION {
            return Err(format!("InMemKV snapshot import: unsupported version {version}"));
        }
        let at_slot = read_u64(&mut pos)?;
        let entry_count = read_u64(&mut pos)?;
        let mut new_map = BTreeMap::new();
        for _ in 0..entry_count {
            let key_len = read_u32(&mut pos)? as usize;
            let key = stream
                .get(pos..pos + key_len)
                .ok_or_else(|| "InMemKV snapshot import: truncated key".to_string())?
                .to_vec();
            pos += key_len;
            let slot = read_u64(&mut pos)?;
            let tombstone = *stream
                .get(pos)
                .ok_or_else(|| "InMemKV snapshot import: truncated tombstone flag".to_string())?;
            pos += 1;
            let value_len = read_u32(&mut pos)? as usize;
            let value = stream
                .get(pos..pos + value_len)
                .ok_or_else(|| "InMemKV snapshot import: truncated value".to_string())?
                .to_vec();
            pos += value_len;
            let cell = if tombstone != 0 {
                Cell::Tombstone
            } else {
                Cell::Value(value)
            };
            new_map.insert(key, (slot, cell));
        }
        *self.map.write() = new_map;
        Ok(at_slot)
    }
}

const MEM_SNAP_MAGIC: u32 = 0x494D_4B56; // "IMKV"
const MEM_SNAP_VERSION: u32 = 1;
