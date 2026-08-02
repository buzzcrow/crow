// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#![allow(clippy::cast_possible_truncation)]
#![allow(dead_code)]

use dashmap::DashMap;
use std::collections::BTreeMap;

use crowkv::kv::{Batch, BatchOp, Cell, KVEngine, KVFuture, Op};

/// In-memory, single-version engine backed by a sharded `DashMap` so
/// reads proceed concurrent with `apply` (no global write lock). `scan`
/// and `iter_all` collect matching entries and sort, since `DashMap` is
/// not key-ordered — acceptable for test-only use. No persistence —
/// test-only, not selectable via the server CLI. Used by
/// unit/integration tests and behavior validation.
pub struct InMemKV {
    map: DashMap<Vec<u8>, (u64, Cell)>,
}

impl Default for InMemKV {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemKV {
    #[must_use]
    pub fn new() -> Self {
        Self { map: DashMap::new() }
    }

    /// Full ordered stream including tombstones. Test-only utility.
    #[must_use]
    pub fn iter_all(&self) -> Vec<(Vec<u8>, u64, Cell)> {
        let mut entries: Vec<(Vec<u8>, u64, Cell)> = self
            .map
            .iter()
            .map(|r| (r.key().clone(), r.value().0, r.value().1.clone()))
            .collect();
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        entries
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
            collapsed.insert(key.as_ref(), op);
        }
        for (key, op) in collapsed {
            let cell = match op {
                Op::Put(v) => Cell::Value(v.to_vec()),
                Op::Delete => Cell::Tombstone,
            };
            // Per-key entry API: atomically check the resolved slot and
            // insert only if this slot is newer. No global lock held —
            // reads proceed concurrent with apply.
            self.map
                .entry(key.to_vec())
                .and_modify(|(resolved, existing)| {
                    if slot > *resolved {
                        *resolved = slot;
                        *existing = cell.clone();
                    }
                })
                .or_insert((slot, cell));
        }
        // No I/O path -- an in-memory apply can never fail.
        KVFuture::ready(Ok(()))
    }

    fn get(&self, key: &[u8]) -> KVFuture<Option<(u64, Vec<u8>)>> {
        let result = match self.map.get(key) {
            Some(r) => match &r.1 {
                Cell::Value(v) => Some((r.0, v.clone())),
                Cell::Tombstone => None,
            },
            None => None,
        };
        KVFuture::ready(result)
    }

    fn scan(
        &self,
        prefix: &[u8],
        start_after: &[u8],
        limit: usize,
    ) -> KVFuture<(Vec<(Vec<u8>, u64, Vec<u8>)>, bool)> {
        // DashMap is not ordered — collect matching live entries, sort,
        // then apply the start_after cursor and limit.
        let mut items: Vec<(Vec<u8>, u64, Vec<u8>)> = self
            .map
            .iter()
            .filter_map(|r| {
                if !r.key().starts_with(prefix) {
                    return None;
                }
                if !start_after.is_empty() && r.key().as_slice() <= start_after {
                    return None;
                }
                let (slot, cell) = r.value();
                if let Cell::Value(v) = cell {
                    Some((r.key().clone(), *slot, v.clone()))
                } else {
                    None
                }
            })
            .collect();
        items.sort_by(|a, b| a.0.cmp(&b.0));
        let mut truncated = false;
        if limit != 0 && items.len() > limit {
            truncated = true;
            items.truncate(limit);
        }
        KVFuture::ready((items, truncated))
    }

    fn live_key_count(&self) -> usize {
        self.map
            .iter()
            .filter(|r| matches!(r.value().1, Cell::Value(_)))
            .count()
    }

    fn clear(&self) {
        self.map.clear();
    }

    fn snapshot_export(&self) -> Result<(u64, Vec<u8>), String> {
        // Collect owned entries and sort for deterministic snapshot output.
        let mut entries: Vec<(Vec<u8>, u64, Cell)> = self
            .map
            .iter()
            .map(|r| (r.key().clone(), r.value().0, r.value().1.clone()))
            .collect();
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        // `at_slot`: the highest slot for which this engine holds any
        // evidence of an apply. May under-report by a NoOp-only trailing
        // range (a repair-filled slot with an empty batch never reaches
        // `apply` at all -- see `PxLearner::apply_entry`), which is safe
        // per `KVEngine::resume_from_slot`'s contract: the joining replica
        // just re-fetches and re-learns a few extra (idempotent) slots.
        let at_slot = entries.iter().map(|(_, slot, _)| *slot).max().unwrap_or(0);
        let mut out = Vec::new();
        out.extend_from_slice(&MEM_SNAP_MAGIC.to_le_bytes());
        out.extend_from_slice(&MEM_SNAP_VERSION.to_le_bytes());
        out.extend_from_slice(&at_slot.to_le_bytes());
        out.extend_from_slice(&(entries.len() as u64).to_le_bytes());
        for (key, slot, cell) in &entries {
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
        self.map.clear();
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
            self.map.insert(key, (slot, cell));
        }
        Ok(at_slot)
    }
}

const MEM_SNAP_MAGIC: u32 = 0x494D_4B56; // "IMKV"
const MEM_SNAP_VERSION: u32 = 1;
