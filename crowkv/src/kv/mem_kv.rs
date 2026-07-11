use std::collections::BTreeMap;

use parking_lot::RwLock;

use super::op::Cell;
use super::{Batch, BatchOp, KVEngine, Op};

/// In-memory, single-version engine backed by an ordered `BTreeMap` under a
/// single `RwLock`. The write lock held for the duration of `apply` makes the
/// batch atomic to readers; ordered iteration gives `scan` / `iter_all` for
/// free. No persistence — intended for unit/integration tests and behavior
/// validation.
#[derive(Default)]
pub struct InMemKV {
    map: RwLock<BTreeMap<Vec<u8>, (u64, Cell)>>,
}

impl InMemKV {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl KVEngine for InMemKV {
    fn apply(&self, slot: u64, batch: &Batch) {
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
    }

    fn get(&self, key: &[u8]) -> Option<(u64, Vec<u8>)> {
        let map = self.map.read();
        match map.get(key) {
            Some((slot, Cell::Value(v))) => Some((*slot, v.clone())),
            _ => None,
        }
    }

    fn scan(&self, prefix: &[u8], limit: usize) -> (Vec<(Vec<u8>, u64, Vec<u8>)>, bool) {
        let map = self.map.read();
        let mut items = Vec::new();
        let mut truncated = false;
        // BTreeMap is key-ordered; iterate from the prefix and stop once keys
        // no longer share it.
        for (key, (slot, cell)) in map.range(prefix.to_vec()..) {
            if !key.starts_with(prefix) {
                break;
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
        (items, truncated)
    }

    fn iter_all(&self) -> Vec<(Vec<u8>, u64, Cell)> {
        self.map
            .read()
            .iter()
            .map(|(k, (slot, cell))| (k.clone(), *slot, cell.clone()))
            .collect()
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
}
