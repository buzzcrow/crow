// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#![allow(dead_code)]

//! Test-only utilities for `KVEngine` inspection and cross-engine comparison.
//! Uses `as_any()` downcasting to call engine-specific `iter_all` methods,
//! keeping `iter_all`/`compare` off the main `KVEngine` trait.

use crate::mem_kv::InMemKV;
use crowdb_kv::kv::{Cell, CrowdbTreeEngine, EngineDiff, KVEngine};

/// Full ordered stream including tombstones, via downcast to the concrete
/// engine type.
pub fn iter_all_dyn(engine: &dyn KVEngine) -> Vec<(Vec<u8>, u64, Cell)> {
    if let Some(e) = engine.as_any().downcast_ref::<InMemKV>() {
        return e.iter_all();
    }
    if let Some(e) = engine.as_any().downcast_ref::<CrowdbTreeEngine>() {
        return e.iter_all_for_tests();
    }
    Vec::new()
}

/// Logical diff between two engines, sorted by key. Empty means both engines
/// hold the same `(slot, cell)` for every key. Compared exactly, including
/// resolved-slot and tombstones.
pub fn compare_dyn(left: &dyn KVEngine, right: &dyn KVEngine) -> Vec<EngineDiff> {
    let l = iter_all_dyn(left);
    let r = iter_all_dyn(right);
    let mut diffs = Vec::new();
    let (mut i, mut j) = (0usize, 0usize);
    while i < l.len() || j < r.len() {
        match (l.get(i), r.get(j)) {
            (Some(li), Some(ri)) => match li.0.cmp(&ri.0) {
                std::cmp::Ordering::Less => {
                    diffs.push(EngineDiff {
                        key: li.0.clone(),
                        left: Some((li.1, li.2.clone())),
                        right: None,
                    });
                    i += 1;
                }
                std::cmp::Ordering::Greater => {
                    diffs.push(EngineDiff {
                        key: ri.0.clone(),
                        left: None,
                        right: Some((ri.1, ri.2.clone())),
                    });
                    j += 1;
                }
                std::cmp::Ordering::Equal => {
                    if li.1 != ri.1 || li.2 != ri.2 {
                        diffs.push(EngineDiff {
                            key: li.0.clone(),
                            left: Some((li.1, li.2.clone())),
                            right: Some((ri.1, ri.2.clone())),
                        });
                    }
                    i += 1;
                    j += 1;
                }
            },
            (Some(li), None) => {
                diffs.push(EngineDiff {
                    key: li.0.clone(),
                    left: Some((li.1, li.2.clone())),
                    right: None,
                });
                i += 1;
            }
            (None, Some(ri)) => {
                diffs.push(EngineDiff {
                    key: ri.0.clone(),
                    left: None,
                    right: Some((ri.1, ri.2.clone())),
                });
                j += 1;
            }
            (None, None) => break,
        }
    }
    diffs
}
