// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! chunkdb range binding strategy — implements `BindingStrategy` for
//! chunkdb instance sharding using non-contiguous sub-ranges.
//!
//! See `doc/working/design-r99-dynamic-range-binding.md` §2.

use crow_protocol::common::{ChunkdbRangeBindingValue, InstanceValue, RangeStatus};
use crow_protocol::key::{ChunkdbRangeBindingKey, TextKey};

use crate::binding_framework::BindingStrategy;
use crate::{CrowkvClient, Error, ReadMode, Result};

const G0_STORE: u64 = 0;
const G0_GROUP: u64 = 0;

/// Default sub-range count for the binding table.
pub const DEFAULT_SUB_RANGE_COUNT: u32 = 1024;

/// chunkdb range binding strategy — divides the bucket space into
/// `sub_range_count` fixed sub-ranges and assigns them to chunkdb
/// instances.
#[derive(Debug, Clone)]
pub struct ChunkdbRangeStrategy {
    sub_range_count: u32,
}

impl ChunkdbRangeStrategy {
    /// Create a new strategy with the default sub-range count (1024).
    #[must_use]
    pub fn new() -> Self {
        Self {
            sub_range_count: DEFAULT_SUB_RANGE_COUNT,
        }
    }

    /// Create a strategy with a custom sub-range count.
    #[must_use]
    pub fn with_count(sub_range_count: u32) -> Self {
        Self { sub_range_count }
    }
}

impl Default for ChunkdbRangeStrategy {
    fn default() -> Self {
        Self::new()
    }
}

impl BindingStrategy for ChunkdbRangeStrategy {
    type Binding = ChunkdbRangeBindingValue;

    fn compute_assignment(&self, instances: &[(u64, InstanceValue)]) -> Vec<Self::Binding> {
        compute_sub_range_assignment(instances, self.sub_range_count)
    }

    async fn write_bindings(&self, kv: &CrowkvClient, bindings: &[Self::Binding]) -> Result<()> {
        // PUT each binding (idempotent overwrite). No delete-all — the
        // sub-range count is fixed, so the key set is stable; changed
        // entries are overwritten in place. This avoids the non-atomic
        // delete-all window.
        for b in bindings {
            let key = ChunkdbRangeBindingKey {
                sub_range_index: b.sub_range_index,
            };
            let path = key.to_path();
            let payload = serde_json::to_vec(b).map_err(|e| Error::SysdataDecode {
                key: path.clone(),
                reason: e.to_string(),
            })?;
            kv.put(G0_STORE, G0_GROUP, path.as_bytes(), &payload, None)
                .await
                .map(|_| ())?;
        }
        Ok(())
    }

    async fn read_bindings(&self, kv: &CrowkvClient) -> Result<Vec<Self::Binding>> {
        let prefix = ChunkdbRangeBindingKey::text_prefix_all();
        let mut bindings: Vec<ChunkdbRangeBindingValue> = Vec::new();
        let mut start_after: Vec<u8> = Vec::new();
        loop {
            let outcome = kv
                .scan(
                    G0_STORE,
                    G0_GROUP,
                    prefix.as_bytes(),
                    &start_after,
                    &[],
                    0,
                    ReadMode::Linearizable,
                    None,
                    false,
                    None,
                )
                .await?;
            for (k, v) in &outcome.items {
                let path = std::str::from_utf8(k).map_err(|e| Error::SysdataDecode {
                    key: prefix.clone(),
                    reason: e.to_string(),
                })?;
                let val: ChunkdbRangeBindingValue =
                    serde_json::from_slice(v).map_err(|e| Error::SysdataDecode {
                        key: path.to_string(),
                        reason: e.to_string(),
                    })?;
                bindings.push(val);
            }
            if !outcome.truncated || outcome.items.is_empty() {
                break;
            }
            if let Some((last_key, _)) = outcome.items.last() {
                start_after = last_key.to_vec();
            } else {
                break;
            }
        }
        Ok(bindings)
    }

    fn compute_incremental_assignment(
        &self,
        current: &[Self::Binding],
        instances: &[(u64, InstanceValue)],
    ) -> (Vec<Self::Binding>, bool) {
        compute_incremental_sub_range_assignment(current, instances, self.sub_range_count)
    }
}

/// Compute a uniform sub-range assignment for the given chunkdb instances.
/// Divides `N` sub-ranges as evenly as possible among the instances.
#[must_use]
pub fn compute_sub_range_assignment(
    instances: &[(u64, InstanceValue)],
    sub_range_count: u32,
) -> Vec<ChunkdbRangeBindingValue> {
    if instances.is_empty() {
        return Vec::new();
    }
    let mut sorted: Vec<&(u64, InstanceValue)> = instances.iter().collect();
    sorted.sort_by_key(|(id, _)| *id);

    let n = u32::try_from(sorted.len()).unwrap_or(u32::MAX);
    let total_buckets = u32::from(u16::MAX) + 1;
    let sub_range_width = total_buckets / sub_range_count;
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX));

    let mut out = Vec::with_capacity(sub_range_count as usize);
    for sr in 0..sub_range_count {
        let i = sr * n / sub_range_count;
        let (id, val) = sorted[i as usize];
        let range_start = sr * sub_range_width;
        let range_end = if sr == sub_range_count - 1 {
            u32::from(u16::MAX)
        } else {
            (sr + 1) * sub_range_width - 1
        };
        out.push(ChunkdbRangeBindingValue {
            sub_range_index: sr,
            range_start,
            range_end,
            instance_id: *id,
            rpc_endpoint: val.rpc_endpoint.clone(),
            original_instance_id: 0,
            original_endpoint: String::new(),
            status: RangeStatus::Stable as i32,
            last_change_time_ms: now_ms,
        });
    }
    out
}

/// Compute an incremental sub-range assignment — preserves existing
/// `InTransition` state for unchanged sub-ranges and marks changed
/// sub-ranges as `InTransition` with `original_instance_id` set to the
/// old owner. Returns the new bindings + whether any sub-range changed
/// owner. When no sub-range changed, the caller skips the write (avoids
/// frequent rewrites). When instances is empty, keeps the existing
/// bindings (does not wipe the table).
///
/// The actual migration flow (dual-serve, cutover, completion) is R103.
/// This algorithm only sets up the transition state; R103 drives the
/// `InTransition → Stable` cutover.
#[must_use]
pub fn compute_incremental_sub_range_assignment(
    current: &[ChunkdbRangeBindingValue],
    instances: &[(u64, InstanceValue)],
    sub_range_count: u32,
) -> (Vec<ChunkdbRangeBindingValue>, bool) {
    if instances.is_empty() {
        // No instances → keep existing bindings, don't wipe the table.
        return (current.to_vec(), false);
    }

    let desired = compute_sub_range_assignment(instances, sub_range_count);
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX));

    // Index current bindings by sub_range_index for O(1) lookup.
    let current_map: std::collections::HashMap<u32, &ChunkdbRangeBindingValue> =
        current.iter().map(|b| (b.sub_range_index, b)).collect();

    let mut changed = false;
    let mut out = Vec::with_capacity(desired.len());
    for d in desired {
        if let Some(c) = current_map.get(&d.sub_range_index) {
            if c.instance_id == d.instance_id {
                // Unchanged — keep existing binding (preserve InTransition).
                out.push((*c).clone());
            } else {
                // Owner changed — mark InTransition with original = old owner.
                changed = true;
                out.push(ChunkdbRangeBindingValue {
                    sub_range_index: d.sub_range_index,
                    range_start: d.range_start,
                    range_end: d.range_end,
                    instance_id: d.instance_id,
                    rpc_endpoint: d.rpc_endpoint.clone(),
                    original_instance_id: c.instance_id,
                    original_endpoint: c.rpc_endpoint.clone(),
                    status: RangeStatus::InTransition as i32,
                    last_change_time_ms: now_ms,
                });
            }
        } else {
            // No existing binding for this sub-range — new entry.
            changed = true;
            out.push(d);
        }
    }
    (out, changed)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn instance(id: u64, endpoint: &str) -> (u64, InstanceValue) {
        (
            id,
            InstanceValue {
                instance_id: id,
                rpc_endpoint: endpoint.to_string(),
                last_heartbeat_ms: 0,
                extra: None,
            },
        )
    }

    #[test]
    fn compute_assignment_single_instance() {
        let instances = vec![instance(1, "http://a:1")];
        let bindings = compute_sub_range_assignment(&instances, 8);
        assert_eq!(bindings.len(), 8);
        for b in &bindings {
            assert_eq!(b.instance_id, 1);
        }
        assert_eq!(bindings[0].sub_range_index, 0);
        assert_eq!(bindings[0].range_start, 0);
        assert_eq!(bindings[7].range_end, u32::from(u16::MAX));
    }

    #[test]
    fn compute_assignment_three_instances() {
        let instances = vec![
            instance(3, "http://c:1"),
            instance(1, "http://a:1"),
            instance(2, "http://b:1"),
        ];
        let bindings = compute_sub_range_assignment(&instances, 9);
        assert_eq!(bindings.len(), 9);
        assert_eq!(bindings[0].instance_id, 1);
        assert_eq!(bindings[2].instance_id, 1);
        assert_eq!(bindings[3].instance_id, 2);
        assert_eq!(bindings[5].instance_id, 2);
        assert_eq!(bindings[6].instance_id, 3);
        assert_eq!(bindings[8].instance_id, 3);
    }

    #[test]
    fn compute_assignment_zero_instances() {
        let instances: Vec<(u64, InstanceValue)> = Vec::new();
        let bindings = compute_sub_range_assignment(&instances, 1024);
        assert!(bindings.is_empty());
    }

    #[test]
    fn compute_assignment_two_instances() {
        let instances = vec![instance(1, "http://a:1"), instance(2, "http://b:1")];
        let bindings = compute_sub_range_assignment(&instances, 8);
        assert_eq!(bindings.len(), 8);
        assert_eq!(bindings[0].instance_id, 1);
        assert_eq!(bindings[3].instance_id, 1);
        assert_eq!(bindings[4].instance_id, 2);
        assert_eq!(bindings[7].instance_id, 2);
    }

    #[test]
    fn sub_range_bounds_correct() {
        let instances = vec![instance(1, "http://a:1")];
        let bindings = compute_sub_range_assignment(&instances, 4);
        // 4 sub-ranges, each 16384 buckets wide.
        assert_eq!(bindings[0].range_start, 0);
        assert_eq!(bindings[0].range_end, 16_383);
        assert_eq!(bindings[1].range_start, 16_384);
        assert_eq!(bindings[1].range_end, 32_767);
        assert_eq!(bindings[2].range_start, 32_768);
        assert_eq!(bindings[2].range_end, 49_151);
        assert_eq!(bindings[3].range_start, 49_152);
        assert_eq!(bindings[3].range_end, u32::from(u16::MAX));
    }

    #[test]
    fn all_bindings_stable_status() {
        let instances = vec![instance(1, "http://a:1"), instance(2, "http://b:1")];
        let bindings = compute_sub_range_assignment(&instances, 8);
        for b in &bindings {
            assert_eq!(b.status, RangeStatus::Stable as i32);
            assert_eq!(b.original_instance_id, 0);
            assert!(b.original_endpoint.is_empty());
        }
    }

    // ── incremental assignment tests ───────────────────────────────

    #[test]
    fn incremental_no_change_returns_unchanged() {
        let instances = vec![instance(1, "http://a:1"), instance(2, "http://b:1")];
        let current = compute_sub_range_assignment(&instances, 8);
        let (result, changed) = compute_incremental_sub_range_assignment(&current, &instances, 8);
        assert!(!changed, "no instance change → should not write");
        assert_eq!(result.len(), 8);
        // All bindings preserved as-is (Stable, no original_instance).
        for b in &result {
            assert_eq!(b.status, RangeStatus::Stable as i32);
            assert_eq!(b.original_instance_id, 0);
        }
    }

    #[test]
    fn incremental_instance_join_marks_changed_ranges() {
        // Start with 1 instance owning all 8 sub-ranges.
        let initial = vec![instance(1, "http://a:1")];
        let current = compute_sub_range_assignment(&initial, 8);

        // Instance 2 joins → half the sub-ranges move to instance 2.
        let after = vec![instance(1, "http://a:1"), instance(2, "http://b:1")];
        let (result, changed) = compute_incremental_sub_range_assignment(&current, &after, 8);
        assert!(changed, "instance join → should write");

        // Sub-ranges 0-3 stay with instance 1 (unchanged → Stable).
        for b in &result[..4] {
            assert_eq!(b.instance_id, 1);
            assert_eq!(b.status, RangeStatus::Stable as i32);
            assert_eq!(b.original_instance_id, 0);
        }
        // Sub-ranges 4-7 move to instance 2 (changed → InTransition).
        for b in &result[4..] {
            assert_eq!(b.instance_id, 2);
            assert_eq!(b.status, RangeStatus::InTransition as i32);
            assert_eq!(b.original_instance_id, 1);
            assert_eq!(b.original_endpoint, "http://a:1");
        }
    }

    #[test]
    fn incremental_instance_leave_marks_changed_ranges() {
        // Start with 2 instances.
        let initial = vec![instance(1, "http://a:1"), instance(2, "http://b:1")];
        let current = compute_sub_range_assignment(&initial, 8);

        // Instance 2 leaves → all sub-ranges go to instance 1.
        let after = vec![instance(1, "http://a:1")];
        let (result, changed) = compute_incremental_sub_range_assignment(&current, &after, 8);
        assert!(changed, "instance leave → should write");

        // Sub-ranges 0-3 were already instance 1 (unchanged → Stable).
        for b in &result[..4] {
            assert_eq!(b.instance_id, 1);
            assert_eq!(b.status, RangeStatus::Stable as i32);
        }
        // Sub-ranges 4-7 move from instance 2 → instance 1 (InTransition).
        for b in &result[4..] {
            assert_eq!(b.instance_id, 1);
            assert_eq!(b.status, RangeStatus::InTransition as i32);
            assert_eq!(b.original_instance_id, 2);
            assert_eq!(b.original_endpoint, "http://b:1");
        }
    }

    #[test]
    fn incremental_empty_instances_keeps_existing() {
        let initial = vec![instance(1, "http://a:1")];
        let current = compute_sub_range_assignment(&initial, 8);
        let (result, changed) = compute_incremental_sub_range_assignment(&current, &[], 8);
        assert!(!changed, "empty instances → should not wipe table");
        assert_eq!(result.len(), 8, "existing bindings preserved");
        for b in &result {
            assert_eq!(b.instance_id, 1);
        }
    }

    #[test]
    fn incremental_empty_current_all_new() {
        let instances = vec![instance(1, "http://a:1")];
        let (result, changed) = compute_incremental_sub_range_assignment(&[], &instances, 8);
        assert!(changed, "empty current → all new entries");
        assert_eq!(result.len(), 8);
        for b in &result {
            assert_eq!(b.status, RangeStatus::Stable as i32);
            assert_eq!(b.original_instance_id, 0);
        }
    }

    #[test]
    fn incremental_preserves_in_transition_for_unchanged() {
        // Current has a sub-range already InTransition (from a previous tick).
        let mut current = compute_sub_range_assignment(&[instance(1, "http://a:1")], 8);
        current[3].status = RangeStatus::InTransition as i32;
        current[3].original_instance_id = 99;
        current[3].original_endpoint = "http://old:1".to_string();

        // Same instance set → no change, InTransition preserved.
        let instances = vec![instance(1, "http://a:1")];
        let (result, changed) = compute_incremental_sub_range_assignment(&current, &instances, 8);
        assert!(!changed);
        assert_eq!(result[3].status, RangeStatus::InTransition as i32);
        assert_eq!(result[3].original_instance_id, 99);
        assert_eq!(result[3].original_endpoint, "http://old:1");
    }
}
