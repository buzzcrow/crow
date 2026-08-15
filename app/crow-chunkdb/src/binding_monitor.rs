// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Dynamic binding monitor — reads chunkdb instances from the service
//! registry, computes a uniform range assignment, and writes the
//! binding table to group 0.
//!
//! See `doc/working/design-r99-dynamic-range-binding.md` §5.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use tracing::{info, warn};

use crow_protocol::common::{ChunkdbRangeBindingValue, InstanceValue};
use crow_protocol::key::{ChunkdbRangeBindingKey, TextKey};

use crow_kv_client::{CrowkvClient, Error, Result, ServiceRegistryClient};

const G0_STORE: u64 = 0;
const G0_GROUP: u64 = 0;
const CHUNKDB_SERVICE: &str = "chunkdb";

/// Dynamic binding monitor — keeps the chunkdb instance range binding
/// table in group 0 in sync with the service registry.
pub struct BindingMonitor {
    kv: Arc<CrowkvClient>,
    svc: ServiceRegistryClient,
    interval: Duration,
}

impl BindingMonitor {
    /// Create a new binding monitor.
    #[must_use]
    pub fn new(kv: Arc<CrowkvClient>, svc: ServiceRegistryClient, interval: Duration) -> Self {
        Self { kv, svc, interval }
    }

    /// One monitoring tick: read chunkdb instances from the service
    /// registry, compute range assignment, write binding table to
    /// group 0.
    pub async fn tick(&self) -> Result<()> {
        let instances = self.svc.read_all_instances(CHUNKDB_SERVICE).await?;
        let bindings = compute_assignment(&instances);
        self.write_bindings(&bindings).await?;
        info!(
            instance_count = instances.len(),
            binding_count = bindings.len(),
            "binding monitor tick"
        );
        Ok(())
    }

    /// Run loop: tick periodically until stop signal.
    pub async fn run(self, mut stop: watch::Receiver<bool>) {
        let mut interval = tokio::time::interval(self.interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if let Err(e) = self.tick().await {
                        warn!(error = %e, "binding monitor tick failed");
                    }
                }
                _ = stop.changed() => {
                    if *stop.borrow() {
                        info!("binding monitor stopping");
                        break;
                    }
                }
            }
        }
    }

    /// Write the binding table to group 0. Replaces all existing
    /// `/chunkdb/range_bind/` entries.
    async fn write_bindings(&self, bindings: &[ChunkdbRangeBindingValue]) -> Result<()> {
        // Delete existing bindings, then write new ones. A full
        // replace is simpler than diffing for v1.
        self.delete_all_bindings().await?;
        for b in bindings {
            let key = ChunkdbRangeBindingKey {
                range_start: u16::try_from(b.range_start).unwrap_or(0),
            };
            let path = key.to_path();
            let payload = serde_json::to_vec(b).map_err(|e| Error::SysdataDecode {
                key: path.clone(),
                reason: e.to_string(),
            })?;
            self.kv
                .put(G0_STORE, G0_GROUP, path.as_bytes(), &payload, None)
                .await
                .map(|_| ())?;
        }
        Ok(())
    }

    /// Delete all existing `/chunkdb/range_bind/` entries.
    async fn delete_all_bindings(&self) -> Result<()> {
        let prefix = ChunkdbRangeBindingKey::text_prefix_all();
        let mut start_after: Vec<u8> = Vec::new();
        loop {
            let outcome = self
                .kv
                .scan(
                    G0_STORE,
                    G0_GROUP,
                    prefix.as_bytes(),
                    &start_after,
                    &[],
                    0,
                    crow_kv_client::ReadMode::Linearizable,
                    None,
                    true, // keys_only
                    None,
                )
                .await?;
            for (k, _) in &outcome.items {
                self.kv.delete(G0_STORE, G0_GROUP, k, None).await.map(|_| ())?;
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
        Ok(())
    }
}

/// Compute a uniform range assignment for the given chunkdb instances.
/// Divides `[0, 65535]` into `N` equal inclusive ranges where
/// `N = instances.len()`. Each instance gets
/// `[i * 65536 / N, (i+1) * 65536 / N - 1]` (inclusive).
///
/// - 0 instances → empty vec.
/// - 1 instance → full range `[0, 65535]`.
/// - 3 instances → `[0, 21845]`, `[21846, 43690]`, `[43691, 65535]`.
pub fn compute_assignment(instances: &[(u64, InstanceValue)]) -> Vec<ChunkdbRangeBindingValue> {
    if instances.is_empty() {
        return Vec::new();
    }
    // Sort by instance_id for deterministic assignment.
    let mut sorted: Vec<&(u64, InstanceValue)> = instances.iter().collect();
    sorted.sort_by_key(|(id, _)| *id);

    let n = u32::try_from(sorted.len()).unwrap_or(u32::MAX);
    let total = u32::from(u16::MAX) + 1; // 65536
    let mut out = Vec::with_capacity(sorted.len());
    for (i, (id, val)) in sorted.iter().enumerate() {
        let i = u32::try_from(i).unwrap_or(u32::MAX);
        let start = (i * total) / n;
        let end = ((i + 1) * total) / n;
        // Inclusive end = exclusive_end - 1; clamp to 0 for the last
        // bucket if total is 0 (never happens, but safe).
        let end_inclusive = if end == 0 { 0 } else { end - 1 };
        out.push(ChunkdbRangeBindingValue {
            range_start: start,
            range_end: end_inclusive,
            instance_id: *id,
            grpc_endpoint: val.grpc_endpoint.clone(),
        });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn instance(id: u64, endpoint: &str) -> (u64, InstanceValue) {
        (
            id,
            InstanceValue {
                instance_id: id,
                grpc_endpoint: endpoint.to_string(),
                last_heartbeat_ms: 0,
                extra: None,
            },
        )
    }

    #[test]
    fn compute_assignment_single_instance() {
        let instances = vec![instance(1, "http://a:1")];
        let bindings = compute_assignment(&instances);
        assert_eq!(bindings.len(), 1);
        assert_eq!(bindings[0].range_start, 0);
        assert_eq!(bindings[0].range_end, u32::from(u16::MAX));
        assert_eq!(bindings[0].instance_id, 1);
    }

    #[test]
    fn compute_assignment_three_instances() {
        let instances = vec![
            instance(3, "http://c:1"),
            instance(1, "http://a:1"),
            instance(2, "http://b:1"),
        ];
        let bindings = compute_assignment(&instances);
        assert_eq!(bindings.len(), 3);
        // Sorted by instance_id.
        assert_eq!(bindings[0].instance_id, 1);
        assert_eq!(bindings[1].instance_id, 2);
        assert_eq!(bindings[2].instance_id, 3);
        // Ranges: [0, 21844], [21845, 43689], [43690, 65535].
        assert_eq!(bindings[0].range_start, 0);
        assert_eq!(bindings[0].range_end, 21_844);
        assert_eq!(bindings[1].range_start, 21_845);
        assert_eq!(bindings[1].range_end, 43_689);
        assert_eq!(bindings[2].range_start, 43_690);
        assert_eq!(bindings[2].range_end, u32::from(u16::MAX));
        // Ranges are contiguous (end + 1 == next start).
        assert_eq!(bindings[0].range_end + 1, bindings[1].range_start);
        assert_eq!(bindings[1].range_end + 1, bindings[2].range_start);
    }

    #[test]
    fn compute_assignment_zero_instances() {
        let instances: Vec<(u64, InstanceValue)> = Vec::new();
        let bindings = compute_assignment(&instances);
        assert!(bindings.is_empty());
    }

    #[test]
    fn compute_assignment_two_instances() {
        let instances = vec![instance(1, "http://a:1"), instance(2, "http://b:1")];
        let bindings = compute_assignment(&instances);
        assert_eq!(bindings.len(), 2);
        // Ranges: [0, 32767], [32768, 65535].
        assert_eq!(bindings[0].range_start, 0);
        assert_eq!(bindings[0].range_end, 32_767);
        assert_eq!(bindings[1].range_start, 32_768);
        assert_eq!(bindings[1].range_end, u32::from(u16::MAX));
        assert_eq!(bindings[0].range_end + 1, bindings[1].range_start);
    }
}
