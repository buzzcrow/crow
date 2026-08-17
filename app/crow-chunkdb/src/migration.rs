// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Migration handling — dual-write during bucket range migration.
//!
//! Design §5.4b: when a bucket range moves from one KV group to
//! another, the migration goes through phases: Copying → Cutover →
//! Cleanup → Complete. During Copying/Cutover, writes go to both
//! groups; reads try new first, fall back to old.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use tracing::{info, warn};

use crow_kv_client::{CrowkvClient, ReadMode, ScanOutcome};
use crow_protocol::common::ChunkId;

use crate::routing::{BindingCache, BucketBinding, MigrationState};

/// Migration task — copies chunk records from old KV group to new
/// KV group for a migrating bucket range.
pub struct MigrationTask {
    kv: Arc<CrowkvClient>,
    binding: BucketBinding,
    /// Dwell time after copy completes before cleanup.
    dwell: Duration,
}

impl MigrationTask {
    #[must_use]
    pub fn new(kv: Arc<CrowkvClient>, binding: BucketBinding, dwell: Duration) -> Self {
        Self { kv, binding, dwell }
    }

    /// Run the migration: copy → dwell → cleanup.
    ///
    /// # Errors
    /// Returns a String error if the copy or cleanup fails.
    pub async fn run(self, stop: watch::Receiver<bool>) -> Result<(), String> {
        let (old_store, old_group) = self
            .binding
            .old_kv_store_id
            .zip(self.binding.old_kv_group_id)
            .ok_or_else(|| "migration task: no old group in binding".to_string())?;

        info!(
            bucket_start = self.binding.start,
            bucket_end = self.binding.end,
            old_group = old_group,
            new_group = self.binding.kv_group_id,
            "migration: starting copy phase"
        );

        // Phase 1: Copy — scan old group, copy each chunk to new group.
        self.copy_phase(old_store, old_group).await?;

        info!("migration: copy complete, dwelling before cleanup");
        tokio::time::sleep(self.dwell).await;

        // Phase 2: Cleanup — delete old copies.
        self.cleanup_phase(old_store, old_group).await?;

        info!("migration: cleanup complete");
        let _ = stop;
        Ok(())
    }

    /// Copy phase: scan old group for chunks in the bucket range,
    /// copy each to the new group.
    async fn copy_phase(&self, old_store: u64, old_group: u64) -> Result<(), String> {
        let prefix = b"/chunk/";
        let mut start_after: Vec<u8> = Vec::new();
        let batch_size = 100u32;

        loop {
            let outcome: ScanOutcome = self
                .kv
                .scan(
                    old_store,
                    old_group,
                    prefix,
                    &start_after,
                    &[],
                    batch_size,
                    ReadMode::Linearizable,
                    None,
                    false,
                    None,
                )
                .await
                .map_err(|e| format!("copy scan: {e}"))?;

            if outcome.items.is_empty() {
                break;
            }

            for (key, value) in &outcome.items {
                // Always advance the cursor — even for out-of-range
                // items, otherwise a batch of only out-of-range items
                // would loop forever.
                start_after = key.to_vec();

                // Check if this chunk's bucket is in our range.
                let in_range = key_to_chunk_id(key).is_some_and(|id| {
                    let b = crate::routing::hash_to_bucket(&id);
                    b >= self.binding.start && b < self.binding.end
                });
                if !in_range {
                    continue;
                }

                // Copy to new group (overwrite — idempotent).
                self.kv
                    .put(
                        self.binding.kv_store_id,
                        self.binding.kv_group_id,
                        key,
                        value,
                        None,
                    )
                    .await
                    .map_err(|e| format!("copy put: {e}"))?;
            }

            if !outcome.truncated {
                break;
            }
        }

        Ok(())
    }

    /// Cleanup phase: delete old copies from the old group.
    async fn cleanup_phase(&self, old_store: u64, old_group: u64) -> Result<(), String> {
        let prefix = b"/chunk/";
        let mut start_after: Vec<u8> = Vec::new();
        let batch_size = 100u32;

        loop {
            let outcome: ScanOutcome = self
                .kv
                .scan(
                    old_store,
                    old_group,
                    prefix,
                    &start_after,
                    &[],
                    batch_size,
                    ReadMode::Linearizable,
                    None,
                    false,
                    None,
                )
                .await
                .map_err(|e| format!("cleanup scan: {e}"))?;

            if outcome.items.is_empty() {
                break;
            }

            for (key, _value) in &outcome.items {
                // Always advance the cursor.
                start_after = key.to_vec();

                let in_range = key_to_chunk_id(key).is_some_and(|id| {
                    let b = crate::routing::hash_to_bucket(&id);
                    b >= self.binding.start && b < self.binding.end
                });
                if !in_range {
                    continue;
                }

                if let Err(e) = self.kv.delete(old_store, old_group, key, None).await {
                    warn!(error = %e, "cleanup: delete failed for old copy");
                }
            }

            if !outcome.truncated {
                break;
            }
        }

        Ok(())
    }
}

/// Parse a chunk key (`/chunk/<16-bytes>`) back to a `ChunkId`.
fn key_to_chunk_id(key: &[u8]) -> Option<ChunkId> {
    if key.len() != 23 || &key[..7] != b"/chunk/" {
        return None;
    }
    let high = u64::from_be_bytes(key[7..15].try_into().ok()?);
    let low = u64::from_be_bytes(key[15..23].try_into().ok()?);
    Some(ChunkId { high, low })
}

/// Check if a binding is in an active migration state.
pub fn is_migrating(state: MigrationState) -> bool {
    matches!(
        state,
        MigrationState::Copying | MigrationState::Cutover | MigrationState::Cleanup
    )
}

/// Update the binding cache with a new binding for a bucket range.
pub fn update_binding(cache: &BindingCache, binding: BucketBinding) {
    let mut table = cache.snapshot();
    // Replace any binding that overlaps with the new one.
    table
        .bindings_mut()
        .retain(|b| b.end <= binding.start || b.start >= binding.end);
    table.bindings_mut().push(binding);
    // Sort by start for deterministic routing.
    table.bindings_mut().sort_unstable_by_key(|b| b.start);
    cache.replace(table);
}
