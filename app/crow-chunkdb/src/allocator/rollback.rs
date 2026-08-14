// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Rollback — free successfully-allocated segments on partial failure.
//!
//! Design §8: rollback is synchronous within the `AllocateChunk` call.
//! If `FreeBlocks` fails, orphan segments are logged for the diskdb
//! orphan scanner to reclaim later.

use tracing::warn;

use crow_protocol::diskdb::rpc::Segment;

/// Free a list of segments via the provided free function.
///
/// `free_fn` is an async function that takes the segments and returns
/// `Result<(), String>`. On failure, logs the orphan segments.
pub async fn rollback_segments<F, Fut>(segments: Vec<Segment>, free_fn: F)
where
    F: FnOnce(Vec<Segment>) -> Fut,
    Fut: std::future::Future<Output = Result<(), String>>,
{
    if segments.is_empty() {
        return;
    }

    match free_fn(segments.clone()).await {
        Ok(()) => {
            tracing::info!(segment_count = segments.len(), "rollback: freed all segments");
        }
        Err(e) => {
            warn!(
                error = %e,
                segment_count = segments.len(),
                "rollback: free_blocks failed — orphan segments logged for scanner"
            );
        }
    }
}
