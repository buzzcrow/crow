// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! `ChunkIoWriter` — shared push-based async interface for chunk
//! data-path writers.

use bytes::Bytes;

use crate::{Location, Result};

/// Result of `on_data` — does the writer need more data?
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeedStatus {
    /// Buffer stored; writer has capacity — send more data.
    Continue,
    /// Buffer stored; writer is at capacity — pause feeding.
    /// Poll `require_data()` before resuming.
    Pause,
}

/// Caller-side backpressure strategy. Selects how to react when
/// `require_data()` returns false. Not a property of the writer.
#[derive(Debug, Clone, Copy)]
pub enum BackpressurePolicy {
    /// Dedicated upload task: ignore `require_data`, call `on_data`
    /// directly — it blocks until capacity. Use when the task has
    /// nothing else to do.
    Blocking,
    /// Shared handler task: check `require_data()` first; if false,
    /// return 503 / apply TCP flow control — never block the handler.
    NonBlocking,
}

/// Push-based chunk IO writer trait.
///
/// Contract:
/// - `on_data` **always stores the buffer** (awaits until internal
///   capacity is available — never rejects). Returns `Continue` if the
///   next push would not block, `Pause` if the writer is now at
///   capacity.
/// - `require_data` is a cheap non-async hint: `true` if `on_data`
///   would not block now.
/// - `on_finish` signals end of input: flush, seal, return
///   `Vec<Location>`.
/// - `on_error` aborts: return `Location`s of already-sealed chunks
///   for cleanup.
///
/// Two caller strategies (selected by `BackpressurePolicy` on the
/// caller side):
/// - **Blocking**: ignore `require_data`, call `on_data` directly.
/// - **Non-blocking**: check `require_data()` first; if false, back
///   off (yield / return 503).
#[async_trait::async_trait]
pub trait ChunkIoWriter: Send {
    /// Push a data buffer. Always stores (awaits capacity if needed).
    async fn on_data(&mut self, buffer: Bytes) -> Result<FeedStatus>;
    /// End of input: flush, seal, return the `Location` array.
    async fn on_finish(&mut self) -> Result<Vec<Location>>;
    /// Abort: return `Location`s of already-sealed chunks for cleanup.
    async fn on_error(&mut self) -> Result<Vec<Location>>;
    /// Non-async pre-check: `true` if `on_data` would not block now.
    fn require_data(&self) -> bool;
}
