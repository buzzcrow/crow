// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! `ParityBatch` — per-strip parallel write tracker.
//!
//! Owned by each `EcStripWriter`. When writing an EC strip, each disk
//! block (data + parity) can be written in parallel. `ParityBatch`
//! tracks the in-flight write task handles and joins them (first-error
//! semantics) or aborts them. No semaphore — parallelism is bounded by
//! the number of blocks in the strip (fixed by the EC scheme).

use tokio::task::JoinHandle;

use crate::{IoError, Result};

/// Per-strip parallel write tracker.
pub struct ParityBatch {
    handles: Vec<JoinHandle<Result<()>>>,
}

impl ParityBatch {
    /// Construct a new empty batch.
    pub fn new() -> Self {
        Self { handles: Vec::new() }
    }

    /// Spawn a parallel write task (one per disk block — data or
    /// parity) and track its handle.
    pub fn spawn(&mut self, handle: JoinHandle<Result<()>>) {
        self.handles.push(handle);
    }

    /// Join all in-flight writes, return first error.
    pub async fn join_all(&mut self) -> Result<()> {
        let mut first_err: Option<IoError> = None;
        for handle in self.handles.drain(..) {
            match handle.await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    if first_err.is_none() {
                        first_err = Some(e);
                    }
                }
                Err(e) => {
                    if first_err.is_none() {
                        first_err = Some(IoError::Internal(format!("write task panicked: {e}")));
                    }
                }
            }
        }
        match first_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// Abort all in-flight writes.
    pub fn abort_all(&mut self) {
        for handle in self.handles.drain(..) {
            handle.abort();
        }
    }

    /// Number of in-flight writes.
    pub fn in_flight(&self) -> usize {
        self.handles.len()
    }
}

impl Default for ParityBatch {
    fn default() -> Self {
        Self::new()
    }
}
