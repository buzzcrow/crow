// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! `WriterPool` — bounds concurrent writers by memory budget.
//!
//! Filled in Phase 5.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::traits::{BlockWriter, ChunkAllocator};
use crate::writer::large_object::{LargeObjectWriter, WriterConfig};
use crate::{IoError, Result};
use crow_common::ec::EcScheme;

/// Pool of large-object writers bounded by a memory budget.
///
/// Each writer's footprint is `per_writer_memory()`. The pool rejects
/// new acquisitions when the budget is exhausted.
pub struct WriterPool<A: ChunkAllocator + Clone, W: BlockWriter + Clone> {
    chunkdb: A,
    diskio: W,
    ec_scheme: EcScheme,
    config: WriterConfig,
    memory_budget: usize,
    in_use: Arc<AtomicUsize>,
}

impl<A: ChunkAllocator + Clone + 'static, W: BlockWriter + Clone + 'static> WriterPool<A, W> {
    /// Create a new pool.
    pub fn new(
        chunkdb: A,
        diskio: W,
        ec_scheme: EcScheme,
        config: WriterConfig,
        memory_budget: usize,
    ) -> Self {
        Self {
            chunkdb,
            diskio,
            ec_scheme,
            config,
            memory_budget,
            in_use: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Per-writer memory footprint.
    fn per_writer_memory(&self) -> usize {
        let block = self.config.read_buffer_size;
        self.config.max_cached_buffer
            + block
            + self.config.parity_depth * self.ec_scheme.total_blocks() * block
    }

    /// Try to acquire a writer. Returns `MemoryBudgetExhausted` if the
    /// budget is full.
    pub fn try_acquire(&self) -> Result<PooledWriter<A, W>> {
        let footprint = self.per_writer_memory();
        let prev = self.in_use.fetch_add(footprint, Ordering::AcqRel);
        if prev + footprint > self.memory_budget {
            self.in_use.fetch_sub(footprint, Ordering::AcqRel);
            return Err(IoError::MemoryBudgetExhausted);
        }
        Ok(PooledWriter {
            writer: LargeObjectWriter::new(
                self.chunkdb.clone(),
                self.diskio.clone(),
                self.ec_scheme,
                self.config.clone(),
            ),
            in_use: self.in_use.clone(),
            footprint,
        })
    }
}

/// A writer acquired from the pool. Releases the budget on drop.
pub struct PooledWriter<A: ChunkAllocator, W: BlockWriter> {
    pub writer: LargeObjectWriter<A, W>,
    in_use: Arc<AtomicUsize>,
    footprint: usize,
}

impl<A: ChunkAllocator, W: BlockWriter> Drop for PooledWriter<A, W> {
    fn drop(&mut self) {
        self.in_use.fetch_sub(self.footprint, Ordering::AcqRel);
    }
}
