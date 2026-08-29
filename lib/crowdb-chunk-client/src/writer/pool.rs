// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `WriterPool` — bounds concurrent writers by memory budget.
//!
//! No generics — holds concrete `Arc<dyn ChunkAllocator>` +
//! `Arc<dyn DiskWriter>` + `Arc<ChunkClientConfig>`. Delegates
//! `per_writer_memory` to `ChunkClientConfig`.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::config::ChunkClientConfig;
use crate::disk_io::DiskWriter;
use crate::traits::ChunkAllocator;
use crate::writer::large_object::LargeObjectWriter;
use crate::{IoError, Result};
use crowdb_common::ec::EcScheme;

/// Pool of large-object writers bounded by a memory budget.
///
/// Each writer's footprint is `per_writer_memory()`. The pool rejects
/// new acquisitions when the budget is exhausted.
pub struct WriterPool {
    allocator: Arc<dyn ChunkAllocator>,
    disk_writer: Arc<dyn DiskWriter>,
    ec_scheme: EcScheme,
    config: Arc<ChunkClientConfig>,
    memory_budget: usize,
    in_use: Arc<AtomicUsize>,
}

impl WriterPool {
    /// Create a new pool.
    pub fn new(
        allocator: Arc<dyn ChunkAllocator>,
        disk_writer: Arc<dyn DiskWriter>,
        ec_scheme: EcScheme,
        config: Arc<ChunkClientConfig>,
        memory_budget: usize,
    ) -> Self {
        Self {
            allocator,
            disk_writer,
            ec_scheme,
            config,
            memory_budget,
            in_use: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Per-writer memory footprint.
    fn per_writer_memory(&self) -> usize {
        self.config.per_writer_memory(&self.ec_scheme)
    }

    /// Try to acquire a writer. Returns `MemoryBudgetExhausted` if the
    /// budget is full.
    pub fn try_acquire(&self) -> Result<PooledWriter> {
        let footprint = self.per_writer_memory();
        let prev = self.in_use.fetch_add(footprint, Ordering::AcqRel);
        if prev + footprint > self.memory_budget {
            self.in_use.fetch_sub(footprint, Ordering::AcqRel);
            return Err(IoError::MemoryBudgetExhausted);
        }
        Ok(PooledWriter {
            writer: LargeObjectWriter::new(
                self.allocator.clone(),
                self.disk_writer.clone(),
                self.ec_scheme,
                self.config.clone(),
            ),
            in_use: self.in_use.clone(),
            footprint,
        })
    }
}

/// A writer acquired from the pool. Releases the budget on drop.
pub struct PooledWriter {
    pub writer: LargeObjectWriter,
    in_use: Arc<AtomicUsize>,
    footprint: usize,
}

impl Drop for PooledWriter {
    fn drop(&mut self) {
        self.in_use.fetch_sub(self.footprint, Ordering::AcqRel);
    }
}
