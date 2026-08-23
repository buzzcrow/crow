// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! `SmallObjectWriter` — placeholder stub (R106).
//!
//! Small objects append to an active chunk (no `ChunkPrefetch`).
//! Filled in by R106. Methods return `IoError::Internal` until then.

use std::sync::Arc;

use bytes::Bytes;

use crate::config::ChunkClientConfig;
use crate::disk_io::DiskWriter;
use crate::io::{ChunkIoWriter, FeedStatus};
use crate::traits::ChunkAllocator;
use crate::{IoError, Location, Result};
use crow_common::ec::EcScheme;

/// Small-object writer — placeholder (R106).
pub struct SmallObjectWriter {
    _allocator: Arc<dyn ChunkAllocator>,
    _disk_writer: Arc<dyn DiskWriter>,
    _ec_scheme: EcScheme,
    _config: Arc<ChunkClientConfig>,
}

impl SmallObjectWriter {
    /// Construct a new small-object writer (placeholder).
    pub fn new(
        allocator: Arc<dyn ChunkAllocator>,
        disk_writer: Arc<dyn DiskWriter>,
        ec_scheme: EcScheme,
        config: Arc<ChunkClientConfig>,
    ) -> Self {
        Self {
            _allocator: allocator,
            _disk_writer: disk_writer,
            _ec_scheme: ec_scheme,
            _config: config,
        }
    }
}

#[async_trait::async_trait]
impl ChunkIoWriter for SmallObjectWriter {
    async fn on_data(&mut self, _buffer: Bytes) -> Result<FeedStatus> {
        Err(IoError::Internal("SmallObjectWriter not yet implemented".into()))
    }

    async fn on_finish(&mut self) -> Result<Vec<Location>> {
        Err(IoError::Internal("SmallObjectWriter not yet implemented".into()))
    }

    async fn on_error(&mut self) -> Result<Vec<Location>> {
        Err(IoError::Internal("SmallObjectWriter not yet implemented".into()))
    }

    fn require_data(&self) -> bool {
        false
    }
}
