// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#![allow(clippy::unused_async)]

//! `MirrorStripWriter` — placeholder stub for mirror strips.
//!
//! Declared so the `StripWriter` enum shape is fixed. The large-write
//! flow never constructs it. Filled in by R93 (mirror-to-EC
//! conversion) and R106. Mirror strips have no parity, so a
//! `MirrorStripWriter` owns no `EcWorker`.

use bytes::Bytes;

use crate::chunk::strip::StripResult;
use crate::io::FeedStatus;
use crate::{IoError, Result};

/// Mirror strip writer — placeholder. Methods return
/// `IoError::Internal` until R93/R106 fills in the impl.
pub struct MirrorStripWriter {
    _placeholder: (),
}

impl MirrorStripWriter {
    /// Construct a new mirror strip writer (placeholder).
    #[must_use]
    pub fn new() -> Self {
        Self { _placeholder: () }
    }

    /// Push a data block to the strip.
    pub async fn push(&mut self, _buffer: Bytes) -> Result<FeedStatus> {
        Err(IoError::Internal("MirrorStripWriter not yet implemented".into()))
    }

    /// End of strip: return the strip result.
    pub async fn finish(&mut self) -> Result<StripResult> {
        Err(IoError::Internal("MirrorStripWriter not yet implemented".into()))
    }

    /// Abort: return already-durable state.
    pub async fn abort(&mut self) -> Result<StripResult> {
        Err(IoError::Internal("MirrorStripWriter not yet implemented".into()))
    }

    /// Non-async capacity hint.
    pub fn ready(&self) -> bool {
        false
    }

    /// True if the strip has any data blocks written.
    pub fn has_data(&self) -> bool {
        false
    }
}

impl Default for MirrorStripWriter {
    fn default() -> Self {
        Self::new()
    }
}
