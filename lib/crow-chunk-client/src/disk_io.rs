// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Block-IO layer — the `DiskWriter` seam.
//!
//! `DiskWriter` is the single test-injection point for block IO.
//! Production impl (`DiskioBlockWriter`) wraps `DiskioClient`; test
//! impl (`LocalFileDiskWriter`, behind `test-util`) writes to local
//! files. Replaces the old `BlockWriter` trait.

pub mod disk_writer;
#[cfg(feature = "test-util")]
pub mod local_file;

pub use disk_writer::{DiskWriter, DiskioBlockWriter};
#[cfg(feature = "test-util")]
pub use local_file::LocalFileDiskWriter;
