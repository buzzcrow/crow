// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Safe Rust adapter over the crowdb-tree C ABI (`c_api.h`, PT8).
//!
//! Wraps the opaque `ct_*` handles in RAII types, translates owned `ct_buf`
//! buffers into `Vec<u8>` (freeing them via `ct_free_buf`), maps `ct_status`
//! into `Result`, and offers an async facade (`AsyncCrowdbtree`). `get`/`flush`/
//! `snapshot`/`scan` drive the engine's io_uring reactor directly (no OS
//! thread hop, Phase 3); the remaining methods (no async C API twin exists
//! for them yet) are called via the synchronous `Crowdbtree` handle.

pub mod async_tree;
pub mod batch;
pub mod cpp_global_metrics;
pub mod crc;
pub mod error;
pub mod options;
pub mod reactor;
pub mod scan;
pub mod snapshot;
pub mod stats;
pub(crate) mod sys;
pub mod tree;
pub mod write_handle;

pub use async_tree::{AsyncCrowdbtree, GetOutcome, PinnedGetOutcome, ScanOutcome};
pub use batch::{BatchOp, ExtOp};
pub use cpp_global_metrics::{cpp_global_metrics_max_name_len, flush_cpp_global_metrics};
pub use crc::{crc32c, crc32c_update};
pub use error::CtError;
pub use options::{Compression, Options, PageStoreBackend, SyncMode};
pub use reactor::PinnedValue;
pub use scan::{ScanEntry, ViewEntry};
pub use stats::{MergeGcStats, Stats};
pub use tree::{
    ct_add_log_stderr, ct_flush_logging, ct_init_logging, ct_init_test_logging, ct_shutdown_logging,
    Crowdbtree,
};
pub use write_handle::WriteHandle;
