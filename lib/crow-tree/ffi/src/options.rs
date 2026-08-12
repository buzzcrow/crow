// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

/// Compression selector.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    None,
    Lz4,
}

/// Durable backend selection, mirrors `ct_options::backend`.
/// Ignored when `Options::path` is `None` (in-memory).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PageStoreBackend {
    /// File-based page store, no alignment.
    #[default]
    File,
    /// Block device: 4K aligned, `O_DIRECT` for a real SSD/SCM
    /// deployment target.
    Block,
    /// Mem block device: in-memory, no alignment.
    MemBlock,
}

/// Durability barrier policy, mirrors `ct_sync_mode`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SyncMode {
    /// fdatasync after every flush (default, production).
    #[default]
    Full,
    /// No fsync (tests/CI only).
    Skip,
    /// fsync once per snapshot commit.
    Batch,
}

impl SyncMode {
    pub(crate) fn as_u8(self) -> u8 {
        match self {
            Self::Full => 0,
            Self::Skip => 1,
            Self::Batch => 2,
        }
    }
}

/// Engine configuration. `path = None` selects an in-memory store.
#[derive(Debug, Clone, Default)]
pub struct Options {
    pub path: Option<String>,
    pub iu_size: u32,
    pub frame_bytes: u32,
    pub buffer_pool_bytes: u64,
    pub compression_lz4: bool,
    pub max_inline_value: u64,
    pub backend: PageStoreBackend,
    /// Block size for array-of-blocks mode (0 = default 64 MiB).
    pub block_size: u64,
    /// Store ID for block file naming.
    pub store_id: u32,
    /// Group ID, maps to PxGroupId in CrowKV.
    pub group_id: u32,
    /// Durability barrier policy.
    pub sync_mode: SyncMode,
    /// C++ engine log directory (empty = no file logging).
    pub log_dir: String,
    /// spdlog level name ("info", "debug", etc.).
    pub log_level: String,
    /// C++ log filename prefix (empty = "crow-tree").
    pub log_file_prefix: String,
    /// Max C++ log file size in MiB before rotation.
    pub log_max_file_mb: usize,
    /// Number of rotated C++ log files to keep.
    pub log_max_files: usize,
}
