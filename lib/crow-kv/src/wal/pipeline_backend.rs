// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

use std::path::PathBuf;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WalBlockAlignment {
    /// No alignment requirement. Writes may target arbitrary byte offsets.
    Unaligned,
    /// Backend requires I/O aligned to the specified unit size.
    Aligned { io_unit_bytes: usize },
}

impl WalBlockAlignment {
    /// Default I/O unit size for a typical SSD/NVMe (4 KiB). Used by
    /// `default_aligned()` and as the `WalConfig` default for
    /// `wal_io_unit_bytes`. Actual deployments may override this to match
    /// the device's logical/physical sector size (512, 8192, 16384, etc.).
    pub const DEFAULT_IO_UNIT_BYTES: usize = 4 * 1024;

    #[must_use]
    pub const fn default_aligned() -> Self {
        Self::Aligned {
            io_unit_bytes: Self::DEFAULT_IO_UNIT_BYTES,
        }
    }

    #[must_use]
    #[allow(dead_code)]
    pub(crate) const fn io_unit_bytes(self) -> Option<usize> {
        match self {
            Self::Unaligned => None,
            Self::Aligned { io_unit_bytes } => Some(io_unit_bytes),
        }
    }

    /// Whether `offset`/`len` already satisfy this alignment requirement.
    #[must_use]
    #[allow(dead_code)]
    pub(crate) const fn is_aligned(self, offset: u64, len: usize) -> bool {
        match self {
            Self::Unaligned => true,
            Self::Aligned { io_unit_bytes } => {
                let unit = io_unit_bytes as u64;
                offset % unit == 0 && (len as u64) % unit == 0
            }
        }
    }

    /// Plan how a logical write at `offset` of `len` bytes maps onto physical
    /// block I/O for this alignment mode.
    ///
    /// For [`Self::Unaligned`] backends (RAM / SCM / PMEM) the write maps 1:1.
    /// For [`Self::Aligned`] backends (SSD / `NVMe` under `O_DIRECT`) the write is
    /// widened to the enclosing block-aligned range; a partial block implies a
    /// read-modify-write and therefore write amplification.
    ///
    /// # Panics
    /// Panics if the configured I/O unit or computed aligned write length does
    /// not fit into the target integer types on the current platform.
    #[must_use]
    pub(crate) fn plan_write(self, offset: u64, len: usize) -> WalBlockWritePlan {
        match self {
            Self::Unaligned => WalBlockWritePlan {
                aligned_offset: offset,
                aligned_len: len,
                payload_offset_within_aligned: 0,
                requires_read_modify_write: false,
            },
            Self::Aligned { io_unit_bytes } => {
                let io_unit = u64::try_from(io_unit_bytes).expect("io_unit_bytes exceeds u64");
                assert!(io_unit > 0, "io_unit_bytes must be non-zero");
                let len_u64 = u64::try_from(len).expect("len exceeds u64");
                let write_end = offset + len_u64;
                let aligned_offset = (offset / io_unit) * io_unit;
                let aligned_end = write_end.div_ceil(io_unit) * io_unit;
                let aligned_len_u64 = aligned_end - aligned_offset;
                WalBlockWritePlan {
                    aligned_offset,
                    aligned_len: usize::try_from(aligned_len_u64).expect("aligned_len exceeds usize"),
                    payload_offset_within_aligned: usize::try_from(offset - aligned_offset)
                        .expect("payload offset exceeds usize"),
                    requires_read_modify_write: offset != aligned_offset || write_end != aligned_end,
                }
            }
        }
    }
}

/// Placement/configuration for a file-backed WAL pipeline.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WalFilePipelineBackend {
    pub root_path: PathBuf,
}

/// Placement/configuration for a memory-backed WAL pipeline.
///
/// `MemBlock` behaves like a raw byte-addressable memory block with no
/// alignment requirement.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct WalMemBlockBackend;

/// Placement/configuration for a block-device WAL pipeline.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WalBlockPipelineBackend {
    pub device_name: String,
    pub alignment: WalBlockAlignment,
}

impl WalBlockPipelineBackend {
    #[must_use]
    pub fn new(device_name: impl Into<String>, alignment: WalBlockAlignment) -> Self {
        Self {
            device_name: device_name.into(),
            alignment,
        }
    }

    /// Plan how a write maps onto block I/O for this backend's alignment mode.
    ///
    /// Delegates to [`WalBlockAlignment::plan_write`].
    #[must_use]
    pub fn plan_write(&self, offset: u64, len: usize) -> WalBlockWritePlan {
        self.alignment.plan_write(offset, len)
    }
}

/// Concrete backend model attached to a WAL pipeline.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WalPipelineBackend {
    File(WalFilePipelineBackend),
    MemBlock(WalMemBlockBackend),
    Block(WalBlockPipelineBackend),
}

impl WalPipelineBackend {
    #[must_use]
    pub(crate) fn file(root_path: PathBuf) -> Self {
        Self::File(WalFilePipelineBackend { root_path })
    }

    #[must_use]
    pub const fn mem_block() -> Self {
        Self::MemBlock(WalMemBlockBackend)
    }

    #[must_use]
    pub(crate) fn block(device_name: impl Into<String>, alignment: WalBlockAlignment) -> Self {
        Self::Block(WalBlockPipelineBackend::new(device_name, alignment))
    }

    /// Physical alignment this backend imposes on WAL writes. The file and
    /// memory-block backends are byte-addressable (`Unaligned`); a block
    /// backend reports its configured device alignment.
    #[must_use]
    pub const fn alignment(&self) -> WalBlockAlignment {
        match self {
            Self::File(_) | Self::MemBlock(_) => WalBlockAlignment::Unaligned,
            Self::Block(b) => b.alignment,
        }
    }
}

/// Computed plan for one block-device write.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WalBlockWritePlan {
    pub aligned_offset: u64,
    pub aligned_len: usize,
    pub payload_offset_within_aligned: usize,
    pub requires_read_modify_write: bool,
}
