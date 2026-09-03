// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! I/O backend selection and capability probe.
//!
//! The backend is chosen once at startup and fixed for the process lifetime.
//! Production: `Fallback` (`tokio::fs` + `spawn_blocking`).
//! Tests: `Sim` (in-memory, deterministic, failure-injectable).
//! Future: `Uring` on Linux >= 5.11 (deferred to V2).

use std::path::{Path, PathBuf};
use std::{fmt, io};

use tracing::info;

use super::block_backend;
use super::file_backend;

/// Chosen I/O backend for the process lifetime.
///
/// Callers never branch on this; they call [`IoBackend::open`] and get an
/// [`super::WalFile`] that dispatches internally.
pub enum IoBackend {
    /// `tokio::fs` + `spawn_blocking` for fdatasync. Works everywhere.
    File,
    /// In-memory block device (test harness, error/corruption injection).
    MemBlock(block_backend::MemBlockDevice),
    /// Real file-backed block device (aligned I/O, configurable `O_DIRECT`).
    BlockDevice(block_backend::BlockDevice),
}

impl fmt::Debug for IoBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::File => write!(f, "File"),
            Self::MemBlock(_) => write!(f, "MemBlock"),
            Self::BlockDevice(_) => write!(f, "BlockDevice"),
        }
    }
}

/// Open options mirroring the subset of `std::fs::OpenOptions` we need.
#[derive(Clone, Debug, Default)]
#[allow(clippy::struct_excessive_bools)]
pub struct OpenOptions {
    pub read: bool,
    pub write: bool,
    pub create: bool,
    pub create_new: bool,
    pub truncate: bool,
    pub append: bool,
}

impl OpenOptions {
    #[must_use]
    pub fn read_only() -> Self {
        Self {
            read: true,
            ..Default::default()
        }
    }
    #[must_use]
    pub(crate) fn read_write() -> Self {
        Self {
            read: true,
            write: true,
            ..Default::default()
        }
    }
    #[must_use]
    pub fn create_rw() -> Self {
        Self {
            read: true,
            write: true,
            create: true,
            ..Default::default()
        }
    }
    #[must_use]
    pub fn create_new_rw() -> Self {
        Self {
            read: true,
            write: true,
            create: true,
            create_new: true,
            ..Default::default()
        }
    }
    pub(crate) fn to_std(&self) -> std::fs::OpenOptions {
        let mut o = std::fs::OpenOptions::new();
        o.read(self.read)
            .write(self.write)
            .create(self.create)
            .truncate(self.truncate)
            .append(self.append);
        if self.create_new {
            o.create_new(true);
        }
        o
    }
}

impl IoBackend {
    /// Select backend via capability probe. Logs the choice at INFO.
    ///
    /// For V1 this always returns `Fallback`. `io_uring` probe deferred to V2.
    #[must_use]
    pub fn detect() -> Self {
        info!(
            backend = "fallback",
            "io backend selected (io_uring deferred to V2)"
        );
        Self::File
    }

    /// In-memory block device (unaligned, test harness with error injection).
    #[must_use]
    pub fn mem_block() -> Self {
        Self::MemBlock(block_backend::MemBlockDevice::new())
    }

    /// Real file-backed block device with `O_DIRECT` (SSD/NVMe, 4K aligned).
    #[must_use]
    pub fn block_device() -> Self {
        Self::BlockDevice(block_backend::BlockDevice::ssd())
    }

    /// Returns `true` if the backend is `BlockDevice` or `MemBlock`
    /// (i.e. has block device counters worth exposing).
    #[must_use]
    pub fn is_block_device(&self) -> bool {
        matches!(self, Self::BlockDevice(_) | Self::MemBlock(_))
    }

    /// Real file-backed block device with buffered I/O (aligned, no `O_DIRECT`).
    /// Ideal for benchmarks that exercise block code paths without disk-bound TPS.
    #[must_use]
    #[allow(dead_code)]
    pub(crate) fn block_buffered() -> Self {
        Self::BlockDevice(block_backend::BlockDevice::new())
    }

    /// Open (or create) a file via the selected backend.
    ///
    /// # Errors
    /// Returns IO error if the file cannot be opened or created.
    pub async fn open(&self, path: impl AsRef<Path>, opts: OpenOptions) -> io::Result<super::WalFile> {
        match self {
            Self::File => {
                let f = file_backend::FileBackendFile::open(path.as_ref(), &opts).await?;
                Ok(super::WalFile {
                    inner: super::WalFileInner::File(f),
                })
            }
            Self::MemBlock(disk) => {
                let f = disk.open_segment(path.as_ref(), &opts)?;
                Ok(super::WalFile {
                    inner: super::WalFileInner::MemBlock(f),
                })
            }
            Self::BlockDevice(disk) => {
                let f = disk.open_segment(path.as_ref(), &opts)?;
                Ok(super::WalFile {
                    inner: super::WalFileInner::Block(f),
                })
            }
        }
    }

    /// Rename a file atomically.
    ///
    /// # Errors
    /// Returns IO error if the rename fails.
    pub async fn rename(&self, from: impl AsRef<Path>, to: impl AsRef<Path>) -> io::Result<()> {
        match self {
            Self::File => tokio::fs::rename(from, to).await,
            Self::MemBlock(disk) => disk.rename_segment(from.as_ref(), to.as_ref()),
            Self::BlockDevice(disk) => disk.rename_segment(from.as_ref(), to.as_ref()),
        }
    }

    /// Remove a file.
    ///
    /// # Errors
    /// Returns IO error if the file cannot be removed.
    pub async fn unlink(&self, path: impl AsRef<Path>) -> io::Result<()> {
        match self {
            Self::File => tokio::fs::remove_file(path).await,
            Self::MemBlock(disk) => disk.unlink_segment(path.as_ref()),
            Self::BlockDevice(disk) => disk.unlink_segment(path.as_ref()),
        }
    }

    /// List entries in a directory.
    ///
    /// # Errors
    /// Returns IO error if the directory cannot be read.
    pub async fn read_dir(&self, path: impl AsRef<Path>) -> io::Result<Vec<PathBuf>> {
        match self {
            Self::File => {
                let mut entries = Vec::new();
                let mut rd = tokio::fs::read_dir(path).await?;
                while let Some(e) = rd.next_entry().await? {
                    entries.push(e.path());
                }
                Ok(entries)
            }
            Self::MemBlock(disk) => disk.list_layout(path.as_ref()),
            Self::BlockDevice(disk) => disk.list_layout(path.as_ref()),
        }
    }

    /// Create directory and all parents.
    ///
    /// # Errors
    /// Returns IO error if the directory cannot be created.
    pub async fn create_dir_all(&self, path: impl AsRef<Path>) -> io::Result<()> {
        match self {
            Self::File => tokio::fs::create_dir_all(path).await,
            Self::MemBlock(disk) => disk.create_layout(path.as_ref()),
            Self::BlockDevice(disk) => disk.create_layout(path.as_ref()),
        }
    }

    /// Check if a path exists.
    pub async fn exists(&self, path: impl AsRef<Path>) -> bool {
        match self {
            Self::File => tokio::fs::try_exists(path).await.unwrap_or(false),
            Self::MemBlock(disk) => disk.contains_path(path.as_ref()),
            Self::BlockDevice(disk) => disk.contains_path(path.as_ref()),
        }
    }

    /// Remove a directory and all contents recursively. For `File` this is
    /// `tokio::fs::remove_dir_all`. For `MemBlock` it clears all in-memory
    /// segments + layouts under the prefix — without this, the mem-block
    /// backend retains old WAL segments across `wipe_user_data` calls,
    /// causing `create_group_with_wal` to replay stale records and stall
    /// for seconds.
    ///
    /// # Errors
    /// Returns `io::Error` from `tokio::fs::remove_dir_all` (`File` /
    /// `BlockDevice`) or from the in-memory prefix clear (`MemBlock`).
    pub async fn remove_dir_all(&self, path: impl AsRef<Path>) -> io::Result<()> {
        match self {
            Self::MemBlock(disk) => disk.remove_prefix(path.as_ref()),
            Self::File | Self::BlockDevice(_) => tokio::fs::remove_dir_all(path).await,
        }
    }
}
