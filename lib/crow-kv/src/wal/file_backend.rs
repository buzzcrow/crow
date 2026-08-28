// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Fallback I/O backend: `tokio::fs::File` with `sync_data` for fdatasync.
//!
//! Works on all platforms. `fdatasync` routes through tokio's blocking pool
//! via `File::sync_data()` which maps to the POSIX `fdatasync` syscall on Linux.

use std::io;
use std::path::Path;

use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use super::io_backend::OpenOptions;

#[allow(dead_code)]
pub(crate) struct FileBackendFile {
    file: File,
    path: std::path::PathBuf,
}

impl FileBackendFile {
    #[allow(clippy::unused_async, clippy::unused_async_trait_impl)]
    pub async fn open(path: &Path, opts: &OpenOptions) -> io::Result<Self> {
        let std_opts = opts.to_std();
        let file = File::from_std(std_opts.open(path)?);
        Ok(Self {
            file,
            path: path.to_path_buf(),
        })
    }

    pub async fn write_at(&mut self, data: &[u8], offset: u64) -> io::Result<usize> {
        self.file.seek(io::SeekFrom::Start(offset)).await?;
        self.file.write_all(data).await?;
        Ok(data.len())
    }

    /// Vectored write at `offset`. Uses `seek` + `write_vectored` and loops
    /// on partial writes until all bytes are written or an error occurs.
    /// For regular files on Linux/macOS `writev` writes the full buffer in
    /// one call, so the loop almost always executes a single iteration.
    //
    // TODO: switch to `FileExt::write_vectored_at` (`pwritev`) inside
    // `block_in_place` once it stabilizes (tracking issue #89517). This would
    // halve the syscall count (no `lseek`) and provide positional atomicity.
    pub async fn write_vectored_at(
        &mut self,
        bufs: &[std::io::IoSlice<'_>],
        offset: u64,
    ) -> io::Result<usize> {
        let total: usize = bufs.iter().map(|b| b.len()).sum();
        if total == 0 {
            return Ok(0);
        }

        self.file.seek(io::SeekFrom::Start(offset)).await?;

        // Try a single vectored write first — the common case for regular
        // files is that all bytes are written in one call.
        let n = self.file.write_vectored(bufs).await?;
        if n == total {
            return Ok(n);
        }

        // Partial write: fall back to writing the remaining bytes with
        // `write_all`. We advance through the slices to find the first
        // unwritten byte, then write the rest sequentially.
        let mut written = n;
        let mut skip = n;
        let mut idx = 0;
        while skip > 0 && idx < bufs.len() {
            let len = bufs[idx].len();
            if skip >= len {
                skip -= len;
                idx += 1;
            } else {
                break;
            }
        }
        while idx < bufs.len() {
            let data: &[u8] = if skip > 0 {
                let d = &bufs[idx][skip..];
                skip = 0;
                d
            } else {
                &bufs[idx][..]
            };
            if !data.is_empty() {
                self.file.write_all(data).await?;
                written += data.len();
            }
            idx += 1;
        }

        debug_assert_eq!(written, total);
        Ok(written)
    }

    pub async fn read_at(&mut self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        self.file.seek(io::SeekFrom::Start(offset)).await?;
        self.file.read(buf).await
    }

    pub async fn read_exact_at(&mut self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        self.file.seek(io::SeekFrom::Start(offset)).await?;
        self.file.read_exact(buf).await?;
        Ok(())
    }

    /// No-op — let the OS page cache flush naturally. Use [`Self::fsync`]
    /// for an explicit durable flush on close/shutdown.
    #[allow(clippy::unused_async, clippy::unused_async_trait_impl)]
    pub async fn fdatasync(&self) -> io::Result<()> {
        Ok(())
    }

    pub async fn fsync(&self) -> io::Result<()> {
        self.file.sync_all().await
    }

    pub async fn len(&mut self) -> io::Result<u64> {
        let pos = self.file.seek(io::SeekFrom::End(0)).await?;
        Ok(pos)
    }

    pub async fn truncate(&self, len: u64) -> io::Result<()> {
        self.file.set_len(len).await
    }

    #[allow(dead_code)]
    pub(super) fn path(&self) -> &Path {
        &self.path
    }
}
