// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Shared test support — `LocalFileDiskWriter` (real file I/O) +
//! read-back helpers for write-then-read verification.

use std::collections::HashMap;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use crow_chunk_client::{DiskWriter, IoError, Result};
use crow_diskio_client::DiskId;
use crow_protocol::diskdb::rpc::Segment;

/// Test-only `DiskWriter` that writes blocks to per-disk files under a
/// temp dir. Tracks write + fsync counts for assertions. Data can be
/// read back via `read_block` for write-then-read verification.
#[derive(Clone)]
pub struct LocalFileDiskWriter {
    root: PathBuf,
    paths: Arc<Mutex<HashMap<(u64, u64), PathBuf>>>,
    write_count: Arc<AtomicUsize>,
    fsync_count: Arc<AtomicUsize>,
}

#[allow(dead_code)] // methods may be unused in some test binaries
impl LocalFileDiskWriter {
    /// Construct a new writer rooted at `root`.
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        let root = root.into();
        // Eagerly create the root directory.
        std::fs::create_dir_all(&root).ok();
        Self {
            root,
            paths: Arc::new(Mutex::new(HashMap::new())),
            write_count: Arc::new(AtomicUsize::new(0)),
            fsync_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Total number of `write` calls.
    pub fn write_count(&self) -> usize {
        self.write_count.load(Ordering::Relaxed)
    }

    /// Total number of `fsync` calls.
    pub fn fsync_count(&self) -> usize {
        self.fsync_count.load(Ordering::Relaxed)
    }

    fn file_path(&self, disk_id: DiskId) -> PathBuf {
        let key = (disk_id.high, disk_id.low);
        let mut paths = self.paths.lock().unwrap();
        paths
            .entry(key)
            .or_insert_with(|| self.root.join(format!("{}_{}.dat", disk_id.high, disk_id.low)))
            .clone()
    }

    /// Read back a block from disk: seek to `zone_offset`, read
    /// `len` bytes.
    pub fn read_block(&self, disk_id: DiskId, zone_offset: u64, len: usize) -> Result<Vec<u8>> {
        let path = self.file_path(disk_id);
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .open(&path)
            .map_err(|e| IoError::WriteFailed(format!("open file failed: {e}")))?;
        file.seek(SeekFrom::Start(zone_offset))
            .map_err(|e| IoError::WriteFailed(format!("seek failed: {e}")))?;
        let mut buf = vec![0u8; len];
        file.read_exact(&mut buf)
            .map_err(|e| IoError::WriteFailed(format!("read failed: {e}")))?;
        Ok(buf)
    }
}

#[async_trait]
impl DiskWriter for LocalFileDiskWriter {
    async fn write(&self, seg: &Segment, unit_bytes: u64, data: Bytes) -> Result<()> {
        let disk_id = seg
            .disk_id
            .as_ref()
            .ok_or_else(|| IoError::WriteFailed("segment missing disk_id".into()))?;
        let id = DiskId::new(disk_id.high, disk_id.low);
        let zone_offset = seg.unit_offset * unit_bytes;
        let path = self.file_path(id);
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&path)
            .map_err(|e| IoError::WriteFailed(format!("open file failed: {e}")))?;
        file.seek(SeekFrom::Start(zone_offset))
            .map_err(|e| IoError::WriteFailed(format!("seek failed: {e}")))?;
        file.write_all(&data)
            .map_err(|e| IoError::WriteFailed(format!("write failed: {e}")))?;
        self.write_count.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    async fn fsync(&self, disk_id: DiskId) -> Result<()> {
        let path = self.file_path(disk_id);
        // Skip fsync if the file doesn't exist (partial strip — not
        // all disks in the placement were written to).
        if !path.exists() {
            return Ok(());
        }
        let file = std::fs::OpenOptions::new()
            .read(true)
            .open(&path)
            .map_err(|e| IoError::WriteFailed(format!("open file failed: {e}")))?;
        file.sync_all()
            .map_err(|e| IoError::WriteFailed(format!("fsync failed: {e}")))?;
        self.fsync_count.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}
