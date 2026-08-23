// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! `LocalFileDiskWriter` — test-only `DiskWriter` impl that writes
//! blocks to per-disk files under a temp dir. Enables UT-level
//! write-flow tests without the `crow-test-harness` diskio/diskdb
//! harness — fast, hermetic, no FFI.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Mutex;

use async_trait::async_trait;
use bytes::Bytes;
use crow_diskio_client::DiskId;
use crow_protocol::diskdb::rpc::Segment;
use tokio::fs::File;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};

use crate::disk_io::DiskWriter;
use crate::{IoError, Result};

/// Test-only `DiskWriter` writing to per-disk files.
pub struct LocalFileDiskWriter {
    root: PathBuf,
    /// Cache of file paths by disk_id — we re-open for each write to
    /// avoid holding a non-Send MutexGuard across await points.
    paths: Mutex<HashMap<(u64, u64), PathBuf>>,
}

impl LocalFileDiskWriter {
    /// Construct a new writer rooted at `root`. Each `DiskId` maps to
    /// a file `root/<high>_<low>.dat`.
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            paths: Mutex::new(HashMap::new()),
        }
    }

    async fn get_path(&self, disk_id: DiskId) -> Result<PathBuf> {
        let key = (disk_id.high, disk_id.low);
        let path = {
            let mut paths = self.paths.lock().unwrap();
            paths
                .entry(key)
                .or_insert_with(|| self.root.join(format!("{}_{}.dat", disk_id.high, disk_id.low)))
                .clone()
        };
        // Ensure dir exists.
        tokio::fs::create_dir_all(&self.root)
            .await
            .map_err(|e| IoError::WriteFailed(format!("mkdir failed: {e}")))?;
        Ok(path)
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

        let path = self.get_path(id).await?;
        let mut file = File::create(&path)
            .await
            .map_err(|e| IoError::WriteFailed(format!("create file failed: {e}")))?;
        file.seek(std::io::SeekFrom::Start(zone_offset))
            .await
            .map_err(|e| IoError::WriteFailed(format!("seek failed: {e}")))?;
        file.write_all(&data)
            .await
            .map_err(|e| IoError::WriteFailed(format!("write failed: {e}")))?;
        Ok(())
    }

    async fn fsync(&self, disk_id: DiskId) -> Result<()> {
        let path = self.get_path(disk_id).await?;
        let file = File::open(&path)
            .await
            .map_err(|e| IoError::WriteFailed(format!("open file failed: {e}")))?;
        file.sync_all()
            .await
            .map_err(|e| IoError::WriteFailed(format!("fsync failed: {e}")))?;
        Ok(())
    }
}
