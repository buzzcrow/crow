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
    #[allow(clippy::unused_async)]
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

    pub async fn read_at(&mut self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        self.file.seek(io::SeekFrom::Start(offset)).await?;
        self.file.read(buf).await
    }

    pub async fn read_exact_at(&mut self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        self.file.seek(io::SeekFrom::Start(offset)).await?;
        self.file.read_exact(buf).await?;
        Ok(())
    }

    pub async fn fdatasync(&self) -> io::Result<()> {
        self.file.sync_data().await
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
    pub fn path(&self) -> &Path {
        &self.path
    }
}
