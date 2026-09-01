// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;

use super::io_backend::OpenOptions;
use super::pipeline_backend::WalBlockAlignment;

#[derive(Clone, Default)]
pub struct BlockDeviceController {
    inner: Arc<BlockDeviceControllerInner>,
}

#[derive(Default)]
struct BlockDeviceControllerInner {
    full: AtomicBool,
    io_error: AtomicBool,
    sync_error: AtomicBool,
    corrupt_requests: Mutex<Vec<(PathBuf, u64)>>,
}

impl BlockDeviceController {
    pub fn set_full(&self, full: bool) {
        self.inner.full.store(full, Ordering::Release);
    }

    pub fn inject_io_error(&self, on: bool) {
        self.inner.io_error.store(on, Ordering::Release);
    }

    /// Inject a durable-flush-only failure: `fdatasync` / `fsync` error while
    /// `write_at` keeps succeeding. Models a media that accepts buffered writes
    /// but cannot persist them, exercising the flush worker's error path (W3).
    pub fn inject_sync_error(&self, on: bool) {
        self.inner.sync_error.store(on, Ordering::Release);
    }

    fn check_sync(&self) -> io::Result<()> {
        if self.inner.sync_error.load(Ordering::Acquire) {
            return Err(io::Error::other("BlockDevice: injected durable-flush failure"));
        }
        Ok(())
    }

    pub fn corrupt_at_offset(&self, segment: impl AsRef<Path>, offset: u64) {
        self.inner
            .corrupt_requests
            .lock()
            .push((segment.as_ref().to_path_buf(), offset));
    }

    fn check_io(&self) -> io::Result<()> {
        if self.inner.io_error.load(Ordering::Acquire) {
            return Err(io::Error::other("BlockDevice: injected EIO"));
        }
        Ok(())
    }

    fn check_write(&self) -> io::Result<()> {
        self.check_io()?;
        if self.inner.full.load(Ordering::Acquire) {
            return Err(io::Error::other("BlockDevice: ENOSPC (device full)"));
        }
        Ok(())
    }

    fn apply_corruptions(&self, segments: &Mutex<BTreeMap<PathBuf, Vec<u8>>>) {
        let mut requests = self.inner.corrupt_requests.lock();
        let mut stored_segments = segments.lock();
        for (segment, offset) in requests.drain(..) {
            if let Some(data) = stored_segments.get_mut(&segment) {
                let off = usize::try_from(offset).expect("offset exceeds usize");
                if off < data.len() {
                    data[off] ^= 0xFF;
                }
            }
        }
    }
}

/// Real file-backed block device. Always opens OS paths and does positional
/// I/O (`pwrite`/`pread`) via `FileExt`. Whether the path is a regular file,
/// a raw block device, or a tmpfs file makes no difference — in Unix, a
/// block device *is* a file.
///
/// Alignment and `O_DIRECT` are independently configurable:
/// - [`BlockDevice::new`] — aligned (4K), buffered (no `O_DIRECT`). Default
///   for benchmarks and general use.
/// - [`BlockDevice::ssd`] — aligned (4K), `O_DIRECT`. Models a real SSD/`NVMe`
///   with direct I/O bypassing the page cache.
#[derive(Clone)]
pub struct BlockDevice {
    write_count: Arc<AtomicU64>,
    fdatasync_count: Arc<AtomicU64>,
    alignment: WalBlockAlignment,
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    use_direct_io: bool,
    rmw_count: Arc<AtomicU64>,
}

impl BlockDevice {
    /// Create an aligned block device using real OS files with buffered I/O
    /// (no `O_DIRECT`). All alignment/RMW logic runs, `fdatasync` performs a
    /// real `sync_data` syscall, but the page cache absorbs writes. Ideal
    /// for benchmarks that exercise block code paths without disk-bound TPS.
    #[must_use]
    pub fn new() -> Self {
        tracing::info!("BlockDevice::new — real OS files, buffered, 4K aligned");
        Self::with_alignment(WalBlockAlignment::default_aligned(), false)
    }

    /// Create an aligned block device using real OS files with `O_DIRECT`
    /// (Linux only). `fdatasync` performs a real `sync_data` syscall. Use
    /// this for production SSD/`NVMe` modelling.
    #[must_use]
    pub fn ssd() -> Self {
        tracing::info!("BlockDevice::ssd — real OS files, O_DIRECT, 4K aligned, fdatasync = sync_data");
        Self::with_alignment(WalBlockAlignment::default_aligned(), true)
    }

    /// Create a block device with an explicit alignment mode and `O_DIRECT` flag.
    #[must_use]
    pub fn with_alignment(alignment: WalBlockAlignment, use_direct_io: bool) -> Self {
        Self {
            write_count: Arc::new(AtomicU64::new(0)),
            fdatasync_count: Arc::new(AtomicU64::new(0)),
            alignment,
            use_direct_io,
            rmw_count: Arc::new(AtomicU64::new(0)),
        }
    }

    #[must_use]
    pub fn alignment(&self) -> WalBlockAlignment {
        self.alignment
    }

    #[must_use]
    pub fn write_count(&self) -> u64 {
        self.write_count.load(Ordering::Acquire)
    }

    #[must_use]
    pub fn fdatasync_count(&self) -> u64 {
        self.fdatasync_count.load(Ordering::Acquire)
    }

    /// Number of writes that triggered a read-modify-write of a partial block.
    #[must_use]
    pub fn rmw_count(&self) -> u64 {
        self.rmw_count.load(Ordering::Acquire)
    }

    pub(crate) fn open_segment(&self, segment_path: &Path, opts: &OpenOptions) -> io::Result<BlockSegment> {
        let segment_path = segment_path.to_path_buf();
        let mut std_opts = std::fs::OpenOptions::new();
        std_opts.read(true).write(true);
        if opts.create {
            std_opts.create(true);
        }
        if opts.create_new {
            std_opts.create_new(true);
        }
        if opts.truncate {
            std_opts.truncate(true);
        }
        #[cfg(target_os = "linux")]
        if self.use_direct_io {
            use std::os::unix::fs::OpenOptionsExt;
            std_opts.custom_flags(0o40000);
        }
        let file = std_opts.open(&segment_path)?;
        Ok(BlockSegment {
            device: self.clone(),
            file,
        })
    }

    #[allow(clippy::unused_self)]
    pub(super) fn rename_segment(&self, from: &Path, to: &Path) -> io::Result<()> {
        std::fs::rename(from, to)
    }

    #[allow(clippy::unused_self)]
    pub(super) fn unlink_segment(&self, segment_path: &Path) -> io::Result<()> {
        std::fs::remove_file(segment_path)
    }

    #[allow(clippy::unused_self)]
    pub(super) fn list_layout(&self, layout_path: &Path) -> io::Result<Vec<PathBuf>> {
        let mut entries = Vec::new();
        let rd = std::fs::read_dir(layout_path)?;
        for entry in rd.flatten() {
            entries.push(entry.path());
        }
        Ok(entries)
    }

    #[allow(clippy::unused_self)]
    pub(super) fn create_layout(&self, layout_path: &Path) -> io::Result<()> {
        std::fs::create_dir_all(layout_path)
    }

    #[allow(clippy::unused_self)]
    pub(super) fn contains_path(&self, path: &Path) -> bool {
        path.exists()
    }
}

impl Default for BlockDevice {
    fn default() -> Self {
        Self::new()
    }
}

pub(crate) struct BlockSegment {
    device: BlockDevice,
    file: std::fs::File,
}

impl BlockSegment {
    pub(super) fn write_at(&self, data: &[u8], offset: u64) -> io::Result<usize> {
        self.write_bytes_to_file(&self.file, data, offset)?;
        self.device.write_count.fetch_add(1, Ordering::AcqRel);
        Ok(data.len())
    }

    pub(super) fn write_vectored_at(&self, bufs: &[std::io::IoSlice<'_>], offset: u64) -> io::Result<usize> {
        let total_len: usize = bufs.iter().map(|b| b.len()).sum();
        let mut cur_offset = offset;
        for buf in bufs {
            self.write_bytes_to_file(&self.file, buf, cur_offset)?;
            cur_offset += buf.len() as u64;
        }
        self.device.write_count.fetch_add(1, Ordering::AcqRel);
        Ok(total_len)
    }

    fn write_bytes_to_file(&self, file: &std::fs::File, data: &[u8], offset: u64) -> io::Result<()> {
        match self.device.alignment {
            WalBlockAlignment::Unaligned => {
                file.write_at(data, offset)?;
            }
            WalBlockAlignment::Aligned { io_unit_bytes } => {
                let plan = self.device.alignment.plan_write(offset, data.len());
                let aligned_off = plan.aligned_offset;
                let aligned_len = plan.aligned_len;
                let mut raw = vec![0u8; aligned_len + io_unit_bytes];
                let start = raw.as_mut_ptr().align_offset(io_unit_bytes);
                if start >= io_unit_bytes {
                    return Err(io::Error::other("BlockDevice: failed to align O_DIRECT buffer"));
                }
                let buf = &mut raw[start..start + aligned_len];
                if plan.requires_read_modify_write {
                    match file.read_at(buf, aligned_off) {
                        Ok(_) => {}
                        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {}
                        Err(e) => return Err(e),
                    }
                    self.device.rmw_count.fetch_add(1, Ordering::AcqRel);
                }
                let payload_off = plan.payload_offset_within_aligned;
                buf[payload_off..payload_off + data.len()].copy_from_slice(data);
                file.write_at(buf, aligned_off)?;
            }
        }
        Ok(())
    }

    pub(super) fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        self.file.read_at(buf, offset)
    }

    pub(super) fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        let n = self.read_at(buf, offset)?;
        if n < buf.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "BlockSegment: read_exact_at short read: wanted {} got {n}",
                    buf.len()
                ),
            ));
        }
        Ok(())
    }

    pub(super) fn fdatasync(&self) -> io::Result<()> {
        self.device.fdatasync_count.fetch_add(1, Ordering::AcqRel);
        self.file.sync_data()?;
        Ok(())
    }

    pub(super) fn fsync(&self) -> io::Result<()> {
        self.fdatasync()
    }

    pub(super) fn len(&self) -> io::Result<u64> {
        Ok(self.file.metadata()?.len())
    }

    pub(super) fn truncate(&self, len: u64) -> io::Result<()> {
        self.file.set_len(len)
    }
}

// ---------------------------------------------------------------------------
// MemBlockDevice — in-memory test harness with error/corruption injection.
// ---------------------------------------------------------------------------

/// In-memory block device for deterministic failure testing. Stores segments
/// as `BTreeMap<PathBuf, Vec<u8>>` and supports error injection
/// ([`BlockDeviceController`]) and corruption injection. `fdatasync` is a
/// no-op (data is already "durable" in RAM).
///
/// Two write implementations are selected by [`Self::alignment`]:
/// - [`WalBlockAlignment::Unaligned`] — byte-addressable (RAM / SCM / PMEM
///   model). Default via [`MemBlockDevice::new`].
/// - [`WalBlockAlignment::Aligned`] — block-aligned with read-modify-write,
///   modelling an SSD/NVMe I/O unit without real files. Via
///   [`MemBlockDevice::with_alignment`].
#[derive(Clone)]
pub struct MemBlockDevice {
    segments: Arc<Mutex<BTreeMap<PathBuf, Vec<u8>>>>,
    layouts: Arc<Mutex<BTreeSet<PathBuf>>>,
    controller: BlockDeviceController,
    write_count: Arc<AtomicU64>,
    fdatasync_count: Arc<AtomicU64>,
    alignment: WalBlockAlignment,
    rmw_count: Arc<AtomicU64>,
}

impl MemBlockDevice {
    /// Create an unaligned (byte-addressable) in-memory block device — the
    /// simple path modelling RAM / SCM / PMEM.
    #[must_use]
    pub fn new() -> Self {
        tracing::info!("MemBlockDevice::new — in-memory, unaligned");
        Self::with_alignment(WalBlockAlignment::Unaligned)
    }

    /// Create an in-memory block device with an explicit alignment mode.
    #[must_use]
    pub fn with_alignment(alignment: WalBlockAlignment) -> Self {
        Self {
            segments: Arc::new(Mutex::new(BTreeMap::new())),
            layouts: Arc::new(Mutex::new(BTreeSet::new())),
            controller: BlockDeviceController::default(),
            write_count: Arc::new(AtomicU64::new(0)),
            fdatasync_count: Arc::new(AtomicU64::new(0)),
            alignment,
            rmw_count: Arc::new(AtomicU64::new(0)),
        }
    }

    #[must_use]
    pub fn controller(&self) -> &BlockDeviceController {
        &self.controller
    }

    #[must_use]
    pub fn alignment(&self) -> WalBlockAlignment {
        self.alignment
    }

    #[must_use]
    pub fn write_count(&self) -> u64 {
        self.write_count.load(Ordering::Acquire)
    }

    #[must_use]
    pub fn fdatasync_count(&self) -> u64 {
        self.fdatasync_count.load(Ordering::Acquire)
    }

    #[must_use]
    pub fn rmw_count(&self) -> u64 {
        self.rmw_count.load(Ordering::Acquire)
    }

    pub(super) fn open_segment(
        &self,
        segment_path: &Path,
        opts: &OpenOptions,
    ) -> io::Result<MemBlockSegment> {
        let segment_path = segment_path.to_path_buf();
        self.controller.apply_corruptions(&self.segments);
        self.controller.check_io()?;
        let mut segments = self.segments.lock();
        if opts.create_new && segments.contains_key(&segment_path) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!(
                    "MemBlockDevice: segment already exists: {}",
                    segment_path.display()
                ),
            ));
        }
        if opts.create && !segments.contains_key(&segment_path) {
            segments.insert(segment_path.clone(), Vec::new());
        }
        if opts.truncate {
            if let Some(data) = segments.get_mut(&segment_path) {
                data.clear();
            }
        }
        if !segments.contains_key(&segment_path) {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("MemBlockDevice: segment not found: {}", segment_path.display()),
            ));
        }
        Ok(MemBlockSegment {
            segment_path,
            device: self.clone(),
        })
    }

    pub(super) fn rename_segment(&self, from: &Path, to: &Path) -> io::Result<()> {
        self.controller.check_io()?;
        let mut segments = self.segments.lock();
        let data = segments.remove(from).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("MemBlockDevice rename_segment: not found: {}", from.display()),
            )
        })?;
        segments.insert(to.to_path_buf(), data);
        Ok(())
    }

    pub(super) fn unlink_segment(&self, segment_path: &Path) -> io::Result<()> {
        self.controller.check_io()?;
        let mut segments = self.segments.lock();
        if segments.remove(segment_path).is_none() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!(
                    "MemBlockDevice unlink_segment: not found: {}",
                    segment_path.display()
                ),
            ));
        }
        Ok(())
    }

    pub(super) fn list_layout(&self, layout_path: &Path) -> io::Result<Vec<PathBuf>> {
        self.controller.check_io()?;
        let segments = self.segments.lock();
        let layouts = self.layouts.lock();
        let mut entries = BTreeSet::new();
        for path in segments.keys() {
            if let Ok(rel) = path.strip_prefix(layout_path) {
                if let Some(first) = rel.components().next() {
                    entries.insert(layout_path.join(first));
                }
            }
        }
        for path in layouts.iter() {
            if let Ok(rel) = path.strip_prefix(layout_path) {
                if let Some(first) = rel.components().next() {
                    entries.insert(layout_path.join(first));
                }
            }
        }
        Ok(entries.into_iter().collect())
    }

    pub(super) fn create_layout(&self, layout_path: &Path) -> io::Result<()> {
        self.controller.check_io()?;
        let mut layouts = self.layouts.lock();
        let mut cur = PathBuf::new();
        for comp in layout_path.components() {
            cur.push(comp);
            layouts.insert(cur.clone());
        }
        Ok(())
    }

    pub(super) fn contains_path(&self, path: &Path) -> bool {
        let segments = self.segments.lock();
        if segments.contains_key(path) {
            return true;
        }
        let layouts = self.layouts.lock();
        layouts.contains(path)
    }
}

impl Default for MemBlockDevice {
    fn default() -> Self {
        Self::new()
    }
}

pub(crate) struct MemBlockSegment {
    segment_path: PathBuf,
    device: MemBlockDevice,
}

impl MemBlockSegment {
    pub(super) fn write_at(&self, data: &[u8], offset: u64) -> io::Result<usize> {
        self.device.controller.check_write()?;
        self.write_bytes(data, offset)?;
        self.device.write_count.fetch_add(1, Ordering::AcqRel);
        Ok(data.len())
    }

    pub(super) fn write_vectored_at(&self, bufs: &[std::io::IoSlice<'_>], offset: u64) -> io::Result<usize> {
        self.device.controller.check_write()?;
        let total_len: usize = bufs.iter().map(|b| b.len()).sum();
        let mut cur_offset = offset;
        for buf in bufs {
            self.write_bytes(buf, cur_offset)?;
            cur_offset += buf.len() as u64;
        }
        self.device.write_count.fetch_add(1, Ordering::AcqRel);
        Ok(total_len)
    }

    fn write_bytes(&self, data: &[u8], offset: u64) -> io::Result<()> {
        match self.device.alignment {
            WalBlockAlignment::Unaligned => self.write_unaligned(data, offset),
            WalBlockAlignment::Aligned { .. } => self.write_aligned(data, offset),
        }
    }

    fn write_unaligned(&self, data: &[u8], offset: u64) -> io::Result<()> {
        let mut segments = self.device.segments.lock();
        let segment_data = segments
            .get_mut(&self.segment_path)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "MemBlockSegment: segment removed"))?;
        let off = usize::try_from(offset).expect("offset exceeds usize");
        let end = off + data.len();
        if end > segment_data.len() {
            segment_data.resize(end, 0);
        }
        segment_data[off..end].copy_from_slice(data);
        Ok(())
    }

    fn write_aligned(&self, data: &[u8], offset: u64) -> io::Result<()> {
        let plan = self.device.alignment.plan_write(offset, data.len());
        let mut segments = self.device.segments.lock();
        let segment_data = segments
            .get_mut(&self.segment_path)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "MemBlockSegment: segment removed"))?;
        let aligned_off = usize::try_from(plan.aligned_offset).expect("aligned offset exceeds usize");
        let aligned_end = aligned_off + plan.aligned_len;
        if aligned_end > segment_data.len() {
            segment_data.resize(aligned_end, 0);
        }
        let payload_off = aligned_off + plan.payload_offset_within_aligned;
        segment_data[payload_off..payload_off + data.len()].copy_from_slice(data);
        if plan.requires_read_modify_write {
            self.device.rmw_count.fetch_add(1, Ordering::AcqRel);
        }
        Ok(())
    }

    pub(super) fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        self.device.controller.check_io()?;
        self.device.controller.apply_corruptions(&self.device.segments);
        let segments = self.device.segments.lock();
        let segment_data = segments
            .get(&self.segment_path)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "MemBlockSegment: segment removed"))?;
        let off = usize::try_from(offset).expect("offset exceeds usize");
        if off >= segment_data.len() {
            return Ok(0);
        }
        let avail = segment_data.len() - off;
        let n = buf.len().min(avail);
        buf[..n].copy_from_slice(&segment_data[off..off + n]);
        Ok(n)
    }

    pub(super) fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        let n = self.read_at(buf, offset)?;
        if n < buf.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "MemBlockSegment: read_exact_at short read: wanted {} got {n}",
                    buf.len()
                ),
            ));
        }
        Ok(())
    }

    pub(super) fn fdatasync(&self) -> io::Result<()> {
        self.device.controller.check_sync()?;
        self.device.controller.check_write()?;
        self.device.fdatasync_count.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }

    pub(super) fn fsync(&self) -> io::Result<()> {
        self.fdatasync()
    }

    pub(super) fn len(&self) -> io::Result<u64> {
        self.device.controller.check_io()?;
        let segments = self.device.segments.lock();
        let segment_data = segments
            .get(&self.segment_path)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "MemBlockSegment: segment removed"))?;
        Ok(segment_data.len() as u64)
    }

    pub(super) fn truncate(&self, len: u64) -> io::Result<()> {
        self.device.controller.check_write()?;
        let mut segments = self.device.segments.lock();
        let segment_data = segments
            .get_mut(&self.segment_path)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "MemBlockSegment: segment removed"))?;
        segment_data.truncate(usize::try_from(len).expect("len exceeds usize"));
        Ok(())
    }
}
