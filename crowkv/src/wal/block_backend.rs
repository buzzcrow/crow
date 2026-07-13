// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet};
use std::io;
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

/// Simulated block device used as a test [`super::IoBackend`].
///
/// Two write implementations are selected by [`Self::alignment`], modelling the
/// two real device classes:
///
/// - [`WalBlockAlignment::Unaligned`] — byte-addressable media (RAM / SCM /
///   PMEM). Writes land at arbitrary offsets with no amplification. This is the
///   simpler path and the default ([`BlockDevice::new`]).
/// - [`WalBlockAlignment::Aligned`] — block media that requires I/O aligned to
///   the device I/O unit (e.g. an SSD/`NVMe` under `O_DIRECT`). Sub-block
///   writes are widened to the enclosing aligned range via read-modify-write,
///   so physical storage always advances in whole I/O units and the device
///   tracks the resulting write amplification ([`BlockDevice::ssd`]).
#[derive(Clone)]
pub struct BlockDevice {
    segments: Arc<Mutex<BTreeMap<PathBuf, Vec<u8>>>>,
    layouts: Arc<Mutex<BTreeSet<PathBuf>>>,
    controller: BlockDeviceController,
    write_count: Arc<AtomicU64>,
    fdatasync_count: Arc<AtomicU64>,
    alignment: WalBlockAlignment,
    /// Logical (payload) bytes accepted by `write_at`.
    logical_bytes_written: Arc<AtomicU64>,
    /// Physical bytes the device actually wrote (aligned ranges). Equals
    /// `logical_bytes_written` for an unaligned device.
    physical_bytes_written: Arc<AtomicU64>,
    /// Number of writes that required a read-modify-write of a partial block.
    rmw_count: Arc<AtomicU64>,
}

impl BlockDevice {
    /// Create an unaligned (byte-addressable) block device — the simple path
    /// modelling RAM / SCM / PMEM.
    #[must_use]
    pub fn new() -> Self {
        Self::with_alignment(WalBlockAlignment::Unaligned)
    }

    /// Create an aligned block device modelling an SSD/NVMe using the default
    /// I/O unit size (`WalBlockAlignment::DEFAULT_IO_UNIT_BYTES`).
    #[must_use]
    pub fn ssd() -> Self {
        Self::with_alignment(WalBlockAlignment::default_aligned())
    }

    /// Create a block device with an explicit alignment mode.
    #[must_use]
    pub fn with_alignment(alignment: WalBlockAlignment) -> Self {
        Self {
            segments: Arc::new(Mutex::new(BTreeMap::new())),
            layouts: Arc::new(Mutex::new(BTreeSet::new())),
            controller: BlockDeviceController::default(),
            write_count: Arc::new(AtomicU64::new(0)),
            fdatasync_count: Arc::new(AtomicU64::new(0)),
            alignment,
            logical_bytes_written: Arc::new(AtomicU64::new(0)),
            physical_bytes_written: Arc::new(AtomicU64::new(0)),
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

    /// Total logical (payload) bytes accepted by writes.
    #[must_use]
    pub fn logical_bytes_written(&self) -> u64 {
        self.logical_bytes_written.load(Ordering::Acquire)
    }

    /// Total physical bytes written to the underlying media. For an aligned
    /// device this is larger than [`Self::logical_bytes_written`] whenever
    /// writes are not block-aligned (write amplification).
    #[must_use]
    pub fn physical_bytes_written(&self) -> u64 {
        self.physical_bytes_written.load(Ordering::Acquire)
    }

    /// Number of writes that triggered a read-modify-write of a partial block.
    #[must_use]
    pub fn rmw_count(&self) -> u64 {
        self.rmw_count.load(Ordering::Acquire)
    }

    pub(crate) fn open_segment(&self, segment_path: &Path, opts: &OpenOptions) -> io::Result<BlockSegment> {
        self.controller.apply_corruptions(&self.segments);
        self.controller.check_io()?;
        let segment_path = segment_path.to_path_buf();
        let mut segments = self.segments.lock();
        if opts.create_new && segments.contains_key(&segment_path) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("BlockDevice: segment already exists: {}", segment_path.display()),
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
                format!("BlockDevice: segment not found: {}", segment_path.display()),
            ));
        }
        Ok(BlockSegment {
            segment_path,
            device: self.clone(),
        })
    }

    pub(crate) fn rename_segment(&self, from: &Path, to: &Path) -> io::Result<()> {
        self.controller.check_io()?;
        let mut segments = self.segments.lock();
        let data = segments.remove(from).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("BlockDevice rename_segment: not found: {}", from.display()),
            )
        })?;
        segments.insert(to.to_path_buf(), data);
        Ok(())
    }

    pub(crate) fn unlink_segment(&self, segment_path: &Path) -> io::Result<()> {
        self.controller.check_io()?;
        let mut segments = self.segments.lock();
        if segments.remove(segment_path).is_none() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!(
                    "BlockDevice unlink_segment: not found: {}",
                    segment_path.display()
                ),
            ));
        }
        Ok(())
    }

    pub(crate) fn list_layout(&self, layout_path: &Path) -> io::Result<Vec<PathBuf>> {
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

    pub(crate) fn create_layout(&self, layout_path: &Path) -> io::Result<()> {
        self.controller.check_io()?;
        let mut layouts = self.layouts.lock();
        let mut cur = PathBuf::new();
        for comp in layout_path.components() {
            cur.push(comp);
            layouts.insert(cur.clone());
        }
        Ok(())
    }

    pub(crate) fn contains_path(&self, path: &Path) -> bool {
        let segments = self.segments.lock();
        if segments.contains_key(path) {
            return true;
        }
        let layouts = self.layouts.lock();
        layouts.contains(path)
    }
}

impl Default for BlockDevice {
    fn default() -> Self {
        Self::new()
    }
}

pub(crate) struct BlockSegment {
    segment_path: PathBuf,
    device: BlockDevice,
}

impl BlockSegment {
    /// Write `data` at byte `offset`, dispatching to the device's alignment
    /// implementation.
    pub fn write_at(&self, data: &[u8], offset: u64) -> io::Result<usize> {
        self.device.controller.check_write()?;
        self.write_bytes(data, offset)?;
        self.device.write_count.fetch_add(1, Ordering::AcqRel);
        self.device
            .logical_bytes_written
            .fetch_add(data.len() as u64, Ordering::AcqRel);
        Ok(data.len())
    }

    /// Write multiple non-contiguous buffers at `offset` as a single logical
    /// write operation. The underlying simulated media still copies each slice,
    /// but this avoids a caller-side concatenation copy.
    pub fn write_vectored_at(&self, bufs: &[std::io::IoSlice<'_>], offset: u64) -> io::Result<usize> {
        self.device.controller.check_write()?;
        let total_len: usize = bufs.iter().map(|b| b.len()).sum();
        let mut cur_offset = offset;
        for buf in bufs {
            self.write_bytes(buf, cur_offset)?;
            cur_offset += buf.len() as u64;
        }
        self.device.write_count.fetch_add(1, Ordering::AcqRel);
        self.device
            .logical_bytes_written
            .fetch_add(total_len as u64, Ordering::AcqRel);
        Ok(total_len)
    }

    /// Internal byte copy: applies the data to the in-memory segment without
    /// updating any counters. Used by both `write_at` and `write_vectored_at`.
    fn write_bytes(&self, data: &[u8], offset: u64) -> io::Result<()> {
        match self.device.alignment {
            WalBlockAlignment::Unaligned => self.write_unaligned(data, offset),
            WalBlockAlignment::Aligned { .. } => self.write_aligned(data, offset),
        }
    }

    /// Byte-addressable write: payload lands directly at `offset`. Models
    /// RAM / SCM / PMEM where there is no alignment requirement.
    fn write_unaligned(&self, data: &[u8], offset: u64) -> io::Result<()> {
        let mut segments = self.device.segments.lock();
        let segment_data = segments
            .get_mut(&self.segment_path)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "BlockSegment: segment removed"))?;
        let off = usize::try_from(offset).expect("offset exceeds usize");
        let end = off + data.len();
        if end > segment_data.len() {
            segment_data.resize(end, 0);
        }
        segment_data[off..end].copy_from_slice(data);
        self.device
            .physical_bytes_written
            .fetch_add(data.len() as u64, Ordering::AcqRel);
        Ok(())
    }

    /// Block-aligned write: the logical write is widened to the enclosing
    /// aligned range and applied as a read-modify-write so the device only ever
    /// performs aligned physical I/O. Models an SSD/NVMe whose I/O unit equals
    /// the configured alignment. Tracks the resulting write amplification.
    fn write_aligned(&self, data: &[u8], offset: u64) -> io::Result<()> {
        let plan = self.device.alignment.plan_write(offset, data.len());
        let mut segments = self.device.segments.lock();
        let segment_data = segments
            .get_mut(&self.segment_path)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "BlockSegment: segment removed"))?;
        let aligned_off = usize::try_from(plan.aligned_offset).expect("aligned offset exceeds usize");
        let aligned_end = aligned_off + plan.aligned_len;
        // Physical media advances in whole I/O units; grow to the aligned end.
        if aligned_end > segment_data.len() {
            segment_data.resize(aligned_end, 0);
        }
        // Read-modify-write: the enclosing block is read (it is already present
        // in `segment_data`), the payload region overwritten, and the whole
        // aligned block rewritten. Direct overlay yields identical final bytes.
        let payload_off = aligned_off + plan.payload_offset_within_aligned;
        segment_data[payload_off..payload_off + data.len()].copy_from_slice(data);
        self.device
            .physical_bytes_written
            .fetch_add(plan.aligned_len as u64, Ordering::AcqRel);
        if plan.requires_read_modify_write {
            self.device.rmw_count.fetch_add(1, Ordering::AcqRel);
        }
        Ok(())
    }

    pub fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        self.device.controller.apply_corruptions(&self.device.segments);
        self.device.controller.check_io()?;
        let segments = self.device.segments.lock();
        let segment_data = segments
            .get(&self.segment_path)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "BlockSegment: segment removed"))?;
        let off = usize::try_from(offset).expect("offset exceeds usize");
        if off >= segment_data.len() {
            return Ok(0);
        }
        let avail = segment_data.len() - off;
        let n = buf.len().min(avail);
        buf[..n].copy_from_slice(&segment_data[off..off + n]);
        Ok(n)
    }

    pub fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
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

    pub fn fdatasync(&self) -> io::Result<()> {
        self.device.controller.check_sync()?;
        self.device.controller.check_write()?;
        self.device.fdatasync_count.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }

    pub fn fsync(&self) -> io::Result<()> {
        self.fdatasync()
    }

    pub fn len(&self) -> io::Result<u64> {
        self.device.controller.check_io()?;
        let segments = self.device.segments.lock();
        let segment_data = segments
            .get(&self.segment_path)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "BlockSegment: segment removed"))?;
        Ok(segment_data.len() as u64)
    }

    pub fn truncate(&self, len: u64) -> io::Result<()> {
        self.device.controller.check_write()?;
        let mut segments = self.device.segments.lock();
        let segment_data = segments
            .get_mut(&self.segment_path)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "BlockSegment: segment removed"))?;
        segment_data.truncate(usize::try_from(len).expect("len exceeds usize"));
        Ok(())
    }
}
