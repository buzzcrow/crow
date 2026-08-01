// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Safe Rust adapter over the crowtree C ABI (`c_api.h`, PT8).
//!
//! Wraps the opaque `ct_*` handles in RAII types, translates owned `ct_buf`
//! buffers into `Vec<u8>` (freeing them via `ct_free_buf`), maps `ct_status`
//! into `Result`, and offers an async facade (`AsyncCrowtree`). `get`/`flush`/
//! `snapshot` drive the engine's io_uring reactor directly (no OS thread hop,
//! Phase 3); the remaining methods (no async C API twin exists
//! for them yet -- Phase 2 scoped only get/flush/snapshot) still bridge via
//! `spawn_blocking`.

use std::ffi::CString;
use std::future::Future;
use std::os::fd::{AsRawFd, RawFd};
use std::os::raw::{c_char, c_int};
use std::pin::Pin;
use std::ptr::NonNull;
use std::sync::Arc;

use tokio::io::unix::AsyncFd;

#[allow(non_camel_case_types)]
mod sys {
    use super::{c_char, c_int};

    #[repr(C)]
    pub struct ct_tree {
        _private: [u8; 0],
    }
    #[repr(C)]
    pub struct ct_view {
        _private: [u8; 0],
    }
    #[repr(C)]
    pub struct ct_iter {
        _private: [u8; 0],
    }
    #[repr(C)]
    pub struct ct_export {
        _private: [u8; 0],
    }
    #[repr(C)]
    pub struct ct_column_widths {
        pub count_w: usize,
        pub tps_w: usize,
    }
    #[repr(C)]
    pub struct ct_import {
        _private: [u8; 0],
    }
    #[repr(C)]
    pub struct ct_future {
        _private: [u8; 0],
    }
    #[repr(C)]
    pub struct ct_write_handle {
        _private: [u8; 0],
    }

    #[repr(C)]
    pub struct ct_write_ptrs {
        pub key: *mut u8,
        pub val: *mut u8,
    }

    #[repr(C)]
    pub struct ct_buf {
        pub data: *mut u8,
        pub len: usize,
    }

    #[repr(C)]
    pub struct ct_kv_ref {
        pub key: *const u8,
        pub key_len: usize,
        pub value: *const u8,
        pub value_len: usize,
        pub kind: u8,
    }

    #[repr(C)]
    pub struct ct_gc_stats {
        pub tombstones_dropped: u64,
        pub pages_freed: u64,
        pub bytes_freed: u64,
    }

    #[repr(C)]
    #[derive(Default)]
    pub struct ct_stats {
        pub last_applied_slot: u64,
        pub contiguous_slot: u64,
        pub gc_watermark: u64,
        pub io_failed: c_int,
        pub snapshot_pages_written: u64,
        pub snapshot_pages_total: u64,
        pub snapshot_segments_written: u64,
        pub buffer_pool_hits: u64,
        pub buffer_pool_misses: u64,
        pub buffer_pool_evictions: u64,
        pub buffer_pool_writebacks: u64,
        pub buffer_pool_resident: u32,
        pub buffer_pool_dirty: u32,
        pub buffer_pool_used: u32,
        pub buffer_pool_num_frames: u32,
        pub mt_upsert_total: u64,
        pub mt_get_total: u64,
        pub mt_get_hit_total: u64,
        pub flush_drain_total: u64,
        pub flush_entries_total: u64,
        pub snapshot_total: u64,
        pub l1_get_total: u64,
        pub l1_get_hit_total: u64,
    }

    #[repr(C)]
    pub struct ct_options {
        pub path: *const c_char,
        pub iu_size: u32,
        pub frame_bytes: u32,
        pub buffer_pool_bytes: u64,
        pub compression: u8,
        pub max_inline_value: u64,
        pub backend: u8,
        pub block_size: u64,
        pub store_id: u32,
        pub group_id: u32,
        pub sync_mode: u8,
        pub log_dir: *const c_char,
        pub log_level: *const c_char,
        pub log_file_prefix: *const c_char,
        pub log_max_file_mb: usize,
        pub log_max_files: usize,
    }

    extern "C" {
        pub fn ct_free_buf(buf: *mut ct_buf);
        pub fn ct_open(opt: *const ct_options, out: *mut *mut ct_tree) -> c_int;
        pub fn ct_close(t: *mut ct_tree);
        pub fn ct_init_logging(
            log_dir: *const c_char,
            level: *const c_char,
            max_file_mb: usize,
            max_files: usize,
            file_prefix: *const c_char,
        );
        pub fn ct_flush_logging();
        pub fn ct_shutdown_logging();
        pub fn ct_snapshot(t: *mut ct_tree, out_last_applied: *mut u64) -> c_int;
        pub fn ct_last_applied_slot(t: *const ct_tree) -> u64;
        pub fn ct_set_gc_watermark(t: *mut ct_tree, snapshot_slot: u64, safe_slot: u64);
        pub fn ct_collect_garbage(t: *mut ct_tree, out_stats: *mut ct_gc_stats) -> c_int;
        pub fn ct_io_failed(t: *const ct_tree) -> c_int;
        pub fn ct_clear_io_error(t: *mut ct_tree);
        pub fn ct_clear(t: *mut ct_tree) -> c_int;
        pub fn ct_get_stats(t: *const ct_tree, out: *mut ct_stats);
        pub fn ct_apply_put(
            t: *mut ct_tree,
            slot: u64,
            key: *const u8,
            klen: usize,
            val: *const u8,
            vlen: usize,
        ) -> c_int;
        pub fn ct_apply_delete(t: *mut ct_tree, slot: u64, key: *const u8, klen: usize) -> c_int;
        pub fn ct_apply_batch_slices(t: *mut ct_tree, slot: u64, ops: *const ct_kv_ref, count: u64) -> c_int;
        pub fn ct_force_advance_slot(t: *mut ct_tree, slot: u64);
        pub fn ct_alloc(
            t: *mut ct_tree,
            key_len: usize,
            val_len: usize,
            out_handle: *mut *mut ct_write_handle,
            out_ptrs: *mut ct_write_ptrs,
        ) -> c_int;
        pub fn ct_apply_put_owned(t: *mut ct_tree, slot: u64, handle: *mut ct_write_handle) -> c_int;
        pub fn ct_free_handle(handle: *mut ct_write_handle);
        pub fn ct_put(t: *mut ct_tree, key: *const u8, klen: usize, val: *const u8, vlen: usize) -> c_int;
        pub fn ct_del(t: *mut ct_tree, key: *const u8, klen: usize) -> c_int;
        pub fn ct_flush(t: *mut ct_tree) -> c_int;
        pub fn ct_get(
            t: *mut ct_tree,
            key: *const u8,
            klen: usize,
            found: *mut c_int,
            slot: *mut u64,
            value: *mut ct_buf,
        ) -> c_int;
        pub fn ct_scan(
            t: *mut ct_tree,
            prefix: *const u8,
            plen: usize,
            limit: usize,
            include_tombstones: c_int,
            out_entries: *mut ct_buf,
            out_count: *mut u64,
            truncated: *mut c_int,
        ) -> c_int;
        pub fn ct_snapshot_view(t: *mut ct_tree, out: *mut *mut ct_view) -> c_int;
        pub fn ct_view_at_slot(v: *const ct_view) -> u64;
        pub fn ct_view_iter(v: *mut ct_view, out: *mut *mut ct_iter) -> c_int;
        pub fn ct_iter_next(
            it: *mut ct_iter,
            key: *mut ct_buf,
            slot: *mut u64,
            kind: *mut u8,
            value: *mut ct_buf,
            valid: *mut c_int,
        ) -> c_int;
        pub fn ct_iter_release(it: *mut ct_iter);
        pub fn ct_view_release(v: *mut ct_view);
        pub fn ct_snapshot_export_begin(t: *mut ct_tree, out: *mut *mut ct_export) -> c_int;
        pub fn ct_snapshot_export_next(e: *mut ct_export, chunk: *mut ct_buf, done: *mut c_int) -> c_int;
        pub fn ct_snapshot_export_end(e: *mut ct_export);
        pub fn ct_snapshot_import_begin(t: *mut ct_tree, out: *mut *mut ct_import) -> c_int;
        pub fn ct_snapshot_import_feed(im: *mut ct_import, chunk: *const u8, len: usize) -> c_int;
        pub fn ct_snapshot_import_finish(im: *mut ct_import, out_at_slot: *mut u64) -> c_int;
        pub fn ct_snapshot_import_end(im: *mut ct_import);

        // ── Async data path ──
        pub fn ct_evict_clean_leaves(t: *mut ct_tree, max_resident_leaves: u64) -> u64;
        pub fn ct_evict_clean_inner(t: *mut ct_tree, max_resident_inner: u64) -> u64;
        pub fn ct_get_async(t: *mut ct_tree, key: *const u8, klen: usize) -> *mut ct_future;
        pub fn ct_flush_async(t: *mut ct_tree) -> *mut ct_future;
        pub fn ct_snapshot_async(t: *mut ct_tree) -> *mut ct_future;
        pub fn ct_scan_async(t: *mut ct_tree, prefix: *const u8, plen: usize, limit: usize)
            -> *mut ct_future;
        pub fn ct_future_poll(
            f: *mut ct_future,
            done: *mut c_int,
            out_found: *mut c_int,
            out_slot: *mut u64,
            out_value: *mut ct_buf,
        ) -> c_int;
        pub fn ct_future_free(f: *mut ct_future);
        pub fn ct_reactor_eventfd(t: *const ct_tree) -> i32;

        // ── Metrics FFI ──
        pub fn ct_flush_metrics_str(
            t: *mut ct_tree,
            window_secs: f64,
            timestamp: *const c_char,
            width: usize,
        ) -> *mut c_char;
        pub fn ct_flush_metrics_str_ext(
            t: *mut ct_tree,
            window_secs: f64,
            timestamp: *const c_char,
            width: usize,
            count_w: usize,
            tps_w: usize,
        ) -> *mut c_char;
        pub fn ct_max_name_len(t: *const ct_tree) -> usize;
        pub fn ct_negotiate_widths(t: *const ct_tree, input: ct_column_widths, out: *mut ct_column_widths);
        pub fn ct_free_string(s: *mut c_char);
    }
}

/// Error mirroring `crowtree::Code` (negative status codes).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CtError {
    NotFound,
    InvalidArgument,
    Corruption,
    IoError,
    NotSupported,
    Internal,
    Unknown(i32),
}

impl std::fmt::Display for CtError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for CtError {}

fn check(code: c_int) -> Result<(), CtError> {
    match code {
        0 => Ok(()),
        -1 => Err(CtError::NotFound),
        -2 => Err(CtError::InvalidArgument),
        -3 => Err(CtError::Corruption),
        -4 => Err(CtError::IoError),
        -5 => Err(CtError::NotSupported),
        -6 => Err(CtError::Internal),
        other => Err(CtError::Unknown(other)),
    }
}

/// Consume an owned `ct_buf` into a `Vec<u8>`, freeing the C allocation.
fn take_buf(mut buf: sys::ct_buf) -> Vec<u8> {
    if buf.data.is_null() || buf.len == 0 {
        unsafe { sys::ct_free_buf(&mut buf) };
        return Vec::new();
    }
    let v = unsafe { std::slice::from_raw_parts(buf.data, buf.len) }.to_vec();
    unsafe { sys::ct_free_buf(&mut buf) };
    v
}

/// Copies a `ct_buf`'s bytes into a `Vec<u8>` *without* freeing it -- unlike
/// `take_buf`, used only for a `ct_get_async` completion's value, which may
/// be a borrowed pointer into a still-live frame (zero-copy
/// fast path, ) that must never be passed to
/// `ct_free_buf`. The underlying `ct_future` (and, with it, any epoch guard
/// backing that borrow) is released separately, immediately afterward, via
/// `ct_future_free` -- see `try_poll_ct_future`.
fn copy_buf(buf: sys::ct_buf) -> Vec<u8> {
    if buf.data.is_null() || buf.len == 0 {
        return Vec::new();
    }
    unsafe { std::slice::from_raw_parts(buf.data, buf.len) }.to_vec()
}

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
    fn as_u8(self) -> u8 {
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
    /// C++ log filename prefix (empty = "crowtree").
    pub log_file_prefix: String,
    /// Max C++ log file size in MiB before rotation.
    pub log_max_file_mb: usize,
    /// Number of rotated C++ log files to keep.
    pub log_max_files: usize,
}

/// One record of a multi-key batch passed to [`Crowtree::apply_batch`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BatchOp<'a> {
    Put { key: &'a [u8], value: &'a [u8] },
    Delete { key: &'a [u8] },
}

/// Result of an explicit [`Crowtree::collect_garbage`] sweep.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct GcStats {
    pub tombstones_dropped: u64,
    pub pages_freed: u64,
    pub bytes_freed: u64,
}

/// Point-in-time diagnostics snapshot; see [`Crowtree::stats`]. Every field
/// is O(1) on the C++ side (an already-tracked atomic counter or
/// `BufferPool::stats`), so this is safe to poll periodically.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Stats {
    pub last_applied_slot: u64,
    pub contiguous_slot: u64,
    pub gc_watermark: u64,
    pub io_failed: bool,
    pub snapshot_pages_written: u64,
    pub snapshot_pages_total: u64,
    pub snapshot_segments_written: u64,
    pub buffer_pool_hits: u64,
    pub buffer_pool_misses: u64,
    pub buffer_pool_evictions: u64,
    pub buffer_pool_writebacks: u64,
    pub buffer_pool_resident: u32,
    pub buffer_pool_dirty: u32,
    pub buffer_pool_used: u32,
    pub buffer_pool_num_frames: u32,
    pub mt_upsert_total: u64,
    pub mt_get_total: u64,
    pub mt_get_hit_total: u64,
    pub flush_drain_total: u64,
    pub flush_entries_total: u64,
    pub snapshot_total: u64,
    pub l1_get_total: u64,
    pub l1_get_hit_total: u64,
}

/// A scan result entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanEntry {
    pub key: Vec<u8>,
    pub slot: u64,
    pub value: Vec<u8>,
    pub tombstone: bool,
}

/// A snapshot-view entry (includes tombstones).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ViewEntry {
    pub key: Vec<u8>,
    pub slot: u64,
    pub tombstone: bool,
    pub value: Vec<u8>,
}

/// RAII wrapper over a crowtree-owned zero-copy write handle (R3).
/// The caller writes key and value bytes directly into crowtree-owned
/// memory via [`WriteHandle::key_mut`] / [`WriteHandle::value_mut`],
/// then [`WriteHandle::apply`] consumes the handle with zero value
/// memcpy. If dropped without applying, the handle is freed via
/// `ct_free_handle` (RAII safety).
///
/// `!Send + !Sync`: the handle's internal pointers are not safe to
/// share across threads.
pub struct WriteHandle {
    ptr: *mut sys::ct_write_handle,
    tree: *mut sys::ct_tree,
    key_ptr: *mut u8,
    val_ptr: *mut u8,
    key_len: usize,
    val_len: usize,
    consumed: bool,
}

impl std::fmt::Debug for WriteHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WriteHandle")
            .field("key_len", &self.key_len)
            .field("val_len", &self.val_len)
            .field("consumed", &self.consumed)
            .finish_non_exhaustive()
    }
}

impl WriteHandle {
    /// Writable key slice `[0, key_len)`.
    pub fn key_mut(&mut self) -> &mut [u8] {
        if self.consumed || self.key_len == 0 || self.key_ptr.is_null() {
            return &mut [];
        }
        unsafe { std::slice::from_raw_parts_mut(self.key_ptr, self.key_len) }
    }

    /// Writable value slice `[0, val_len)`.
    pub fn value_mut(&mut self) -> &mut [u8] {
        if self.consumed || self.val_len == 0 || self.val_ptr.is_null() {
            return &mut [];
        }
        unsafe { std::slice::from_raw_parts_mut(self.val_ptr, self.val_len) }
    }

    /// Apply the pre-allocated key+value at `slot` (zero value memcpy).
    /// Consumes the handle.
    pub fn apply(mut self, slot: u64) -> Result<(), CtError> {
        if self.consumed {
            return Err(CtError::InvalidArgument);
        }
        self.consumed = true;
        check(unsafe { sys::ct_apply_put_owned(self.tree, slot, self.ptr) })
    }
}

impl Drop for WriteHandle {
    fn drop(&mut self) {
        if !self.consumed {
            unsafe { sys::ct_free_handle(self.ptr) };
        }
    }
}

/// Owning handle to a crowtree engine. Send + Sync: the C++ engine serializes
/// writes internally and keeps reads lock-free.
pub struct Crowtree {
    ptr: NonNull<sys::ct_tree>,
    // Lazily-spawned eventfd pump for this tree's Reactor eventfd
    //, shared by every concurrently-pending
    // drive_ct_future call. `None` once resolved means no Reactor is wired
    // (or the pump failed to spawn); see `eventfd_notify`/`EventfdPump`.
    eventfd_pump: std::sync::OnceLock<Option<EventfdPump>>,
}

unsafe impl Send for Crowtree {}
unsafe impl Sync for Crowtree {}

impl std::fmt::Debug for Crowtree {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Crowtree")
            .field("ptr", &self.ptr)
            .finish_non_exhaustive()
    }
}

impl Crowtree {
    /// Open (recovering durable state when `path` is set, else fresh in-memory).
    pub fn open(opt: &Options) -> Result<Self, CtError> {
        let cpath: Option<CString> = opt
            .path
            .as_ref()
            .map(|p| CString::new(p.as_str()).map_err(|_| CtError::InvalidArgument))
            .transpose()?;
        let clog_dir: Option<CString> = if opt.log_dir.is_empty() {
            None
        } else {
            Some(CString::new(opt.log_dir.as_str()).map_err(|_| CtError::InvalidArgument)?)
        };
        let clog_level: Option<CString> = if opt.log_level.is_empty() {
            None
        } else {
            Some(CString::new(opt.log_level.as_str()).map_err(|_| CtError::InvalidArgument)?)
        };
        let clog_prefix: Option<CString> = if opt.log_file_prefix.is_empty() {
            None
        } else {
            Some(CString::new(opt.log_file_prefix.as_str()).map_err(|_| CtError::InvalidArgument)?)
        };
        let copt = sys::ct_options {
            path: cpath.as_ref().map_or(std::ptr::null(), |c| c.as_ptr()),
            iu_size: opt.iu_size,
            frame_bytes: opt.frame_bytes,
            buffer_pool_bytes: opt.buffer_pool_bytes,
            compression: u8::from(opt.compression_lz4),
            max_inline_value: opt.max_inline_value,
            backend: match opt.backend {
                PageStoreBackend::File => 0,
                PageStoreBackend::Block => 1,
                PageStoreBackend::MemBlock => 2,
            },
            block_size: opt.block_size,
            store_id: opt.store_id,
            group_id: opt.group_id,
            sync_mode: opt.sync_mode.as_u8(),
            log_dir: clog_dir.as_ref().map_or(std::ptr::null(), |c| c.as_ptr()),
            log_level: clog_level.as_ref().map_or(std::ptr::null(), |c| c.as_ptr()),
            log_file_prefix: clog_prefix.as_ref().map_or(std::ptr::null(), |c| c.as_ptr()),
            log_max_file_mb: opt.log_max_file_mb,
            log_max_files: opt.log_max_files,
        };
        let mut out: *mut sys::ct_tree = std::ptr::null_mut();
        check(unsafe { sys::ct_open(&copt, &mut out) })?;
        Ok(Self {
            ptr: NonNull::new(out).ok_or(CtError::Internal)?,
            eventfd_pump: std::sync::OnceLock::new(),
        })
    }

    fn as_ptr(&self) -> *mut sys::ct_tree {
        self.ptr.as_ptr()
    }

    pub fn apply_put(&self, slot: u64, key: &[u8], value: &[u8]) -> Result<(), CtError> {
        check(unsafe {
            sys::ct_apply_put(
                self.as_ptr(),
                slot,
                key.as_ptr(),
                key.len(),
                value.as_ptr(),
                value.len(),
            )
        })
    }

    /// Allocate crowtree-owned memory for a zero-copy put (R3). Returns a
    /// [`WriteHandle`] whose `key_mut`/`value_mut` slices the caller writes
    /// into directly, then [`WriteHandle::apply`] consumes with zero value
    /// memcpy.
    pub fn alloc_put(&self, key_len: usize, val_len: usize) -> Result<WriteHandle, CtError> {
        let mut handle: *mut sys::ct_write_handle = std::ptr::null_mut();
        let mut ptrs = sys::ct_write_ptrs {
            key: std::ptr::null_mut(),
            val: std::ptr::null_mut(),
        };
        check(unsafe { sys::ct_alloc(self.as_ptr(), key_len, val_len, &mut handle, &mut ptrs) })?;
        Ok(WriteHandle {
            ptr: handle,
            tree: self.as_ptr(),
            key_ptr: ptrs.key,
            val_ptr: ptrs.val,
            key_len,
            val_len,
            consumed: false,
        })
    }

    pub fn apply_delete(&self, slot: u64, key: &[u8]) -> Result<(), CtError> {
        check(unsafe { sys::ct_apply_delete(self.as_ptr(), slot, key.as_ptr(), key.len()) })
    }

    /// Apply `ops` atomically at `slot` (one call into the C++ engine, so a
    /// concurrent reader never observes a partially-applied batch -- unlike
    /// looping [`Self::apply_put`]/[`Self::apply_delete`] per key).
    pub fn apply_batch(&self, slot: u64, ops: &[BatchOp<'_>]) -> Result<(), CtError> {
        let refs: Vec<sys::ct_kv_ref> = ops
            .iter()
            .map(|op| match op {
                BatchOp::Put { key, value } => sys::ct_kv_ref {
                    key: key.as_ptr(),
                    key_len: key.len(),
                    value: value.as_ptr(),
                    value_len: value.len(),
                    kind: 0,
                },
                BatchOp::Delete { key } => sys::ct_kv_ref {
                    key: key.as_ptr(),
                    key_len: key.len(),
                    value: std::ptr::null(),
                    value_len: 0,
                    kind: 1,
                },
            })
            .collect();
        check(unsafe { sys::ct_apply_batch_slices(self.as_ptr(), slot, refs.as_ptr(), refs.len() as u64) })
    }

    pub fn force_advance_slot(&self, slot: u64) {
        unsafe { sys::ct_force_advance_slot(self.as_ptr(), slot) }
    }

    /// Convenience: auto-assign the next slot and apply a put (single-writer only).
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<(), CtError> {
        check(unsafe {
            sys::ct_put(
                self.as_ptr(),
                key.as_ptr(),
                key.len(),
                value.as_ptr(),
                value.len(),
            )
        })
    }

    /// Convenience: auto-assign the next slot and apply a delete (single-writer only).
    pub fn del(&self, key: &[u8]) -> Result<(), CtError> {
        check(unsafe { sys::ct_del(self.as_ptr(), key.as_ptr(), key.len()) })
    }

    pub fn flush(&self) -> Result<(), CtError> {
        check(unsafe { sys::ct_flush(self.as_ptr()) })
    }

    pub fn snapshot(&self) -> Result<u64, CtError> {
        let mut last = 0u64;
        check(unsafe { sys::ct_snapshot(self.as_ptr(), &mut last) })?;
        Ok(last)
    }

    pub fn last_applied_slot(&self) -> u64 {
        unsafe { sys::ct_last_applied_slot(self.as_ptr()) }
    }

    /// Logical retention GC watermark: `gc_slot = min(snapshot_slot, safe_slot)`.
    /// See `crowtree::Crowtree::set_gc_watermark`.
    pub fn set_gc_watermark(&self, snapshot_slot: u64, safe_slot: u64) {
        unsafe { sys::ct_set_gc_watermark(self.as_ptr(), snapshot_slot, safe_slot) }
    }

    /// Explicit in-memory tombstone-retention sweep; does not persist. See
    /// `crowtree::Crowtree::collect_garbage`.
    pub fn collect_garbage(&self) -> Result<GcStats, CtError> {
        let mut stats = sys::ct_gc_stats {
            tombstones_dropped: 0,
            pages_freed: 0,
            bytes_freed: 0,
        };
        check(unsafe { sys::ct_collect_garbage(self.as_ptr(), &mut stats) })?;
        Ok(GcStats {
            tombstones_dropped: stats.tombstones_dropped,
            pages_freed: stats.pages_freed,
            bytes_freed: stats.bytes_freed,
        })
    }

    /// True if a demand-load hit an I/O error or CRC mismatch on a committed
    /// page (the offending read degraded to a miss). Latched until cleared.
    pub fn io_failed(&self) -> bool {
        unsafe { sys::ct_io_failed(self.as_ptr()) != 0 }
    }

    pub fn clear_io_error(&self) {
        unsafe { sys::ct_clear_io_error(self.as_ptr()) }
    }

    /// Wipe every key/value back to a fresh, empty tree (the same wipe
    /// `snapshot_import` performs before loading imported entries, exposed
    /// standalone for a caller with nothing to load afterward -- e.g.
    /// resetting a diverged/corrupted replica in place before a later
    /// snapshot import). Not durable by itself -- an explicit `snapshot`/
    /// `flush` afterward is required to persist the wipe to a file-backed
    /// store.
    pub fn clear(&self) -> Result<(), CtError> {
        check(unsafe { sys::ct_clear(self.as_ptr()) })
    }

    /// Batched diagnostics snapshot. O(1) -- safe to poll periodically for
    /// metrics/console display.
    pub fn stats(&self) -> Stats {
        let mut raw = sys::ct_stats::default();
        unsafe { sys::ct_get_stats(self.as_ptr(), &mut raw) };
        Stats {
            last_applied_slot: raw.last_applied_slot,
            contiguous_slot: raw.contiguous_slot,
            gc_watermark: raw.gc_watermark,
            io_failed: raw.io_failed != 0,
            snapshot_pages_written: raw.snapshot_pages_written,
            snapshot_pages_total: raw.snapshot_pages_total,
            snapshot_segments_written: raw.snapshot_segments_written,
            buffer_pool_hits: raw.buffer_pool_hits,
            buffer_pool_misses: raw.buffer_pool_misses,
            buffer_pool_evictions: raw.buffer_pool_evictions,
            buffer_pool_writebacks: raw.buffer_pool_writebacks,
            buffer_pool_resident: raw.buffer_pool_resident,
            buffer_pool_dirty: raw.buffer_pool_dirty,
            buffer_pool_used: raw.buffer_pool_used,
            buffer_pool_num_frames: raw.buffer_pool_num_frames,
            mt_upsert_total: raw.mt_upsert_total,
            mt_get_total: raw.mt_get_total,
            mt_get_hit_total: raw.mt_get_hit_total,
            flush_drain_total: raw.flush_drain_total,
            flush_entries_total: raw.flush_entries_total,
            snapshot_total: raw.snapshot_total,
            l1_get_total: raw.l1_get_total,
            l1_get_hit_total: raw.l1_get_hit_total,
        }
    }

    /// Flush C++ metrics into a formatted string for the `[cpp-metrics]`
    /// log section. `width` overrides per-section max name length for
    /// column alignment with the Rust section (0 = use C++ internal max).
    pub fn flush_metrics_str(&self, window_secs: f64, timestamp: &str, width: usize) -> String {
        let c_ts = CString::new(timestamp).unwrap_or_default();
        let ptr = unsafe { sys::ct_flush_metrics_str(self.as_ptr(), window_secs, c_ts.as_ptr(), width) };
        if ptr.is_null() {
            return String::new();
        }
        let result = unsafe { std::ffi::CStr::from_ptr(ptr).to_string_lossy().into_owned() };
        unsafe { sys::ct_free_string(ptr) };
        result
    }

    /// Extended flush with negotiated column widths (count_w, tps_w).
    pub fn flush_metrics_str_ext(
        &self,
        window_secs: f64,
        timestamp: &str,
        width: usize,
        count_w: usize,
        tps_w: usize,
    ) -> String {
        let c_ts = CString::new(timestamp).unwrap_or_default();
        let ptr = unsafe {
            sys::ct_flush_metrics_str_ext(self.as_ptr(), window_secs, c_ts.as_ptr(), width, count_w, tps_w)
        };
        if ptr.is_null() {
            return String::new();
        }
        let result = unsafe { std::ffi::CStr::from_ptr(ptr).to_string_lossy().into_owned() };
        unsafe { sys::ct_free_string(ptr) };
        result
    }

    /// Negotiate column widths: returns C++ preferred (count_w, tps_w).
    pub fn negotiate_widths(&self, rust_count_w: usize, rust_tps_w: usize) -> (usize, usize) {
        let input = sys::ct_column_widths {
            count_w: rust_count_w,
            tps_w: rust_tps_w,
        };
        let mut out = sys::ct_column_widths { count_w: 0, tps_w: 0 };
        unsafe { sys::ct_negotiate_widths(self.as_ptr(), input, &mut out) };
        (out.count_w, out.tps_w)
    }

    /// Current max metric name length from the C++ registry.
    pub fn max_name_len(&self) -> usize {
        unsafe { sys::ct_max_name_len(self.as_ptr()) }
    }

    /// Evict clean, delta-free resident leaf bases down to at most
    /// `max_resident_leaves`. Test/ops hook -- forces the
    /// demand-load path a subsequent `get`/`AsyncCrowtree::get` will have to
    /// take. Returns the number of leaves evicted.
    pub fn evict_clean_leaves(&self, max_resident_leaves: u64) -> u64 {
        unsafe { sys::ct_evict_clean_leaves(self.as_ptr(), max_resident_leaves) }
    }

    /// D3: same contract, but for resident *inner* bases --
    /// a genuinely separate ranked budget/pass from [`Self::evict_clean_leaves`],
    /// never evicting a leaf. Returns the number of inner bases evicted.
    pub fn evict_clean_inner(&self, max_resident_inner: u64) -> u64 {
        unsafe { sys::ct_evict_clean_inner(self.as_ptr(), max_resident_inner) }
    }

    /// The tree's Reactor eventfd, or -1 if none is wired (an
    /// in-memory tree, or a build without liburing). Reactor-owned -- see
    /// `RawFdView`'s doc comment for why callers must never close it.
    fn reactor_eventfd(&self) -> RawFd {
        unsafe { sys::ct_reactor_eventfd(self.as_ptr()) }
    }

    /// True when the tree was opened with a durable path and the build has
    /// an io_uring reactor wired. In-memory trees and non-Linux/liburing
    /// builds return `false`; `ct_get_async`/`ct_scan_async` then complete
    /// synchronously, so callers cannot observe a genuine `Pending` future.
    #[must_use]
    pub fn is_reactor_available(&self) -> bool {
        self.reactor_eventfd() >= 0
    }

    /// Lazily spawns (once) and returns the `Notify` fanned out by this
    /// tree's eventfd pump -- see the module-level fan-out note above
    /// `RawFdView` for why a single pump task, not a per-future
    /// registration, is required. `None` if there's no Reactor wired, or
    /// the pump failed to spawn. Must be called from within a Tokio runtime
    /// context; every call site is inside an async fn body driven by one,
    /// so this always holds.
    fn eventfd_notify(&self) -> Option<Arc<tokio::sync::Notify>> {
        self.eventfd_pump
            .get_or_init(|| {
                let raw_fd = self.reactor_eventfd();
                if raw_fd < 0 {
                    return None;
                }
                let async_fd = AsyncFd::new(RawFdView(raw_fd)).ok()?;
                let notify = Arc::new(tokio::sync::Notify::new());
                let pump_notify = notify.clone();
                let task = tokio::spawn(async move {
                    loop {
                        let mut guard = match async_fd.readable().await {
                            Ok(g) => g,
                            Err(_) => return, // I/O driver gone; stop pumping.
                        };
                        drain_eventfd(async_fd.as_raw_fd());
                        guard.clear_ready();
                        pump_notify.notify_waiters();
                    }
                })
                .abort_handle();
                Some(EventfdPump { notify, task })
            })
            .as_ref()
            .map(|pump| pump.notify.clone())
    }

    /// Point read. Returns `None` for absent / tombstoned keys.
    pub fn get(&self, key: &[u8]) -> Result<Option<(u64, Vec<u8>)>, CtError> {
        let mut found: c_int = 0;
        let mut slot = 0u64;
        let mut val = sys::ct_buf {
            data: std::ptr::null_mut(),
            len: 0,
        };
        check(unsafe {
            sys::ct_get(
                self.as_ptr(),
                key.as_ptr(),
                key.len(),
                &mut found,
                &mut slot,
                &mut val,
            )
        })?;
        let value = take_buf(val);
        if found == 0 {
            Ok(None)
        } else {
            Ok(Some((slot, value)))
        }
    }

    /// Range scan over `prefix` (empty = whole keyspace).
    /// When `include_tombstones` is true, tombstone entries are included.
    pub fn scan(
        &self,
        prefix: &[u8],
        limit: usize,
        include_tombstones: bool,
    ) -> Result<(Vec<ScanEntry>, bool), CtError> {
        let mut buf = sys::ct_buf {
            data: std::ptr::null_mut(),
            len: 0,
        };
        let mut count = 0u64;
        let mut truncated: c_int = 0;
        check(unsafe {
            sys::ct_scan(
                self.as_ptr(),
                prefix.as_ptr(),
                prefix.len(),
                limit,
                if include_tombstones { 1 } else { 0 },
                &mut buf,
                &mut count,
                &mut truncated,
            )
        })?;
        let bytes = take_buf(buf);
        let entries = decode_scan(&bytes, count as usize)?;
        Ok((entries, truncated != 0))
    }

    /// Materialize the durable snapshot view (key-sorted, includes tombstones).
    pub fn snapshot_view(&self) -> Result<(u64, Vec<ViewEntry>), CtError> {
        let mut view: *mut sys::ct_view = std::ptr::null_mut();
        check(unsafe { sys::ct_snapshot_view(self.as_ptr(), &mut view) })?;
        let at = unsafe { sys::ct_view_at_slot(view) };
        let mut it: *mut sys::ct_iter = std::ptr::null_mut();
        let rc = unsafe { sys::ct_view_iter(view, &mut it) };
        if rc != 0 {
            unsafe { sys::ct_view_release(view) };
            return Err(check(rc).unwrap_err());
        }
        let mut out = Vec::new();
        loop {
            let mut key = sys::ct_buf {
                data: std::ptr::null_mut(),
                len: 0,
            };
            let mut value = sys::ct_buf {
                data: std::ptr::null_mut(),
                len: 0,
            };
            let mut slot = 0u64;
            let mut kind = 0u8;
            let mut valid: c_int = 0;
            let rc = unsafe { sys::ct_iter_next(it, &mut key, &mut slot, &mut kind, &mut value, &mut valid) };
            if rc != 0 {
                unsafe {
                    sys::ct_iter_release(it);
                    sys::ct_view_release(view);
                }
                return Err(check(rc).unwrap_err());
            }
            let k = take_buf(key);
            let v = take_buf(value);
            if valid == 0 {
                break;
            }
            out.push(ViewEntry {
                key: k,
                slot,
                tombstone: kind == 1,
                value: v,
            });
        }
        unsafe {
            sys::ct_iter_release(it);
            sys::ct_view_release(view);
        }
        Ok((at, out))
    }

    /// Export the current durable snapshot as the portable byte stream
    /// (concatenated chunks). The snapshot's slot is carried in the stream.
    pub fn snapshot_export(&self) -> Result<Vec<u8>, CtError> {
        let mut exp: *mut sys::ct_export = std::ptr::null_mut();
        check(unsafe { sys::ct_snapshot_export_begin(self.as_ptr(), &mut exp) })?;
        let mut stream = Vec::new();
        loop {
            let mut chunk = sys::ct_buf {
                data: std::ptr::null_mut(),
                len: 0,
            };
            let mut done: c_int = 0;
            let rc = unsafe { sys::ct_snapshot_export_next(exp, &mut chunk, &mut done) };
            if rc != 0 {
                unsafe { sys::ct_snapshot_export_end(exp) };
                return Err(check(rc).unwrap_err());
            }
            stream.extend_from_slice(&take_buf(chunk));
            if done != 0 {
                break;
            }
        }
        unsafe { sys::ct_snapshot_export_end(exp) };
        Ok(stream)
    }

    /// Import a portable snapshot stream, replacing this engine's state.
    pub fn snapshot_import(&self, stream: &[u8]) -> Result<u64, CtError> {
        let mut im: *mut sys::ct_import = std::ptr::null_mut();
        check(unsafe { sys::ct_snapshot_import_begin(self.as_ptr(), &mut im) })?;
        let rc = unsafe { sys::ct_snapshot_import_feed(im, stream.as_ptr(), stream.len()) };
        if rc != 0 {
            unsafe { sys::ct_snapshot_import_end(im) };
            return Err(check(rc).unwrap_err());
        }
        let mut at = 0u64;
        let rc = unsafe { sys::ct_snapshot_import_finish(im, &mut at) };
        unsafe { sys::ct_snapshot_import_end(im) };
        check(rc)?;
        Ok(at)
    }
}

impl Drop for Crowtree {
    fn drop(&mut self) {
        // Stop the eventfd pump (if any) before ct_close tears down the
        // Reactor -- once that happens the eventfd is closed and the raw fd
        // number may be reused elsewhere in the process, so the pump must
        // not still be waiting on it.
        if let Some(Some(pump)) = self.eventfd_pump.get() {
            pump.task.abort();
        }
        unsafe { sys::ct_close(self.ptr.as_ptr()) }
    }
}

/// Initialize the C++ spdlog async file logger. Call this once at process
/// startup, before any `Crowtree::open`. All parameters map to the C++
/// `init_logging()` function. Safe to call when the build has no spdlog
/// (no-op).
///
/// - `log_dir` — directory for log files (empty => stderr)
/// - `level` — spdlog level name (trace/debug/info/warn/error/off)
/// - `max_file_mb` — max file size before rotation (0 => 30)
/// - `max_files` — max rotated files to keep (0 => 5)
/// - `file_prefix` — filename prefix (empty => "crowtree")
pub fn ct_init_logging(log_dir: &str, level: &str, max_file_mb: usize, max_files: usize, file_prefix: &str) {
    let log_dir_c = std::ffi::CString::new(log_dir).unwrap_or_default();
    let level_c = std::ffi::CString::new(level).unwrap_or_default();
    let prefix_c = std::ffi::CString::new(file_prefix).unwrap_or_default();
    unsafe {
        sys::ct_init_logging(
            log_dir_c.as_ptr(),
            level_c.as_ptr(),
            max_file_mb,
            max_files,
            prefix_c.as_ptr(),
        );
    }
}

/// Flush buffered C++ log messages to disk without stopping the logger.
/// Safe to call when logging was never initialized (no-op).
pub fn ct_flush_logging() {
    unsafe { sys::ct_flush_logging() };
}

/// Flush and stop the C++ spdlog async logger. Call this during process
/// shutdown (after all `Crowtree` instances are dropped) to ensure
/// buffered log messages are written to disk. Safe to call when logging
/// was never initialized (no-op in that case).
pub fn ct_shutdown_logging() {
    unsafe { sys::ct_shutdown_logging() };
}

fn decode_scan(bytes: &[u8], count: usize) -> Result<Vec<ScanEntry>, CtError> {
    let mut out = Vec::with_capacity(count);
    let mut pos = 0usize;
    let rd_u32 = |b: &[u8], p: usize| -> u32 { u32::from_le_bytes([b[p], b[p + 1], b[p + 2], b[p + 3]]) };
    let rd_u64 = |b: &[u8], p: usize| -> u64 {
        let mut a = [0u8; 8];
        a.copy_from_slice(&b[p..p + 8]);
        u64::from_le_bytes(a)
    };
    for _ in 0..count {
        if pos + 4 > bytes.len() {
            return Err(CtError::Corruption);
        }
        let klen = rd_u32(bytes, pos) as usize;
        pos += 4;
        if pos + klen + 13 > bytes.len() {
            return Err(CtError::Corruption);
        }
        let key = bytes[pos..pos + klen].to_vec();
        pos += klen;
        let slot = rd_u64(bytes, pos);
        pos += 8;
        let tombstone = bytes[pos] != 0;
        pos += 1;
        let vlen = rd_u32(bytes, pos) as usize;
        pos += 4;
        if pos + vlen > bytes.len() {
            return Err(CtError::Corruption);
        }
        let value = bytes[pos..pos + vlen].to_vec();
        pos += vlen;
        out.push(ScanEntry {
            key,
            slot,
            value,
            tombstone,
        });
    }
    Ok(out)
}

// ── Reactor-driven async futures ───────────
//
// AsyncCrowtree::get/flush/snapshot drive drive_ct_future below directly:
// no spawn_blocking, no OS thread hop. A fast-path completion (get_view's
// cached L0/L1 hit, or flush's always-in-memory work) resolves on the
// *first* poll without ever touching the reactor; only a genuine
// demand-load miss waits on the tree's eventfd.
//
// Fan-out note: this deliberately does *not* have every pending future call
// `AsyncFd::poll_read_ready` on a shared registration -- that method's
// single reserved waker slot only keeps the *most recently polling* task's
// waker (tokio's own doc comment on `poll_read_ready`), so N concurrently
// pending gets would silently starve all but the last one to (re)poll,
// hanging forever. Only one task -- a lazily-spawned pump owned by
// `Crowtree` -- ever touches the eventfd's `AsyncFd`; every other future
// waits on a `tokio::sync::Notify` the pump fans out to instead, which does
// support any number of concurrent waiters.

/// Non-owning view of a raw fd for `AsyncFd` registration. The engine's
/// `Reactor` owns the eventfd `ct_reactor_eventfd` returns and closes it in
/// its own destructor (~`Reactor`); Rust must wrap it *without* taking
/// closing ownership -- unlike `std::os::fd::OwnedFd`, this type's `Drop`
/// is a no-op.
struct RawFdView(RawFd);

impl AsRawFd for RawFdView {
    fn as_raw_fd(&self) -> RawFd {
        self.0
    }
}

extern "C" {
    // libc read(2) -- used only to drain the reactor's eventfd counter
    // below, nothing to do with the ct_* C ABI.
    fn read(fd: c_int, buf: *mut u8, count: usize) -> isize;
}

/// Best-effort drain of the eventfd's accumulated counter back to 0. The
/// Reactor (`reactor.cpp`) never reads it -- draining is left entirely to
/// this side, deliberately, so that a later write (a 0 -> nonzero
/// transition) reliably produces a fresh edge for Tokio's I/O driver to
/// wake on; without draining, a still-nonzero counter can leave a second
/// completion's wakeup silently lost.
fn drain_eventfd(fd: RawFd) {
    let mut buf = [0u8; 8];
    unsafe {
        read(fd, buf.as_mut_ptr(), buf.len());
    }
}

/// The tree's lazily-spawned eventfd pump (see the module-level fan-out
/// note above): `notify` is fanned out to every waiting `drive_ct_future`
/// call each time the pump observes the eventfd fire; `task` is aborted by
/// `Crowtree`'s `Drop` before `ct_close` runs (the eventfd itself becomes
/// invalid once the Reactor is torn down).
struct EventfdPump {
    notify: Arc<tokio::sync::Notify>,
    task: tokio::task::AbortHandle,
}

/// Decoded (but not yet interpreted) result of one completed `ct_future`.
struct RawOutcome {
    found: bool,
    slot: u64,
    value: Vec<u8>,
}

/// Which `ct_*_async` call produced a `FutureGuard` -- `ct_future_poll`'s
/// freeing contract differs for `Get`:
/// a resolved kGet future is deliberately *not* freed by `ct_future_poll`
/// itself (its `out_value` may still borrow from a resident frame, kept
/// alive by the future's own epoch guard), so the caller must free it
/// explicitly once done reading -- unlike Flush/Snapshot/Scan, which
/// `ct_future_poll` already frees on completion, same as before Phase 4.
/// `Scan`'s `out_value` (follow-up) is always a *malloc'd*
/// owned buffer (never borrowed, unlike Get) -- `try_poll_ct_future` must
/// free it via `take_buf`, not `copy_buf`.
#[derive(Clone, Copy, PartialEq, Eq)]
enum FutureKind {
    Get,
    Flush,
    Snapshot,
    Scan,
}

/// RAII guard for one in-flight `ct_future`: frees it via `ct_future_free`
/// if dropped before completion (task cancellation while `.await`ing
/// `drive_ct_future` below). Runs correctly even mid-`.await`: async fn
/// locals still in scope at a suspension point are dropped normally when
/// the generated future itself is dropped.
struct FutureGuard(*mut sys::ct_future);

impl Drop for FutureGuard {
    fn drop(&mut self) {
        if !self.0.is_null() {
            unsafe { sys::ct_future_free(self.0) };
        }
    }
}

// SAFETY: *mut ct_future is an opaque handle the C++ side documents as
// freely movable/pollable from any thread; this narrow impl is
// what lets drive_ct_future's generated future (which holds a FutureGuard
// across its `.await` points) be Send, without having to bless the
// non-Send raw ct_buf pointer that only ever lives inside the fully
// synchronous try_poll_ct_future below (never held across a suspension
// point).
unsafe impl Send for FutureGuard {}

/// One synchronous `ct_future_poll` attempt. `None` if still pending;
/// `Some` if done, in which case `guard` has already been nulled out --
/// either `ct_future_poll` itself freed the underlying `ct_future`
/// (Flush/Snapshot), or, for a `Get`, this function did so explicitly via
/// `ct_future_free` right after copying `out_value`'s bytes out (which may
/// be a *borrowed* pointer into a still-live frame -- see `copy_buf`).
///
/// Deliberately synchronous and free of any `.await`: `sys::ct_buf` holds a
/// raw `*mut u8` which is not `Send`, so it must never be a value held
/// across a suspension point in `drive_ct_future`'s generated future.
fn try_poll_ct_future(guard: &mut FutureGuard, kind: FutureKind) -> Option<Result<RawOutcome, CtError>> {
    let mut done: c_int = 0;
    let mut found: c_int = 0;
    let mut slot: u64 = 0;
    let mut value = sys::ct_buf {
        data: std::ptr::null_mut(),
        len: 0,
    };
    let rc = unsafe { sys::ct_future_poll(guard.0, &mut done, &mut found, &mut slot, &mut value) };
    if done == 0 {
        return None;
    }
    // Extracted unconditionally, before checking status: for Scan, `value`
    // is always a malloc'd owned buffer (see FutureKind::Scan's doc
    // comment) that must be freed via take_buf/ct_free_buf regardless of
    // whether the underlying op errored (ct_future_poll's kScan branch
    // populates *out_value with an owned buffer -- possibly empty, but
    // still malloc'd -- either way; leaving `value` unhandled inside a
    // match arm that only runs on Ok would leak it on an errored scan).
    let value_bytes = if kind == FutureKind::Scan {
        take_buf(value)
    } else {
        // copy_buf, not take_buf: for a Get, `value` may borrow from a
        // still-live frame (zero-copy fast path) and must
        // not be passed to ct_free_buf. Flush/Snapshot never populate
        // `value` at all, so this is a no-op for them either way.
        copy_buf(value)
    };
    let result = match check(rc) {
        Ok(()) => Ok(RawOutcome {
            found: found != 0,
            slot,
            value: value_bytes,
        }),
        Err(e) => Err(e),
    };
    if kind == FutureKind::Get {
        // ct_future_poll deliberately does *not* free a kGet future (see
        // its doc comment in c_api.h) -- the epoch guard behind a
        // zero-copy fast-path value must outlive the copy_buf call above.
        unsafe { sys::ct_future_free(guard.0) };
    }
    // Flush/Snapshot: already freed by ct_future_poll itself. Either way,
    // the underlying ct_future is gone now -- don't let FutureGuard's Drop
    // free it again.
    guard.0 = std::ptr::null_mut();
    Some(result)
}

/// Like [`try_poll_ct_future`] for `FutureKind::Get`, but instead of
/// copying the value bytes out and freeing the future, returns a
/// [`PinnedValue`] that borrows directly from the C++ frame. The
/// `ct_future` handle is transferred into the `PinnedValue` (its `Drop`
/// calls `ct_future_free`), so `guard.0` is nulled to prevent
/// `FutureGuard`'s `Drop` from double-freeing.
fn try_poll_ct_future_pinned(guard: &mut FutureGuard) -> Option<Result<Option<(u64, PinnedValue)>, CtError>> {
    let mut done: c_int = 0;
    let mut found: c_int = 0;
    let mut slot: u64 = 0;
    let mut value = sys::ct_buf {
        data: std::ptr::null_mut(),
        len: 0,
    };
    let rc = unsafe { sys::ct_future_poll(guard.0, &mut done, &mut found, &mut slot, &mut value) };
    if done == 0 {
        return None;
    }
    let result = match check(rc) {
        Ok(()) => {
            if found != 0 {
                let pv = PinnedValue {
                    handle: guard.0,
                    data: value.data,
                    len: value.len,
                };
                Ok(Some((slot, pv)))
            } else {
                // Not found: still need to free the future.
                unsafe { sys::ct_future_free(guard.0) };
                Ok(None)
            }
        }
        Err(e) => {
            unsafe { sys::ct_future_free(guard.0) };
            Err(e)
        }
    };
    guard.0 = std::ptr::null_mut();
    Some(result)
}

/// Drives one `ct_get_async`/`ct_flush_async`/`ct_snapshot_async` handle to
/// completion: polls it, and if not yet done, waits for the tree's eventfd
/// pump to fan out a notification before polling again. A fast-path
/// completion never reaches the `notified.await` at all.
async fn drive_ct_future(
    mut guard: FutureGuard,
    tree: &Arc<Crowtree>,
    kind: FutureKind,
) -> Result<RawOutcome, CtError> {
    loop {
        // Construct (but do not yet await) the notification future *before*
        // checking ct_future_poll below, not after: Notify::notified
        // captures the pump's current notify_waiters call count at
        // construction time and is guaranteed to fire for any
        // notify_waiters after that point even before this is polled --
        // constructing it only *after* seeing done=0 would leave a window
        // where a completion + notify racing in right there is silently
        // missed, hanging until some unrelated later notification (or
        // forever, if none ever comes).
        let notify_arc = tree.eventfd_notify();
        let notified = notify_arc.as_ref().map(|n| n.notified());

        if let Some(result) = try_poll_ct_future(&mut guard, kind) {
            return result;
        }

        match notified {
            Some(n) => n.await,
            None => {
                // No reactor wired (or the pump failed to spawn): per
                // ct_get_async/ct_flush_async/ct_snapshot_async's contract,
                // no reactor means every op already completes
                // synchronously, so done=0 here should be unreachable --
                // yield instead of busy-looping just in case.
                tokio::task::yield_now().await;
            }
        }
    }
}

/// Async facade. `get`/`flush`/`snapshot` drive the engine's io_uring
/// reactor directly via [`drive_ct_future`] -- no thread pool hop. The
/// remaining methods have no async C API twin yet (Phase 2 scoped only
/// get/flush/snapshot) and still bridge onto Tokio via `spawn_blocking`. Cheap to clone (shares
/// one `Arc<Crowtree>`).
#[derive(Clone, Debug)]
pub struct AsyncCrowtree {
    inner: Arc<Crowtree>,
}

impl AsyncCrowtree {
    pub fn open(opt: &Options) -> Result<Self, CtError> {
        Ok(Self {
            inner: Arc::new(Crowtree::open(opt)?),
        })
    }

    pub fn from_sync(tree: Crowtree) -> Self {
        Self {
            inner: Arc::new(tree),
        }
    }

    pub fn handle(&self) -> Arc<Crowtree> {
        Arc::clone(&self.inner)
    }

    pub async fn apply_put(&self, slot: u64, key: Vec<u8>, value: Vec<u8>) -> Result<(), CtError> {
        let t = self.inner.clone();
        tokio::task::spawn_blocking(move || t.apply_put(slot, &key, &value))
            .await
            .map_err(|_| CtError::Internal)?
    }

    pub async fn apply_delete(&self, slot: u64, key: Vec<u8>) -> Result<(), CtError> {
        let t = self.inner.clone();
        tokio::task::spawn_blocking(move || t.apply_delete(slot, &key))
            .await
            .map_err(|_| CtError::Internal)?
    }

    /// Convenience: auto-assign the next slot and apply a put (single-writer only).
    pub async fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), CtError> {
        let t = self.inner.clone();
        tokio::task::spawn_blocking(move || t.put(&key, &value))
            .await
            .map_err(|_| CtError::Internal)?
    }

    /// Convenience: auto-assign the next slot and apply a delete (single-writer only).
    pub async fn del(&self, key: Vec<u8>) -> Result<(), CtError> {
        let t = self.inner.clone();
        tokio::task::spawn_blocking(move || t.del(&key))
            .await
            .map_err(|_| CtError::Internal)?
    }

    /// Drives the engine's io_uring reactor directly (Phase
    /// 3) -- no `spawn_blocking`, since flush never touches the page
    /// store (only the in-memory L1), this always resolves on the very
    /// first poll.
    pub async fn flush(&self) -> Result<(), CtError> {
        let fut = unsafe { sys::ct_flush_async(self.inner.as_ptr()) };
        drive_ct_future(FutureGuard(fut), &self.inner, FutureKind::Flush).await?;
        Ok(())
    }

    /// Drives the engine's io_uring reactor directly (Phase
    /// 3) -- no `spawn_blocking`; the write phase always waits on the
    /// reactor, unlike `flush`/the fast `get` path.
    pub async fn snapshot(&self) -> Result<u64, CtError> {
        let fut = unsafe { sys::ct_snapshot_async(self.inner.as_ptr()) };
        Ok(
            drive_ct_future(FutureGuard(fut), &self.inner, FutureKind::Snapshot)
                .await?
                .slot,
        )
    }

    /// Drives the engine's io_uring reactor directly (Phase
    /// 3) -- no `spawn_blocking`. Resolves on the very first poll for a
    /// resident hit (`get_view`'s existing fast path, `#5 B3`); only a
    /// genuine demand-load miss waits on the reactor.
    pub async fn get(&self, key: Vec<u8>) -> Result<Option<(u64, Vec<u8>)>, CtError> {
        let fut = unsafe { sys::ct_get_async(self.inner.as_ptr(), key.as_ptr(), key.len()) };
        let out = drive_ct_future(FutureGuard(fut), &self.inner, FutureKind::Get).await?;
        Ok(out.found.then_some((out.slot, out.value)))
    }

    /// Like [`Self::get`], but never allocates or boxes anything for the
    /// fast (resident hit/miss) path -- only the genuine demand-load-miss
    /// path (`GetOutcome::Pending`) does. Lets a caller with its own
    /// fast-path/slow-path return type (`crowkv`'s `KVFuture`,     /// #11 Phase 6) mirror `ct_get_async`'s own C++-layer split one layer
    /// up, instead of collapsing it back into a uniform `async fn` (which
    /// would force boxing on every call, fast path included -- exactly what
    /// `KVFuture::Ready` exists to avoid).
    pub fn try_get(&self, key: &[u8]) -> GetOutcome {
        let fut = unsafe { sys::ct_get_async(self.inner.as_ptr(), key.as_ptr(), key.len()) };
        let mut guard = FutureGuard(fut);
        if let Some(result) = try_poll_ct_future(&mut guard, FutureKind::Get) {
            return GetOutcome::Ready(result.map(|out| out.found.then_some((out.slot, out.value))));
        }
        let tree = self.inner.clone();
        GetOutcome::Pending(Box::pin(async move {
            let out = drive_ct_future(guard, &tree, FutureKind::Get).await?;
            Ok(out.found.then_some((out.slot, out.value)))
        }))
    }

    /// Like [`Self::try_get`] but the fast path returns a [`PinnedValue`]
    /// borrowing directly from the C++ engine's internal buffer (no
    /// `copy_buf` allocation). The slow path is identical to
    /// [`Self::try_get`]'s (`PinnedGetOutcome::Pending` resolves to an
    /// owned `Vec<u8>`).
    pub fn try_get_pinned(&self, key: &[u8]) -> PinnedGetOutcome {
        let fut = unsafe { sys::ct_get_async(self.inner.as_ptr(), key.as_ptr(), key.len()) };
        let mut guard = FutureGuard(fut);
        if let Some(result) = try_poll_ct_future_pinned(&mut guard) {
            return PinnedGetOutcome::Ready(result);
        }
        let tree = self.inner.clone();
        PinnedGetOutcome::Pending(Box::pin(async move {
            let out = drive_ct_future(guard, &tree, FutureKind::Get).await?;
            Ok(out.found.then_some((out.slot, out.value)))
        }))
    }

    /// Async twin of [`Crowtree::scan`] (follow-up). Drives
    /// the reactor directly like `get`/`flush`/`snapshot` -- resolves on the
    /// first poll whenever every leaf in range is already resident
    /// (`scan`'s own fast path), only waiting on the reactor for a
    /// genuine cold leaf (or the initial root->leaf descent). See
    /// `Crowtree::scan_async`'s doc comment (crowtree.h) for why a miss
    /// retries the whole scan rather than resuming a cursor.
    pub async fn scan(&self, prefix: Vec<u8>, limit: usize) -> Result<(Vec<ScanEntry>, bool), CtError> {
        let fut = unsafe { sys::ct_scan_async(self.inner.as_ptr(), prefix.as_ptr(), prefix.len(), limit) };
        let out = drive_ct_future(FutureGuard(fut), &self.inner, FutureKind::Scan).await?;
        let entries = decode_scan(&out.value, out.slot as usize)?;
        Ok((entries, out.found))
    }

    /// Like [`Self::try_get`], but for [`Self::scan`]: never allocates or
    /// boxes anything for the fast (all-leaves-resident) path -- only a
    /// genuine cold-leaf miss (`ScanOutcome::Pending`) does. Same
    /// motivation as `try_get`'s doc comment: lets a caller with its own
    /// fast-path/slow-path return type mirror `ct_scan_async`'s own
    /// C++-layer split one layer up instead of forcing a box on every call.
    pub fn try_scan(&self, prefix: Vec<u8>, limit: usize) -> ScanOutcome {
        let fut = unsafe { sys::ct_scan_async(self.inner.as_ptr(), prefix.as_ptr(), prefix.len(), limit) };
        let mut guard = FutureGuard(fut);
        if let Some(result) = try_poll_ct_future(&mut guard, FutureKind::Scan) {
            return ScanOutcome::Ready(result.and_then(|out| {
                let entries = decode_scan(&out.value, out.slot as usize)?;
                Ok((entries, out.found))
            }));
        }
        let tree = self.inner.clone();
        ScanOutcome::Pending(Box::pin(async move {
            let out = drive_ct_future(guard, &tree, FutureKind::Scan).await?;
            let entries = decode_scan(&out.value, out.slot as usize)?;
            Ok((entries, out.found))
        }))
    }
}

/// Result of [`AsyncCrowtree::try_get`] -- see its doc comment for why this
/// exists instead of a single uniform `async fn`.
#[allow(clippy::type_complexity)]
pub enum GetOutcome {
    /// Resolved on the very first (and only) poll attempt -- no allocation.
    Ready(Result<Option<(u64, Vec<u8>)>, CtError>),
    /// A genuine demand-load miss, already registered with the reactor
    /// (or, absent one, that will complete synchronously on the next poll
    /// regardless -- `.await` this to completion.
    Pending(Pin<Box<dyn Future<Output = Result<Option<(u64, Vec<u8>)>, CtError>> + Send>>),
}

/// Result of [`AsyncCrowtree::try_scan`] -- see its doc comment for why
/// this exists instead of a single uniform `async fn`.
#[allow(clippy::type_complexity)]
pub enum ScanOutcome {
    /// Resolved on the very first (and only) poll attempt -- no allocation
    /// beyond the returned entries themselves.
    Ready(Result<(Vec<ScanEntry>, bool), CtError>),
    /// A genuine cold-leaf miss, already registered with the reactor (or,
    /// absent one, that will complete synchronously on the next poll
    /// regardless -- `.await` this to completion.
    Pending(Pin<Box<dyn Future<Output = Result<(Vec<ScanEntry>, bool), CtError>> + Send>>),
}

/// Zero-copy borrowed value from a `ct_get_async` completion. Holds the
/// `ct_future` handle so the C++ page refcount (R6) keeping the frame
/// resident stays alive until this value is dropped. `Send` because the
/// per-page refcount is thread-independent (R6: pin/unpin from any thread).
pub struct PinnedValue {
    handle: *mut sys::ct_future,
    data: *const u8,
    len: usize,
}

// R6: PinnedValue is Send — the C++ page refcount (pin_state_ on PageBase)
// is a thread-independent atomic. ct_future_free unpins from the dropping
// thread. SAFETY: the handle is a unique pointer to a heap-allocated
// ct_future_impl; no shared mutable state across threads except the
// refcount atomics, which are designed for cross-thread access.
unsafe impl Send for PinnedValue {}

impl PinnedValue {
    /// Borrow the value bytes directly from the C++ engine's internal
    /// buffer. Valid until `self` is dropped.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        if self.data.is_null() || self.len == 0 {
            &[]
        } else {
            unsafe { std::slice::from_raw_parts(self.data, self.len) }
        }
    }
}

impl Drop for PinnedValue {
    fn drop(&mut self) {
        if !self.handle.is_null() {
            unsafe { sys::ct_future_free(self.handle) };
        }
    }
}

/// Result of [`AsyncCrowtree::try_get_pinned`] -- like [`GetOutcome`] but
/// the fast path returns a [`PinnedValue`] (zero-copy borrow from the C++
/// frame) instead of an owned `Vec<u8>`. The slow path is identical to
/// [`GetOutcome::Pending`]: the value is always owned (copied by
/// `materialize_owned` on the reactor thread).
#[allow(clippy::type_complexity)]
pub enum PinnedGetOutcome {
    /// Fast path (resident hit/miss) -- zero-copy borrow, no `copy_buf`.
    Ready(Result<Option<(u64, PinnedValue)>, CtError>),
    /// Slow path (demand-load miss) -- resolves to an owned `Vec<u8>`,
    /// same as [`GetOutcome::Pending`].
    Pending(Pin<Box<dyn Future<Output = Result<Option<(u64, Vec<u8>)>, CtError>> + Send>>),
}
