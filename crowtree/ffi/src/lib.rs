//! Safe Rust adapter over the crowtree C ABI (`c_api.h`, PT8).
//!
//! Wraps the opaque `ct_*` handles in RAII types, translates owned `ct_buf`
//! buffers into `Vec<u8>` (freeing them via `ct_free_buf`), maps `ct_status`
//! into `Result`, and offers an async facade that bridges the still-synchronous
//! engine onto Tokio via `spawn_blocking`.

use std::ffi::CString;
use std::os::raw::{c_char, c_int};
use std::ptr::NonNull;
use std::sync::Arc;

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
    pub struct ct_import {
        _private: [u8; 0],
    }

    #[repr(C)]
    pub struct ct_buf {
        pub data: *mut u8,
        pub len: usize,
    }

    #[repr(C)]
    pub struct ct_gc_stats {
        pub tombstones_dropped: u64,
        pub pages_freed: u64,
        pub bytes_freed: u64,
    }

    #[repr(C)]
    pub struct ct_options {
        pub path: *const c_char,
        pub iu_size: u32,
        pub frame_bytes: u32,
        pub buffer_pool_bytes: u64,
        pub compression: u8,
        pub max_inline_value: u64,
    }

    extern "C" {
        pub fn ct_free_buf(buf: *mut ct_buf);
        pub fn ct_open(opt: *const ct_options, out: *mut *mut ct_tree) -> c_int;
        pub fn ct_close(t: *mut ct_tree);
        pub fn ct_snapshot(t: *mut ct_tree, out_last_applied: *mut u64) -> c_int;
        pub fn ct_last_applied_slot(t: *const ct_tree) -> u64;
        pub fn ct_set_gc_watermark(t: *mut ct_tree, snapshot_slot: u64, safe_slot: u64);
        pub fn ct_collect_garbage(t: *mut ct_tree, out_stats: *mut ct_gc_stats) -> c_int;
        pub fn ct_io_failed(t: *const ct_tree) -> c_int;
        pub fn ct_clear_io_error(t: *mut ct_tree);
        pub fn ct_apply_put(
            t: *mut ct_tree,
            slot: u64,
            key: *const u8,
            klen: usize,
            val: *const u8,
            vlen: usize,
        ) -> c_int;
        pub fn ct_apply_delete(t: *mut ct_tree, slot: u64, key: *const u8, klen: usize) -> c_int;
        pub fn ct_apply_batch(
            t: *mut ct_tree,
            slot: u64,
            ops: *const u8,
            ops_len: usize,
            count: u64,
        ) -> c_int;
        pub fn ct_force_advance_slot(t: *mut ct_tree, slot: u64);
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

/// Compression selector.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    None,
    Lz4,
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

/// Encode `ops` into `ct_apply_batch`'s packed wire format:
/// `[u8 kind][u32 klen][key][u32 vlen][value] * count` (kind 0=put, 1=delete).
fn encode_batch(ops: &[BatchOp<'_>]) -> Vec<u8> {
    let mut out = Vec::new();
    for op in ops {
        let (kind, key, value): (u8, &[u8], &[u8]) = match op {
            BatchOp::Put { key, value } => (0, key, value),
            BatchOp::Delete { key } => (1, key, b""),
        };
        out.push(kind);
        out.extend_from_slice(&(key.len() as u32).to_le_bytes());
        out.extend_from_slice(key);
        out.extend_from_slice(&(value.len() as u32).to_le_bytes());
        out.extend_from_slice(value);
    }
    out
}

/// A scan result entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanEntry {
    pub key: Vec<u8>,
    pub slot: u64,
    pub value: Vec<u8>,
}

/// A snapshot-view entry (includes tombstones).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ViewEntry {
    pub key: Vec<u8>,
    pub slot: u64,
    pub tombstone: bool,
    pub value: Vec<u8>,
}

/// Owning handle to a crowtree engine. Send + Sync: the C++ engine serializes
/// writes internally and keeps reads lock-free.
pub struct Crowtree {
    ptr: NonNull<sys::ct_tree>,
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
        let copt = sys::ct_options {
            path: cpath.as_ref().map_or(std::ptr::null(), |c| c.as_ptr()),
            iu_size: opt.iu_size,
            frame_bytes: opt.frame_bytes,
            buffer_pool_bytes: opt.buffer_pool_bytes,
            compression: u8::from(opt.compression_lz4),
            max_inline_value: opt.max_inline_value,
        };
        let mut out: *mut sys::ct_tree = std::ptr::null_mut();
        check(unsafe { sys::ct_open(&copt, &mut out) })?;
        Ok(Self {
            ptr: NonNull::new(out).ok_or(CtError::Internal)?,
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

    pub fn apply_delete(&self, slot: u64, key: &[u8]) -> Result<(), CtError> {
        check(unsafe { sys::ct_apply_delete(self.as_ptr(), slot, key.as_ptr(), key.len()) })
    }

    /// Apply `ops` atomically at `slot` (one call into the C++ engine, so a
    /// concurrent reader never observes a partially-applied batch -- unlike
    /// looping [`Self::apply_put`]/[`Self::apply_delete`] per key).
    pub fn apply_batch(&self, slot: u64, ops: &[BatchOp<'_>]) -> Result<(), CtError> {
        let packed = encode_batch(ops);
        check(unsafe {
            sys::ct_apply_batch(
                self.as_ptr(),
                slot,
                packed.as_ptr(),
                packed.len(),
                ops.len() as u64,
            )
        })
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
    pub fn scan(&self, prefix: &[u8], limit: usize) -> Result<(Vec<ScanEntry>, bool), CtError> {
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
        unsafe { sys::ct_close(self.ptr.as_ptr()) }
    }
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
        if pos + klen + 12 > bytes.len() {
            return Err(CtError::Corruption);
        }
        let key = bytes[pos..pos + klen].to_vec();
        pos += klen;
        let slot = rd_u64(bytes, pos);
        pos += 8;
        let vlen = rd_u32(bytes, pos) as usize;
        pos += 4;
        if pos + vlen > bytes.len() {
            return Err(CtError::Corruption);
        }
        let value = bytes[pos..pos + vlen].to_vec();
        pos += vlen;
        out.push(ScanEntry { key, slot, value });
    }
    Ok(out)
}

/// Async facade: bridges the synchronous engine onto Tokio via `spawn_blocking`.
/// Cheap to clone (shares one `Arc<Crowtree>`).
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

    pub async fn flush(&self) -> Result<(), CtError> {
        let t = self.inner.clone();
        tokio::task::spawn_blocking(move || t.flush())
            .await
            .map_err(|_| CtError::Internal)?
    }

    pub async fn snapshot(&self) -> Result<u64, CtError> {
        let t = self.inner.clone();
        tokio::task::spawn_blocking(move || t.snapshot())
            .await
            .map_err(|_| CtError::Internal)?
    }

    pub async fn get(&self, key: Vec<u8>) -> Result<Option<(u64, Vec<u8>)>, CtError> {
        let t = self.inner.clone();
        tokio::task::spawn_blocking(move || t.get(&key))
            .await
            .map_err(|_| CtError::Internal)?
    }
}
