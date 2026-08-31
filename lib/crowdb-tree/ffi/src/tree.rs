// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::ffi::CString;
use std::os::fd::{AsRawFd, RawFd};
use std::os::raw::c_int;
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::Once;

use tokio::io::unix::AsyncFd;

use crate::error::{check, take_buf, CtError};
use crate::options::{Options, PageStoreBackend};
use crate::reactor::{drain_eventfd, EventfdPump, RawFdView};
use crate::sys;

/// Owning handle to a crowdb-tree engine. Send + Sync: the C++ engine serializes
/// writes internally and keeps reads lock-free.
pub struct Crowdbtree {
    ptr: NonNull<sys::ct_tree>,
    // Lazily-spawned eventfd pumps for this tree's DiskIOUring eventfds
    // (one per pipeline), shared by every concurrently-pending
    // drive_ct_future call. Empty once resolved means no DiskIOUring is
    // wired (or the pumps failed to spawn); see `eventfd_notify`/`EventfdPump`.
    eventfd_pumps: std::sync::OnceLock<Vec<EventfdPump>>,
}

unsafe impl Send for Crowdbtree {}
unsafe impl Sync for Crowdbtree {}

impl std::fmt::Debug for Crowdbtree {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Crowdbtree")
            .field("ptr", &self.ptr)
            .finish_non_exhaustive()
    }
}

impl Crowdbtree {
    /// Open (recovering durable state when `path` is set, else fresh in-memory).
    pub fn open(opt: &Options) -> Result<Self, CtError> {
        #[cfg(feature = "test-util")]
        ct_init_test_logging();

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
            eventfd_pumps: std::sync::OnceLock::new(),
        })
    }

    pub(crate) fn as_ptr(&self) -> *mut sys::ct_tree {
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

    pub fn last_applied_slot(&self) -> u64 {
        unsafe { sys::ct_last_applied_slot(self.as_ptr()) }
    }

    /// Logical retention GC watermark: `gc_slot = min(snapshot_slot, safe_slot)`.
    /// See `crow::tree::Crowdbtree::set_gc_watermark`.
    pub fn set_gc_watermark(&self, snapshot_slot: u64, safe_slot: u64) {
        unsafe { sys::ct_set_gc_watermark(self.as_ptr(), snapshot_slot, safe_slot) }
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

    /// Flush C++ metrics into a formatted string for the `[cpp-tree]`
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
    /// demand-load path a subsequent `get`/`AsyncCrowdbtree::get` will have to
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

    /// The tree's DiskIOUring eventfds (one per pipeline), or empty if
    /// none is wired (an in-memory tree, or a build without liburing).
    /// DiskIOUring-owned -- see `RawFdView`'s doc comment for why callers
    /// must never close them.
    fn uring_eventfds(&self) -> Vec<RawFd> {
        // Query the count first, then fetch the fds.
        let count = unsafe { sys::ct_uring_eventfds(self.as_ptr(), std::ptr::null_mut(), 0) };
        if count == 0 {
            return Vec::new();
        }
        let mut fds = vec![0i32; count];
        let got = unsafe { sys::ct_uring_eventfds(self.as_ptr(), fds.as_mut_ptr(), count) };
        fds.truncate(got);
        fds
    }

    /// True when the tree was opened with a durable path and the build has
    /// an io_uring DiskIOUring wired. In-memory trees and non-Linux/liburing
    /// builds return `false`; `ct_get_async`/`ct_scan_async` then complete
    /// synchronously, so callers cannot observe a genuine `Pending` future.
    #[must_use]
    pub fn is_reactor_available(&self) -> bool {
        !self.uring_eventfds().is_empty()
    }

    /// Lazily spawns (once) and returns the `Notify` fanned out by this
    /// tree's eventfd pumps -- one pump per pipeline eventfd, all sharing
    /// a single `Arc<Notify>`. See the module-level fan-out note above
    /// `RawFdView` for why a single Notify (not a per-future registration)
    /// is required. `None` if there's no DiskIOUring wired, or all pumps
    /// failed to spawn. Must be called from within a Tokio runtime context;
    /// every call site is inside an async fn body driven by one, so this
    /// always holds.
    pub(crate) fn eventfd_notify(&self) -> Option<Arc<tokio::sync::Notify>> {
        let pumps = self.eventfd_pumps.get_or_init(|| {
            let fds = self.uring_eventfds();
            if fds.is_empty() {
                return Vec::new();
            }
            let notify = Arc::new(tokio::sync::Notify::new());
            let mut pumps = Vec::with_capacity(fds.len());
            for fd in fds {
                let async_fd = match AsyncFd::new(RawFdView(fd)) {
                    Ok(af) => af,
                    Err(_) => continue, // skip this pump, try the rest
                };
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
                pumps.push(EventfdPump {
                    notify: notify.clone(),
                    task,
                });
            }
            pumps
        });
        if pumps.is_empty() {
            None
        } else {
            Some(pumps[0].notify.clone())
        }
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
}

impl Drop for Crowdbtree {
    fn drop(&mut self) {
        // Stop all eventfd pumps (if any) before ct_close tears down the
        // DiskIOUring -- once that happens the eventfds are closed and the
        // raw fd numbers may be reused elsewhere in the process, so the
        // pumps must not still be waiting on them.
        if let Some(pumps) = self.eventfd_pumps.get() {
            for pump in pumps {
                pump.task.abort();
            }
        }
        unsafe { sys::ct_close(self.ptr.as_ptr()) }
    }
}

/// Initialize the C++ spdlog async file logger. Call this once at process
/// startup, before any `Crowdbtree::open`. All parameters map to the C++
/// `init_logging()` function. Safe to call when the build has no spdlog
/// (no-op).
///
/// - `log_dir` — directory for log files (empty => stderr)
/// - `level` — spdlog level name (trace/debug/info/warn/error/off)
/// - `max_file_mb` — max file size before rotation (0 => 30)
/// - `max_files` — max rotated files to keep (0 => 5)
/// - `file_prefix` — filename prefix (empty => "crowdb-tree")
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
/// shutdown (after all `Crowdbtree` instances are dropped) to ensure
/// buffered log messages are written to disk. Safe to call when logging
/// was never initialized (no-op in that case).
pub fn ct_shutdown_logging() {
    unsafe { sys::ct_shutdown_logging() };
}

/// Add a stderr sink with a per-sink level filter (e.g. "error").
/// Only messages at or above `level` go to stderr; file sinks keep their
/// original level. No-op when the C++ build has no spdlog.
pub fn ct_add_log_stderr(level: &str) {
    let level_c = CString::new(level).unwrap_or_default();
    unsafe { sys::ct_add_log_stderr(level_c.as_ptr()) };
}

static TEST_LOGGING_INIT: Once = Once::new();

/// Initialize C++ spdlog to write to a per-process directory under
/// `<workspace_root>/test-logs/`. Idempotent (guarded by `Once`); safe
/// to call from every test. Redirects C++ tree/engine logs to files
/// under `test-logs/crowdb-tree-test-<pid>/` instead of stderr. Error-
/// level messages are also mirrored to stderr so they are visible in CI
/// output for debugging. No-op when the C++ build was compiled without
/// `CROWDB_HAVE_SPDLOG`.
pub fn ct_init_test_logging() {
    TEST_LOGGING_INIT.call_once(|| {
        let dir = test_log_dir().join(format!("crowdb-tree-test-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let dir_str = dir.to_string_lossy().into_owned();
        ct_init_logging(&dir_str, "info", 30, 5, "test");
        ct_add_log_stderr("error");
    });
}

/// Find the workspace root by walking up from `CARGO_MANIFEST_DIR`
/// until a `pixi.toml` marker is found.
fn workspace_root() -> std::path::PathBuf {
    let mut dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    while !dir.join("pixi.toml").exists() {
        if !dir.pop() {
            return std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        }
    }
    dir
}

/// `<workspace_root>/test-logs/` — created if it does not exist.
fn test_log_dir() -> std::path::PathBuf {
    let dir = workspace_root().join("test-logs");
    let _ = std::fs::create_dir_all(&dir);
    dir
}
