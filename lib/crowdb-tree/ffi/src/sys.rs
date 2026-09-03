// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#![allow(non_camel_case_types)]

use std::os::raw::{c_char, c_int};

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

// One zero-copy op for ct_apply_batch_external. `bytes_ref` is an
// opaque Rust handle (a boxed Bytes); `drop_fn` decrements its refcount
// when crowdb-tree frees the borrowed buffer.
pub type ct_drop_fn = extern "C" fn(*mut std::ffi::c_void);

#[repr(C)]
pub struct ct_ext_op {
    pub key: *const u8,
    pub key_len: usize,
    pub value: *const u8,
    pub value_len: usize,
    pub kind: u8,
    pub bytes_ref: *mut std::ffi::c_void,
    pub drop_fn: Option<ct_drop_fn>,
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
    pub fn ct_add_log_stderr(level: *const c_char);
    pub fn ct_shutdown_logging();
    pub fn ct_snapshot(t: *mut ct_tree, out_last_applied: *mut u64) -> c_int;
    pub fn ct_last_applied_slot(t: *const ct_tree) -> u64;
    pub fn ct_frozen_table_count(t: *const ct_tree) -> usize;
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
    pub fn ct_apply_batch_external(t: *mut ct_tree, slot: u64, ops: *const ct_ext_op, count: u64) -> c_int;
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
        start_after: *const u8,
        salen: usize,
        end_key: *const u8,
        elen: usize,
        limit: usize,
        byte_budget: usize,
        keys_only: c_int,
        deadline_ms: u64,
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

    // Async data path
    pub fn ct_evict_clean_leaves(t: *mut ct_tree, max_resident_leaves: u64) -> u64;
    pub fn ct_evict_clean_inner(t: *mut ct_tree, max_resident_inner: u64) -> u64;
    pub fn ct_get_async(t: *mut ct_tree, key: *const u8, klen: usize) -> *mut ct_future;
    pub fn ct_flush_async(t: *mut ct_tree) -> *mut ct_future;
    pub fn ct_snapshot_async(t: *mut ct_tree) -> *mut ct_future;
    pub fn ct_scan_async(
        t: *mut ct_tree,
        prefix: *const u8,
        plen: usize,
        start_after: *const u8,
        salen: usize,
        end_key: *const u8,
        elen: usize,
        limit: usize,
        byte_budget: usize,
        keys_only: c_int,
        deadline_ms: u64,
    ) -> *mut ct_future;
    pub fn ct_future_poll(
        f: *mut ct_future,
        done: *mut c_int,
        out_found: *mut c_int,
        out_slot: *mut u64,
        out_value: *mut ct_buf,
    ) -> c_int;
    pub fn ct_future_free(f: *mut ct_future);
    pub fn ct_uring_eventfds(t: *const ct_tree, out_fds: *mut i32, max_fds: usize) -> usize;

    // Metrics FFI
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

    // ── C++ global metrics registry (crowdb-common) ────────────────
    pub fn crowdb_common_metrics_global_flush(
        window_secs: f64,
        timestamp: *const c_char,
        section_label: *const c_char,
        width: usize,
        count_w: usize,
        tps_w: usize,
    ) -> *mut c_char;
    pub fn crowdb_common_metrics_global_max_name_len() -> usize;
    pub fn crowdb_common_metrics_global_free(s: *mut c_char);
}
