// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Safe wrappers for the C++ global MetricsRegistry flush. Used by
//! the Rust `MetricsRunner` to include process-level C++ metrics
//! (e.g. `rpc.client.*`) in the periodic metrics log.

use std::ffi::{CStr, CString};

use crate::sys;

/// Flush the C++ global MetricsRegistry to a String.
/// Returns `None` if no metrics are registered or the flush is empty.
pub fn flush_cpp_global_metrics(
    window_secs: f64,
    timestamp: &str,
    section_label: &str,
    width: usize,
    count_w: usize,
    tps_w: usize,
) -> Option<String> {
    let ts = CString::new(timestamp).ok()?;
    let label = CString::new(section_label).ok()?;
    // SAFETY: ts and label are valid null-terminated C strings.
    let ptr = unsafe {
        sys::crow_common_metrics_global_flush(window_secs, ts.as_ptr(), label.as_ptr(), width, count_w, tps_w)
    };
    if ptr.is_null() {
        return None;
    }
    // SAFETY: ptr is a valid null-terminated C string from the FFI.
    let result = unsafe { CStr::from_ptr(ptr) }.to_str().ok().map(String::from);
    // SAFETY: ptr was allocated by malloc in the C++ FFI; free it.
    unsafe { sys::crow_common_metrics_global_free(ptr) };
    result
}

/// Max metric name length in the C++ global registry (for column
/// alignment in the metrics log).
pub fn cpp_global_metrics_max_name_len() -> usize {
    // SAFETY: no pointers, just returns a usize.
    unsafe { sys::crow_common_metrics_global_max_name_len() }
}
