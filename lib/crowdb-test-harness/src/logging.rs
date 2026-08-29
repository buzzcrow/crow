// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! One-shot C++ spdlog initialization for test processes.
//!
//! Uses `#[ctor]` to run before `main()` (or before the first `#[test]`)
//! so C++ info/debug logs go to `test-logs/` files instead of stderr.
//! Only error-level messages are mirrored to stderr for CI visibility.
//! No manual calls needed — any test binary that links `crowdb-test-harness`
//! with a `diskio`/`diskdb`/`chunkdb` feature gets this automatically.

use std::sync::Once;

static LOGGING_INIT: Once = Once::new();

/// Initialize C++ spdlog for both crowdb-tree and crowdb-rpc.
/// Idempotent — safe to call multiple times. No-op when the C++
/// build was compiled without `CROWDB_HAVE_SPDLOG`.
pub fn init_test_logging() {
    LOGGING_INIT.call_once(|| {
        #[cfg(any(feature = "diskio", feature = "diskdb", feature = "chunkdb"))]
        {
            crowdb_tree_ffi::ct_init_test_logging();
            crowdb_rpc_ffi::init_test_logging();
        }
    });
}

/// Auto-init before main / first test. Runs once when the crate is loaded.
#[ctor::ctor(unsafe)]
fn auto_init() {
    init_test_logging();
}
