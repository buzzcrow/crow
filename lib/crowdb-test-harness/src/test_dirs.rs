// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Project-local test directories — no system temp folder.
//!
//! All test intermediate files (WAL data, config, logs, workspace dirs)
//! go under `<workspace_root>/test-data/` and `<workspace_root>/test-logs/`
//! so they persist for inspection and CI artifact upload.
//!
//! [`TestDir`] is a drop guard that auto-deletes on success but preserves
//! the directory when the test panics (via `std::thread::panicking()`),
//! so failed-test data is always available for debugging.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// Find the workspace root by walking up from `CARGO_MANIFEST_DIR` until
/// a `pixi.toml` marker file is found. Falls back to `CARGO_MANIFEST_DIR`
/// if the marker is never found (should not happen in this workspace).
#[must_use]
pub fn workspace_root() -> PathBuf {
    let mut dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    while !dir.join("pixi.toml").exists() {
        if !dir.pop() {
            return PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        }
    }
    dir
}

/// `<workspace_root>/test-data/` — for WAL segments, config files,
/// database files, and other test data. Created if it does not exist.
#[must_use]
pub fn test_data_dir() -> PathBuf {
    let dir = workspace_root().join("test-data");
    let _ = std::fs::create_dir_all(&dir);
    dir
}

/// `<workspace_root>/test-logs/` — for server stdout, C++ spdlog,
/// and other log files. Created if it does not exist.
#[must_use]
pub fn test_log_dir() -> PathBuf {
    let dir = workspace_root().join("test-logs");
    let _ = std::fs::create_dir_all(&dir);
    dir
}

fn unique_suffix() -> String {
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{}-{}", std::process::id(), n)
}

/// A project-local test directory that auto-deletes on success but
/// preserves on failure (panic). Drop-in replacement for
/// `tempfile::TempDir` — provides `path()` and can be stored in struct
/// fields.
///
/// Created under `test-data/<tag>-<pid>-<counter>/`. The tag helps
/// identify which test created the directory when inspecting leftover
/// data from a failed test.
///
/// # Panics
/// Panics if directory creation fails (disk full, permission denied).
pub struct TestDir {
    path: PathBuf,
    cleanup: bool,
}

impl TestDir {
    /// Create a new unique directory under `test-data/`.
    ///
    /// # Errors
    /// Returns `io::Error` if `create_dir_all` fails.
    pub fn new(tag: &str) -> std::io::Result<Self> {
        let path = test_data_dir().join(format!("{tag}-{}", unique_suffix()));
        std::fs::create_dir_all(&path)?;
        Ok(Self { path, cleanup: true })
    }

    /// The directory path.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Consume the guard and return the path without scheduling cleanup.
    /// The directory will persist indefinitely.
    #[must_use]
    pub fn keep(mut self) -> PathBuf {
        self.cleanup = false;
        self.path.clone()
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        if self.cleanup && !std::thread::panicking() {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }
}

/// Create a [`TestDir`] under `test-data/` with the given tag.
/// Convenience wrapper for `TestDir::new(tag).unwrap()`.
///
/// # Panics
/// Panics if directory creation fails.
#[must_use]
pub fn tempdir_in_test_data(tag: &str) -> TestDir {
    TestDir::new(tag).unwrap_or_else(|e| panic!("create test dir '{tag}': {e}"))
}
