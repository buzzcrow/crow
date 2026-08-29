// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Leak detection scaffold. v1 returns `deferred` — full leak
//! detection requires caller registries that do not exist yet. The
//! ghost allocation detector covers crash-related orphans; this
//! module establishes the interface so the scanner loop can call it
//! without conditional code.

/// Result of a leak scan cycle. v1 always reports `deferred`.
#[derive(Debug, Clone, Copy)]
pub struct LeakScanResult {
    /// `"deferred"` in v1.
    pub status: &'static str,
    /// Human-readable explanation.
    pub message: &'static str,
}

impl Default for LeakScanResult {
    fn default() -> Self {
        Self {
            status: "deferred",
            message: "Leak detection requires caller registries (not yet \
                      implemented). Use ghost allocation detection for \
                      crash-related orphans.",
        }
    }
}

/// Run a leak-detection scan. v1 returns `deferred` immediately —
/// no KV reads, no bitmap work. The scanner calls this each cycle so
/// the `ScanSummary.leak_status` field is populated.
pub async fn scan_for_leaks() -> LeakScanResult {
    LeakScanResult::default()
}
