// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Cross-cutting utilities shared by the `cluster`, `paxos`, and `rpc`
//! modules: static configuration profiles, per-layer metrics counters,
//! shutdown / multi-step operation reporting, monotonic-time helpers,
//! and tracing-subscriber initialization.

pub mod config;
pub mod logging;
pub(crate) mod metrics;
pub(crate) mod report;
pub(crate) mod time;

pub use report::OperationReport;

/// Convert a protobuf `u64` field to `Option<u64>`, treating 0 as `None`.
pub(crate) fn optional_u64(value: u64) -> Option<u64> {
    if value == 0 {
        None
    } else {
        Some(value)
    }
}
