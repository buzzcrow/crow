// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Shared utilities for the `crow` storage-system components
//! (`crowkv`, `crow-tree`, future `crow-*` libs).
//!
//! Re-exports the generic, project-agnostic primitives extracted out of
//! `crowkv` and `crowtree` so each component depends on `crow-common`
//! instead of re-implementing or vendoring them:
//!
//! - [`metrics`] — lightweight atomic counters, gauges, bandwidth,
//!   latency histograms/summaries, registry, and periodic flush runner.
//! - [`logging`] — `tracing-subscriber` + rotating/compressing file
//!   appender initialization.
//! - [`time`] — process-wide monotonic-time anchor helpers.
//! - [`report`] — multi-step operation error aggregation.

pub mod logging;
pub mod metrics;
pub mod report;
pub mod time;
