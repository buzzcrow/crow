// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! R72 metrics — registered in the `crow-common` `MetricsRegistry`.
//!
//! Two metrics (§11):
//! - `zone.allocate.retry.cms.bit` (counter) — incremented on each
//!   failed `cas_bit` in the allocate path.
//! - `disk.bad.impacted_blocks` (counter) — incremented for each busy
//!   block on a disk that transitions to `Bad` (§3.7).

use std::sync::Arc;

use crow_common::metrics::{Counter, MetricsRegistry};

/// Handles for the R72 metrics.
#[derive(Clone)]
pub struct DiskdbMetrics {
    /// `zone.allocate.retry.cms.bit` — CAS retry counter.
    pub allocate_retry_cas_bit: Arc<Counter>,
    /// `disk.bad.impacted_blocks` — bad-disk impacted block counter.
    pub disk_bad_impacted_blocks: Arc<Counter>,
}

impl DiskdbMetrics {
    /// Register the R72 metrics in the given registry.
    pub fn register(registry: &mut MetricsRegistry) -> Self {
        Self {
            allocate_retry_cas_bit: registry.register_counter("zone.allocate.retry.cms.bit"),
            disk_bad_impacted_blocks: registry.register_counter("disk.bad.impacted_blocks"),
        }
    }

    /// Create a no-op set of metrics (for tests that don't have a
    /// registry).
    #[must_use]
    pub fn disabled() -> Self {
        let mut registry = MetricsRegistry::new();
        Self::register(&mut registry)
    }
}
