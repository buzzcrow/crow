// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Re-export of the generic metrics primitives now living in
//! `crow-common`. The module path `crow_kv::metrics::*` is preserved so
//! existing call sites compile unchanged; the implementation moved to
//! `crow_common::metrics` (R12).

pub use crow_common::metrics::{
    flush_system, Bandwidth, BandwidthSnapshot, Counter, CounterSnapshot, Gauge, HistogramSnapshot,
    LatencyHistogram, LatencySummary, MetricName, MetricsRegistry, MetricsRunner, SummarySnapshot,
    SystemCollector, SystemMetrics,
};

pub mod bandwidth {
    pub use crow_common::metrics::bandwidth::{Bandwidth, BandwidthSnapshot};
}
pub mod counter {
    pub use crow_common::metrics::counter::{Counter, CounterSnapshot, Gauge};
}
pub mod histogram {
    pub use crow_common::metrics::histogram::{HistogramSnapshot, LatencyHistogram};
}
pub mod summary {
    pub use crow_common::metrics::summary::{LatencySummary, SummarySnapshot};
}
pub mod system {
    pub use crow_common::metrics::system::{flush_system, SystemCollector, SystemMetrics};
}
