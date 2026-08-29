// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Re-export of the generic metrics primitives now living in
//! `crowdb-common`. The module path `crowdb_kv::metrics::*` is preserved so
//! existing call sites compile unchanged; the implementation moved to
//! `crowdb_common::metrics` (R12).

pub use crowdb_common::metrics::{
    flush_system, iso8601_now, Bandwidth, BandwidthSnapshot, Counter, CounterSnapshot, Gauge,
    HistogramSnapshot, LatencyHistogram, LatencySummary, MetricName, MetricPoint, MetricsRegistry,
    MetricsRunner, SummarySnapshot, SystemCollector, SystemMetrics,
};

pub mod bandwidth {
    pub use crowdb_common::metrics::bandwidth::{Bandwidth, BandwidthSnapshot};
}
pub mod counter {
    pub use crowdb_common::metrics::counter::{Counter, CounterSnapshot, Gauge};
}
pub mod histogram {
    pub use crowdb_common::metrics::histogram::{HistogramSnapshot, LatencyHistogram};
}
pub mod summary {
    pub use crowdb_common::metrics::summary::{LatencySummary, SummarySnapshot};
}
pub mod system {
    pub use crowdb_common::metrics::system::{flush_system, SystemCollector, SystemMetrics};
}
