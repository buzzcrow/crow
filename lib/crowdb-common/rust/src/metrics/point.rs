// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

/// Typed metric point for the `/metrics` HTTP endpoint. Each variant
/// carries the metric `name` and the type-specific snapshot fields. The
/// UI renders by variant without parsing log-format strings.
#[derive(Debug, Clone, PartialEq)]
pub enum MetricPoint {
    Counter {
        name: String,
        count: u64,
        tps: f64,
        total: u64,
    },
    Gauge {
        name: String,
        value: u64,
    },
    Bandwidth {
        name: String,
        count: u64,
        avg_size: u64,
        rate: u64,
        total_bytes: u64,
    },
    Histogram {
        name: String,
        count: u64,
        avg_ns: u64,
        p50_ns: u64,
        p99_ns: u64,
        max_ns: u64,
        total: u64,
    },
    Summary {
        name: String,
        count: u64,
        avg_ns: u64,
        max_ns: u64,
        total: u64,
    },
}

impl MetricPoint {
    /// The metric name (e.g. `s.1.g.2.kv.put.c`).
    #[must_use]
    #[cfg(test)]
    pub(crate) fn name(&self) -> &str {
        match self {
            Self::Counter { name, .. }
            | Self::Gauge { name, .. }
            | Self::Bandwidth { name, .. }
            | Self::Histogram { name, .. }
            | Self::Summary { name, .. } => name,
        }
    }

    /// Lowercase kind tag matching the metric type suffix
    /// (`counter`/`gauge`/`bandwidth`/`histogram`/`summary`).
    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Counter { .. } => "counter",
            Self::Gauge { .. } => "gauge",
            Self::Bandwidth { .. } => "bandwidth",
            Self::Histogram { .. } => "histogram",
            Self::Summary { .. } => "summary",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metric_point_kind_and_name() {
        let p = MetricPoint::Counter {
            name: "x.c".into(),
            count: 1,
            tps: 0.2,
            total: 1,
        };
        assert_eq!(p.name(), "x.c");
        assert_eq!(p.kind(), "counter");
        let p = MetricPoint::Gauge {
            name: "x.g".into(),
            value: 5,
        };
        assert_eq!(p.kind(), "gauge");
        let p = MetricPoint::Bandwidth {
            name: "x.bw".into(),
            count: 1,
            avg_size: 1,
            rate: 1,
            total_bytes: 1,
        };
        assert_eq!(p.kind(), "bandwidth");
        let p = MetricPoint::Histogram {
            name: "x.lh".into(),
            count: 1,
            avg_ns: 1,
            p50_ns: 1,
            p99_ns: 1,
            max_ns: 1,
            total: 1,
        };
        assert_eq!(p.kind(), "histogram");
        let p = MetricPoint::Summary {
            name: "x.l".into(),
            count: 1,
            avg_ns: 1,
            max_ns: 1,
            total: 1,
        };
        assert_eq!(p.kind(), "summary");
    }
}
