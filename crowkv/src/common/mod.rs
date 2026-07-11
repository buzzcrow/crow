pub mod config;
pub mod logging;
pub mod metrics;

pub use metrics::{LayerMetrics, MetricsSnapshot};

/// Convert a protobuf `u64` field to `Option<u64>`, treating 0 as `None`.
pub(crate) fn optional_u64(value: u64) -> Option<u64> {
    if value == 0 {
        None
    } else {
        Some(value)
    }
}
