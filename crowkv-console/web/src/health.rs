//! Liveness probe for the console web binary.
//!
//! The pre-A12 `/api/cluster/snapshot` aggregator that lived here has
//! been retired per design-console.md §6.1; the SPA composes the same
//! information from the per-resource endpoints under `/api/racks/...`,
//! `/api/nodes/...`, and `/api/stores/...`, all of which read from the
//! monitor cache.

pub async fn healthz() -> &'static str {
    "ok"
}
