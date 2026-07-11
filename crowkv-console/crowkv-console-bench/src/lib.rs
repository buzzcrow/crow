//! Load testing engine for the `CrowKV` Console CLI (`crowkv bench ...`).
//!
//! Key work: Workload trait + read/write/list/mix impls, multi-connection
//! pool, blocking worker threads (1..=1000), HDR latency histograms,
//! JSON report files.
//!
//! C0 status: skeleton; real impl lands in C7.

#![cfg_attr(not(test), allow(dead_code))]

/// Placeholder workload kinds, kept stable so CLI scaffolding can already
/// match on them in C0.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkloadKind {
    Read,
    Write,
    List,
    Mix,
}

#[cfg(test)]
mod tests {
    use super::WorkloadKind;

    #[test]
    fn workload_kinds_distinct() {
        assert_ne!(WorkloadKind::Read, WorkloadKind::Write);
    }
}
