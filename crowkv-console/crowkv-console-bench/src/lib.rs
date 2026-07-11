//! Load testing engine for the `CrowKV` Console CLI (`crowkv bench ...`).
//!
//! Key work: workload kinds (read / write / list / mix), connection
//! pool over tonic `Channel`s, tokio-task worker model with HDR
//! latency histograms, and JSON report files written to
//! `~/.crowkv/bench/<run-id>.json`.

pub mod report;
pub mod runner;
pub mod scenarios;
pub mod workload;

pub use report::{percentiles_from_histogram, BenchReport, OpReport, Percentiles};
pub use runner::{run_bench, BenchConfig};
pub use scenarios::stress_scenario;
pub use workload::{OpKind, WorkloadKind};
