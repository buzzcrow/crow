// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Load testing engine for the `CrowKV` Console CLI (`crowkv bench ...`).
//!
//! Key work: workload kinds (read / write / list / mix), connection
//! pool over tonic `Channel`s, tokio-task worker model with HDR
//! latency histograms, and JSON report files written to
//! `bench-runs/<run-id>.json`.

pub mod provision;
pub mod report;
pub mod runner;
pub mod workload;

pub use provision::{BenchFixture, BenchMode};
pub use report::BenchReport;
pub use runner::{run_bench, BenchConfig};
pub use workload::WorkloadKind;
