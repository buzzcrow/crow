// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Load testing engine for the `CrowDB` Console CLI (`crowdb_kv bench ...`).
//!
//! Key work: workload kinds (read / write / list / mix), connection
//! pool over tonic `Channel`s, tokio-task worker model with HDR
//! latency histograms, and JSON report files written to
//! `bench-runs/<run-id>.json`.

pub(crate) mod handle;
pub(crate) mod metrics_flusher;
pub(crate) mod metrics_log;
pub(crate) mod report;
pub(crate) mod report_format;
pub(crate) mod runner;
pub(crate) mod target;
pub(crate) mod worker;
pub(crate) mod workload;

pub(crate) use report::BenchReport;
pub(crate) use runner::{run_bench, BenchConfig};
pub(crate) use workload::{MinSlotPolicy, WorkloadKind};
