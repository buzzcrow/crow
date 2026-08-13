// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! `crow-diskdb` library — exposes modules for integration tests.

#![allow(
    clippy::must_use_candidate,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::match_same_arms
)]

pub mod bg_task;
pub mod ddb_config;
pub mod ddb_kv_client;
pub mod keepalive;
pub mod lifecycle;
pub mod metrics;
pub mod model;
pub mod recovery;
pub mod service;
pub mod status_machine;
