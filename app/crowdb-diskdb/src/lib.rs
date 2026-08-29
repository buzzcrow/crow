// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `crowdb-diskdb` library — exposes modules for integration tests.

#![allow(
    clippy::must_use_candidate,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::match_same_arms
)]

pub mod bg_task;
pub mod ddb_config;
pub mod ddb_kv_client;
pub mod health;
pub mod liveness;
pub mod metrics;
pub mod model;
pub mod recovery;
pub mod scanner;
pub mod service;
