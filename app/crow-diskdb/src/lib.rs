// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! `crow-diskdb` library — exposes modules for integration tests.

#![allow(
    clippy::must_use_candidate,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::match_same_arms
)]

pub mod config;
pub mod grpc;
pub mod node;
pub mod status;
pub mod sync;
