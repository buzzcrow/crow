// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! `crow-chunkdb` library — exposes modules for integration tests.

#![allow(
    clippy::must_use_candidate,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::match_same_arms,
    clippy::doc_markdown
)]

pub mod allocator;
pub mod chunkdb_config;
pub mod selector;
pub mod service;
pub mod topology;
