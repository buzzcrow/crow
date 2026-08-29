// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Shared test harness for CROWDB E2E tests.
//!
//! Provides cluster lifecycle management (`KvCluster`), hardware
//! seeding, and service-specific test helpers, gated behind Cargo
//! features so lower-layer crates can depend on it without pulling in
//! higher-layer dependencies.
//!
//! # Feature hierarchy
//!
//! - **Base (no features):** `KvCluster` — starts a kv-server cluster,
//!   wires topology, discovers leaders. Depends only on `reqwest`,
//!   `serde_json`, `tempfile`.
//! - **`kv-client`:** adds `KvCluster::make_hardware_client()` and
//!   `make_service_registry_client()`. Pulls in `crowdb-kv-client`.
//! - **`hardware`:** adds `seed_hardware()`. Implies `kv-client`,
//!   pulls in `crowdb-protocol`.
//! - **`diskio`:** adds diskio harness (`DiskioProcess`,
//!   `test_io_round`, etc.). Implies `hardware`, pulls in
//!   `crowdb-diskio-client` and `crowdb-rpc-ffi`.
//! - **`diskdb`:** adds diskdb harness (`DiskdbProcess`, etc.).
//!   Implies `hardware`, pulls in `crowdb-diskdb-client`.

#![allow(dead_code)] // test harness — not all items used by every consumer
#![allow(
    clippy::must_use_candidate,
    clippy::doc_markdown,
    clippy::missing_panics_doc,
    clippy::missing_errors_doc
)]

pub mod cluster;

#[cfg(feature = "hardware")]
pub mod hardware;

#[cfg(feature = "diskio")]
pub mod diskio;

#[cfg(feature = "diskdb")]
pub mod diskdb;

#[cfg(feature = "chunkdb")]
pub mod chunkdb;
