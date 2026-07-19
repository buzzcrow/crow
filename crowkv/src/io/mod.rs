// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! `CrowKV` async I/O facade.
//!
//! All local disk operations route through [`AsyncFile`]. Backend selection
//! happens once at startup via [`IoBackend::detect`]:
//! - **Fallback** — `tokio::fs` + `spawn_blocking` for fdatasync. Works everywhere.
//! - **`SimDisk`** — in-memory, deterministic, failure-injectable (test-util only).
//! - **`io_uring`** — deferred to V2; capability probe stub always selects fallback.
//!
//! Callers never branch on backend. The WAL, engine, and snapshot layers all
//! go through `IoBackend::open` → `AsyncFile`.

mod async_file;
pub mod backend;
mod fallback;
#[cfg(feature = "test-util")]
pub mod sim;

pub use async_file::{AsyncFile, AsyncFileInner};
pub use backend::{IoBackend, OpenOptions};
#[cfg(feature = "test-util")]
pub use sim::{SimDisk, SimDiskController};
