// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Computation layer — pure compute, no IO.
//!
//! `EcWorker` (streaming EC compute, owned by `EcStripWriter`) and
//! `HashWorker` (whole-object digest, owned by object-level writer,
//! future) are separate structs with separate APIs. No shared
//! `Worker` trait — they live at different layers with different queue
//! lengths and capabilities.

pub mod ec_worker;
pub mod hash_worker;

pub use ec_worker::EcWorker;
pub use hash_worker::HashWorker;
