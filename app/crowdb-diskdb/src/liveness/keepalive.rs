// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `DiskDB` keep-alive scheduling and reconciliation.

mod scheduler;

pub use scheduler::{KeepAlive, KeepAliveOutcome};
