// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Benchmark operations: KV workload runners and RPC bench.

use crate::error::{Error, Result};
use crate::ops::OpContext;

/// Run a KV write benchmark. Stub — will delegate to the existing bench runner.
///
/// # Errors
/// Always returns [`Error::NotImplemented`] until the bench runner is wired.
pub fn kv_write_bench(_ctx: &OpContext) -> Result<()> {
    Err(Error::NotImplemented("kv_write_bench".into()))
}
