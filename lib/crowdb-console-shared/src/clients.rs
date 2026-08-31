// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Transport clients used across the console.
//!
//! - `http`: low-level management API on a single `crowdb-kv-server`.
//!   Used by `crowdb-web` to fan out per-node primitives.
//! - `console`: high-level two-tree API on a `crowdb-web`. Used by
//!   the CLI, which never talks to a `crowdb-kv-server` directly.
//!
//! KV data-plane access used to live here too (`rpc::KvClient`,
//! C6). It's gone: `crowdb-web`'s KV handlers and `crowdb-cli`'s `kv`
//! commands and bench runner all depend on the standalone `crowdb-kv-client`
//! crate instead for topology
//! discovery, retry, and connection pooling on top of the same generated
//! `crowdb_kv::rpc` types.

pub mod console;
pub mod http;

/// Log one outbound HTTP call as a structured `tracing::info!` event.
/// Replaces the former `ops_log::append_http` record; the line lands in
/// the process's tracing log (`crowdb-cli-*.log` / `console-web-*.log`).
/// `body_summary` carries an error detail or short response note (no secrets).
pub(crate) fn log_ops_http(
    corr_id: &str,
    method: &str,
    url: &str,
    status: u16,
    duration_ms: u128,
    body_summary: Option<&str>,
) {
    let dur = u64::try_from(duration_ms).unwrap_or(u64::MAX);
    if let Some(body) = body_summary {
        tracing::info!(
            corr_id = corr_id,
            method = method,
            url = url,
            status = status,
            duration_ms = dur,
            body = body,
            "ops http",
        );
    } else {
        tracing::info!(
            corr_id = corr_id,
            method = method,
            url = url,
            status = status,
            duration_ms = dur,
            "ops http",
        );
    }
}
