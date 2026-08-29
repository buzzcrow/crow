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
