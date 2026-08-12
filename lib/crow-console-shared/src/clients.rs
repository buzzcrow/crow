// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Transport clients used across the console.
//!
//! - `http`: low-level management API on a single `crow-kv-server`.
//!   Used by `crow-web` to fan out per-node primitives.
//! - `console`: high-level two-tree API on a `crow-web`. Used by
//!   the CLI, which never talks to a `crow-kv-server` directly.
//!
//! KV data-plane gRPC access used to live here too (`grpc::KvClient`,
//! C6). It's gone: `crow-web`'s KV handlers and `crow-cli`'s `kv`
//! commands and bench runner all depend on the standalone `crow-kv-client`
//! crate instead for topology
//! discovery, retry, and connection pooling on top of the same generated
//! `crow_kv::rpc` types.

pub mod console;
pub mod http;
