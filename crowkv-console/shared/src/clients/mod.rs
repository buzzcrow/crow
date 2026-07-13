// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Transport clients used across the console.
//!
//! - `http`: low-level management API on a single `crowkv-server`.
//!   Used by `crowkv-web` to fan out per-node primitives.
//! - `console`: high-level two-tree API on a `crowkv-web`. Used by
//!   the CLI, which never talks to a `crowkv-server` directly.
//!
//! KV data-plane gRPC access used to live here too (`grpc::KvClient`,
//! C6). It's gone: `crowkv-web`'s KV handlers and `crowkv-cli`'s `kv`
//! commands and bench runner all depend on the standalone `crowkv-client`
//! crate instead for topology
//! discovery, retry, and connection pooling on top of the same generated
//! `crowkv::rpc` types.

pub mod console;
pub mod http;
