// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Hand-written Rust types replacing the former prost-generated proto types.
//! API-compatible — same field names, types, and derives. No `prost::Message`.

pub mod chunkdb;
pub mod common;
pub mod diskdb;
pub mod diskio;
pub mod kv_client;
pub mod kv_consensus;
