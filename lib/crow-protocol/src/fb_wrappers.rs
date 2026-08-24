// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Zero-copy flatbuffer wrapper classes (design-crow-rpc.md §6).
//!
//! Each `Ref` struct holds a `&[u8]` reference to the control buffer
//! and exposes typed accessors that read through the flatbuffer root
//! pointer — no per-field copy, no owned intermediate struct.

pub mod kv_consensus;
