// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! chunkdb service — delegates to lifecycle handlers.
//!
//! The legacy tonic server trait impl has been removed. The crow-rpc
//! dispatch lives in `chunkdb_rpc_service.rs`.

use crate::lifecycle::LifecycleHandler;

/// Suppress unused-import warning — `LifecycleHandler` was used by the
/// former tonic `ChunkdbService` struct. The crow-rpc service in
/// `chunkdb_rpc_service.rs` has its own struct with the same handler.
#[allow(dead_code)]
type _DeprecatedChunkdbService = std::sync::Arc<LifecycleHandler>;
