// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Chunk lifecycle state, serialization, and orchestration.

mod handler;
pub mod state;

pub use handler::{CacheHint, ChunkGuard, ChunkLockMap, LifecycleError, LifecycleHandler, LockPolicy};
pub use state::{ChunkState, StateTransitionError};
