// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Object-level writers — user-facing entry points. Own the drive loop.

pub mod fetch;
pub mod large_async_object;
pub mod large_object;
pub mod pool;
pub mod small_object;

pub use large_async_object::LargeAsyncObjectWriter;
pub use large_object::LargeObjectWriter;
pub use pool::{PooledWriter, WriterPool};
pub use small_object::SmallObjectWriter;
