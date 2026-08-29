// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Chunk + strip layer — chunk-info wrapper + strip primitives.
//!
//! `ChunkWriter` wraps the current `Chunk` protobuf and provides write
//! ability (iterates strips, triggers append-strip, replaces blocks).
//! `ChunkPrefetch` pre-creates chunks and appends strips ahead of the
//! write cursor. `StripWriter` (enum) owns one strip's data + parity
//! write. `parity_writer::spawn_parity_writes` spawns parity write +
//! fsync tasks (joined at seal, not at strip finish). Later:
//! `chunk_reader.rs` / `strip_reader.rs` for R107.

pub mod chunk_prefetch;
pub mod chunk_reader;
pub mod chunk_writer;
pub mod ec_strip_writer;
pub mod mirror_strip_writer;
pub mod parity_writer;
pub mod strip;
pub mod strip_reader;

pub use chunk_prefetch::ChunkPrefetch;
pub use chunk_writer::ChunkWriter;
pub use ec_strip_writer::EcStripWriter;
pub use mirror_strip_writer::MirrorStripWriter;
pub use strip::{StripResult, StripWriter};
