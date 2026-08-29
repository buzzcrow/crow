// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Zero-copy flatbuffer wrapper classes (design-crowdb-rpc.md §6).
//!
//! Each `Ref` struct holds a `&[u8]` reference to the control buffer
//! and exposes typed accessors that read through the flatbuffer root
//! pointer — no per-field copy, no owned intermediate struct.

pub mod chunkdb;
pub mod diskdb;
pub mod kv_client;
pub mod kv_consensus;

/// Parse a flatbuffer root from a byte slice, returning `None` on
/// parse failure (malformed buffer / wrong type). Shared by all
/// wrapper modules. The bound matches `flatbuffers::root` —
/// `Follow<'a>` (with `Inner = Self` for generated table types) +
/// `Verifiable`.
pub(super) fn parse_root<'a, T>(buf: &'a [u8]) -> Option<T::Inner>
where
    T: 'a + flatbuffers::Follow<'a> + flatbuffers::Verifiable,
{
    if buf.len() < 4 {
        return None;
    }
    flatbuffers::root::<T>(buf).ok()
}
