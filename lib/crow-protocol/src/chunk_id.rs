// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! 128-bit chunk ID generation, parsing, and routing helpers.
//!
//! Layout (design §5.4): 8 type + 48 timestamp + 72 random, packed into
//! the proto `ChunkId` (high, low):
//! - high byte 0: chunk type (8 bits)
//! - high bits 8-55: timestamp (48 bits, ms since epoch)
//! - high bits 56-63: 8 random bits
//! - low: 64 random bits
//!
//! Total randomness: 72 bits. Hashed to a 16-bit logical bucket
//! (0-65535) per design §5.4a.

#![allow(
    clippy::must_use_candidate,
    clippy::missing_panics_doc,
    clippy::cast_possible_truncation
)]

use std::time::{SystemTime, UNIX_EPOCH};

use xxhash_rust::xxh64;

use crate::common::ChunkId;

/// Chunk type values (design §5.5). Matches the proto `ChunkType` enum.
pub const CHUNK_TYPE_REPO: u8 = 0;
pub const CHUNK_TYPE_WAL: u8 = 1;
pub const CHUNK_TYPE_BTREE_PAGE: u8 = 2;
pub const CHUNK_TYPE_PAGE_INDEX: u8 = 3;

/// 128-bit chunk ID parts — mirrors the proto `ChunkId` (high, low).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ChunkIdParts {
    pub high: u64,
    pub low: u64,
}

impl ChunkIdParts {
    pub fn to_bytes(&self) -> [u8; 16] {
        let mut buf = [0u8; 16];
        buf[..8].copy_from_slice(&self.high.to_be_bytes());
        buf[8..].copy_from_slice(&self.low.to_be_bytes());
        buf
    }

    pub fn from_bytes(buf: &[u8; 16]) -> Self {
        let high = u64::from_be_bytes(buf[..8].try_into().expect("16-byte buffer"));
        let low = u64::from_be_bytes(buf[8..].try_into().expect("16-byte buffer"));
        Self { high, low }
    }

    pub fn chunk_type(&self) -> u8 {
        (self.high >> 56) as u8
    }

    /// Hash to a 16-bit logical bucket (0-65535) per design §5.4a.
    pub fn hash_to_bucket(&self) -> u16 {
        let bytes = self.to_bytes();
        let hash = xxh64::xxh64(&bytes, 0);
        (hash & 0xFFFF) as u16
    }

    /// Convert to a proto `ChunkId`.
    pub fn to_proto(&self) -> ChunkId {
        ChunkId {
            high: self.high,
            low: self.low,
        }
    }

    /// Convert from a proto `ChunkId`.
    pub fn from_proto(id: &ChunkId) -> Self {
        Self {
            high: id.high,
            low: id.low,
        }
    }
}

/// Generate a new 128-bit chunk ID with the given chunk type.
///
/// Uses `getrandom` for randomness + system timestamp. Stateless — no
/// global counter.
pub fn generate(chunk_type: u8) -> ChunkIdParts {
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64);

    // 48-bit timestamp in bits 8-55 of high.
    let ts_48 = now_ms & 0xFFFF_FFFFFFFF;
    let type_bits = u64::from(chunk_type) << 56;
    let ts_bits = ts_48 << 8;
    let rand_high = random_u64() & 0xFF; // remaining 8 bits of high
    let high = type_bits | ts_bits | rand_high;

    let low = random_u64();

    ChunkIdParts { high, low }
}

/// Check if a `ChunkId` is all-zeros.
pub fn is_zero(id: &ChunkId) -> bool {
    id.high == 0 && id.low == 0
}

fn random_u64() -> u64 {
    let mut buf = [0u8; 8];
    // getrandom should not fail on Linux; if it does, use a
    // time-based fallback (low quality but non-panicking).
    if getrandom::getrandom(&mut buf).is_err() {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos() as u64);
        return now.wrapping_mul(0x5851_F42D_4C95_7F2D);
    }
    u64::from_be_bytes(buf)
}
