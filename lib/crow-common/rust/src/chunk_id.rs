// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Chunk ID generation and routing helpers.
//!
//! 192-bit chunk ID packed into the proto `ChunkId` (high, mid, low):
//! - high byte 0: chunk type (8 bits, design §5.5)
//! - high bits 8-55: timestamp (48 bits, ms since epoch)
//! - remaining bits: random (88 bits across high/mid/low)
//!
//! Hashed to a 16-bit logical bucket (0-65535) per design §5.4a.

#![allow(
    clippy::must_use_candidate,
    clippy::missing_panics_doc,
    clippy::cast_possible_truncation
)]

use std::time::{SystemTime, UNIX_EPOCH};

use xxhash_rust::xxh64;

/// Chunk type values (design §5.5). Matches the proto `ChunkType` enum.
pub const CHUNK_TYPE_REPO: u8 = 0;
pub const CHUNK_TYPE_WAL: u8 = 1;
pub const CHUNK_TYPE_BTREE_PAGE: u8 = 2;
pub const CHUNK_TYPE_PAGE_INDEX: u8 = 3;

/// 192-bit chunk ID packed into three u64s (high, mid, low).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ChunkIdParts {
    pub high: u64,
    pub mid: u64,
    pub low: u64,
}

impl ChunkIdParts {
    pub fn to_bytes(&self) -> [u8; 24] {
        let mut buf = [0u8; 24];
        buf[..8].copy_from_slice(&self.high.to_be_bytes());
        buf[8..16].copy_from_slice(&self.mid.to_be_bytes());
        buf[16..24].copy_from_slice(&self.low.to_be_bytes());
        buf
    }

    pub fn from_bytes(buf: &[u8; 24]) -> Self {
        let high = u64::from_be_bytes(buf[..8].try_into().expect("24-byte buffer"));
        let mid = u64::from_be_bytes(buf[8..16].try_into().expect("24-byte buffer"));
        let low = u64::from_be_bytes(buf[16..24].try_into().expect("24-byte buffer"));
        Self { high, mid, low }
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
}

/// Generate a new chunk ID with the given chunk type.
///
/// Uses `getrandom` for randomness + system timestamp. Stateless — no
/// global counter.
pub fn generate(chunk_type: u8) -> ChunkIdParts {
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64);

    // 48-bit timestamp in bits 8-55 of high.
    let ts_48 = now_ms & 0xFFFF_FFFFFFFF;
    let type_bits = (u64::from(chunk_type)) << 56;
    let ts_bits = ts_48 << 8;
    let rand_high = random_u64() & 0xFF; // remaining 8 bits of high
    let high = type_bits | ts_bits | rand_high;

    let mid = random_u64();
    let low = random_u64();

    ChunkIdParts { high, mid, low }
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
