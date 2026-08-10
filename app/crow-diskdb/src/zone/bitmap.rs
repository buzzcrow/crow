// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Lock-free usage bitmap for zone block tracking.
//!
//! Wraps `Vec<AtomicU64>` for lock-free bit operations. Each bit
//! represents one block (default 1 MB) in a zone. `range_set` marks
//! blocks allocated (on allocate); `range_clear` marks them free (on
//! free). Double-set and double-clear are detected and rolled back.

use std::sync::atomic::{AtomicU64, Ordering};

/// Lock-free usage bitmap. One bit per block.
pub struct UsageBitmap {
    bits: Vec<AtomicU64>,
    block_count: u32,
}

impl UsageBitmap {
    /// Create a bitmap of `block_count` bits, all initialized to 0.
    #[must_use]
    pub fn new(block_count: u32) -> Self {
        let word_count = (block_count as usize).div_ceil(64);
        let bits = (0..word_count).map(|_| AtomicU64::new(0)).collect();
        Self { bits, block_count }
    }

    /// Number of blocks this bitmap tracks.
    #[must_use]
    pub fn block_count(&self) -> u32 {
        self.block_count
    }

    /// Number of `u64` words backing the bitmap.
    #[must_use]
    pub fn word_count(&self) -> usize {
        self.bits.len()
    }

    /// Set bits `[offset..offset+count)`. Returns `false` if any bit was
    /// already set (double-allocation); rolls back on collision.
    #[must_use]
    pub fn range_set(&self, offset: u32, count: u32) -> bool {
        for i in 0..count {
            let bit_index = (offset + i) as usize;
            let word_index = bit_index / 64;
            let bit_pos = bit_index % 64;
            let mask = 1u64 << bit_pos;
            let prev = self.bits[word_index].fetch_or(mask, Ordering::AcqRel);
            if prev & mask != 0 {
                // Bit was already set — roll back what we just set.
                for j in 0..i {
                    let rb_index = (offset + j) as usize;
                    let rb_word = rb_index / 64;
                    let rb_bit = rb_index % 64;
                    self.bits[rb_word].fetch_and(!(1u64 << rb_bit), Ordering::AcqRel);
                }
                return false;
            }
        }
        true
    }

    /// Clear bits `[offset..offset+count)`. Returns `false` if any bit
    /// was already clear (double-free); rolls back on collision.
    #[must_use]
    pub fn range_clear(&self, offset: u32, count: u32) -> bool {
        for i in 0..count {
            let bit_index = (offset + i) as usize;
            let word_index = bit_index / 64;
            let bit_pos = bit_index % 64;
            let mask = 1u64 << bit_pos;
            let prev = self.bits[word_index].fetch_and(!mask, Ordering::AcqRel);
            if prev & mask == 0 {
                // Bit was already clear — roll back what we just cleared.
                for j in 0..i {
                    let rb_index = (offset + j) as usize;
                    let rb_word = rb_index / 64;
                    let rb_bit = rb_index % 64;
                    self.bits[rb_word].fetch_or(1u64 << rb_bit, Ordering::AcqRel);
                }
                return false;
            }
        }
        true
    }

    /// Snapshot all bitmap words for serialization (little-endian bytes).
    #[must_use]
    pub fn snapshot(&self) -> Vec<u8> {
        let mut result = Vec::with_capacity(self.bits.len() * 8);
        for word in &self.bits {
            result.extend_from_slice(&word.load(Ordering::Acquire).to_le_bytes());
        }
        result
    }

    /// Restore a bitmap from serialized bytes (little-endian).
    #[must_use]
    pub fn restore(bytes: &[u8]) -> Self {
        let word_count = bytes.len().div_ceil(8);
        let mut bits = Vec::with_capacity(word_count);
        for i in 0..word_count {
            let start = i * 8;
            let end = (start + 8).min(bytes.len());
            let mut buf = [0u8; 8];
            buf[..end - start].copy_from_slice(&bytes[start..end]);
            bits.push(AtomicU64::new(u64::from_le_bytes(buf)));
        }
        #[allow(clippy::cast_possible_truncation)]
        let block_count = (word_count * 64) as u32;
        Self { bits, block_count }
    }

    /// Count the number of set bits (allocated blocks).
    #[must_use]
    pub fn count_set(&self) -> u64 {
        self.bits
            .iter()
            .map(|w| u64::from(w.load(Ordering::Acquire).count_ones()))
            .sum()
    }
}

/// Create a usage bitmap of `block_count` bits, all initialized to 0.
#[must_use]
pub fn create_usage_bitmap(block_count: u32) -> UsageBitmap {
    UsageBitmap::new(block_count)
}
