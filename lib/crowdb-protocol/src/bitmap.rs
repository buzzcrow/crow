// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Lock-free bitmap for block tracking.
//!
//! Wraps `Vec<AtomicU64>` for lock-free bit operations. Each bit
//! represents one block. `range_set` marks blocks allocated; `range_clear`
//! marks them free. Double-set and double-clear are detected and rolled back.

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

    /// Load one 64-bit word (`Acquire`).
    ///
    /// # Panics
    /// Panics if `index >= word_count()`.
    #[must_use]
    pub fn load_word(&self, index: usize) -> u64 {
        self.bits[index].load(Ordering::Acquire)
    }

    /// Compare-and-swap one 64-bit word (`AcqRel` / `Acquire`).
    /// Returns `Ok(actual)` on success (the value now stored) or
    /// `Err(actual)` on failure (the value the word held instead of
    /// `expected`).
    ///
    /// # Errors
    /// Returns `Err(actual)` when the word did not contain `expected`
    /// at the time of the CAS (either another thread modified it or the
    /// word was already in the `new` state).
    ///
    /// # Panics
    /// Panics if `index >= word_count()`.
    pub fn cas_word(&self, index: usize, expected: u64, new: u64) -> Result<u64, u64> {
        self.bits[index].compare_exchange(expected, new, Ordering::AcqRel, Ordering::Acquire)
    }

    /// Compare-and-swap a single bit — one attempt. Sets the bit if
    /// `set` is true and it was clear, or clears it if `set` is false
    /// and it was set. Returns `true` if the CAS succeeded (the bit
    /// transitioned to the target state), `false` if the bit was
    /// already in the target state or the CAS lost a race (the caller
    /// reloads the word and retries, bounded by the allocator's
    /// `cas_retry_limit`).
    ///
    /// # Panics
    /// Panics if `bit_index >= block_count`.
    #[must_use]
    pub fn cas_bit(&self, bit_index: u32, set: bool) -> bool {
        let word_index = bit_index as usize / 64;
        let bit_pos = bit_index % 64;
        let mask = 1u64 << bit_pos;
        let current = self.bits[word_index].load(Ordering::Acquire);
        let bit_set = current & mask != 0;
        if set == bit_set {
            return false; // already in target state
        }
        let new = if set { current | mask } else { current & !mask };
        self.bits[word_index]
            .compare_exchange(current, new, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
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

    /// Check whether bit `offset` is set (`Acquire`).
    ///
    /// # Panics
    /// Panics if `offset >= block_count`.
    #[must_use]
    pub fn is_set(&self, offset: u32) -> bool {
        let word_index = offset as usize / 64;
        let bit_pos = offset % 64;
        let mask = 1u64 << bit_pos;
        self.bits[word_index].load(Ordering::Acquire) & mask != 0
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
