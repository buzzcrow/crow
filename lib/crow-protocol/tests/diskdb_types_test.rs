// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Tests for `crow-protocol` diskdb domain logic: `HwStatus` rules,
//! `ZoneAllocationState` interop, `ZoneValue` CRC integrity, and bitmap.

use crow_protocol::common::HwStatus;
use crow_protocol::diskdb::rpc::{ZoneAllocationState, ZoneValue};
use crow_protocol::{HwStatusExt, UsageBitmap, ZoneAllocationStateExt, ZoneValueExt};

// ── HwStatus ────────────────────────────────────────────────────

#[test]
fn hw_status_allows_allocate_and_free() {
    assert!(HwStatus::Up.allows_allocate());
    assert!(!HwStatus::Init.allows_allocate());
    assert!(!HwStatus::Maintenance.allows_allocate());
    assert!(!HwStatus::Suspect.allows_allocate());
    assert!(!HwStatus::Offline.allows_allocate());

    assert!(HwStatus::Up.allows_free());
    assert!(!HwStatus::Init.allows_free());
    assert!(HwStatus::Maintenance.allows_free());
    assert!(HwStatus::Suspect.allows_free());
    assert!(!HwStatus::Offline.allows_free());
}

// ── ZoneAllocationState ─────────────────────────────────────────

#[test]
fn zone_allocation_state_from_u8_unknown_defaults_to_full() {
    assert_eq!(
        ZoneAllocationState::from_u8(99),
        ZoneAllocationState::ZoneAllocFull
    );
}

// ── ZoneValue CRC ───────────────────────────────────────────────

fn sample_zone_value() -> ZoneValue {
    ZoneValue {
        usage_bitmap: vec![0xFF; 16],
        snapshot_slot: 42,
        crc32: 0,
    }
}

#[test]
fn zone_value_crc_tamper_detected() {
    let mut val = sample_zone_value();
    val.compute_checksum();
    assert!(val.verify_checksum());
    val.usage_bitmap[0] = 0x00; // tamper the bitmap
    assert!(!val.verify_checksum());
}

#[test]
fn zone_value_empty_bitmap_baseline_is_valid() {
    // Baseline ZoneValue written during disk-add init: empty bitmap,
    // snapshot_slot = 0, crc32 = crc32fast::hash(&[]) (= 0).
    let mut val = ZoneValue {
        usage_bitmap: vec![],
        snapshot_slot: 0,
        crc32: 0,
    };
    val.compute_checksum();
    assert!(val.verify_checksum());
    assert_eq!(val.crc32, 0); // CRC32 of empty is 0
}

// ── Bitmap ──────────────────────────────────────────────────────

#[test]
fn bitmap_double_set_fails() {
    let bm = UsageBitmap::new(128);
    assert!(bm.range_set(0, 4));
    assert!(!bm.range_set(2, 4));
}

#[test]
fn bitmap_double_clear_fails() {
    let bm = UsageBitmap::new(128);
    assert!(bm.range_set(0, 4));
    assert!(bm.range_clear(0, 4));
    assert!(!bm.range_clear(0, 4));
}

#[test]
fn bitmap_cross_word_boundary() {
    let bm = UsageBitmap::new(128);
    assert!(bm.range_set(62, 4));
    let snap = bm.snapshot();
    let w0 = u64::from_le_bytes(snap[0..8].try_into().unwrap());
    let w1 = u64::from_le_bytes(snap[8..16].try_into().unwrap());
    assert_ne!(w0 & (1u64 << 62), 0);
    assert_ne!(w0 & (1u64 << 63), 0);
    assert_ne!(w1 & 1, 0);
    assert_ne!(w1 & 2, 0);
}

#[test]
fn bitmap_snapshot_restore_roundtrip() {
    let bm = UsageBitmap::new(128);
    let _ = bm.range_set(0, 10);
    let _ = bm.range_set(70, 5);
    let snap = bm.snapshot();
    let restored = UsageBitmap::restore(&snap);
    assert_eq!(restored.snapshot(), snap);
}

#[test]
fn bitmap_count_set_after_set_and_clear() {
    let bm = UsageBitmap::new(128);
    let _ = bm.range_set(0, 10);
    let _ = bm.range_set(70, 5);
    assert_eq!(bm.count_set(), 15);
    let _ = bm.range_clear(0, 4);
    assert_eq!(bm.count_set(), 11);
}

// ── Bitmap CAS helpers ──────────────────────────────────────────

#[test]
fn bitmap_load_word_and_cas_word_roundtrip() {
    let bm = UsageBitmap::new(128);
    assert_eq!(bm.load_word(0), 0);
    assert!(bm.cas_word(0, 0, 0xFFFF).is_ok());
    assert_eq!(bm.load_word(0), 0xFFFF);
    // CAS with wrong expected fails.
    assert!(bm.cas_word(0, 0, 0xAAAA).is_err());
    assert_eq!(bm.load_word(0), 0xFFFF);
}

#[test]
fn bitmap_cas_bit_set_and_clear() {
    let bm = UsageBitmap::new(128);
    // Set a bit that was clear → succeeds.
    assert!(bm.cas_bit(5, true));
    assert_eq!(bm.load_word(0) & (1u64 << 5), 1u64 << 5);
    // Set the same bit again → fails (already set).
    assert!(!bm.cas_bit(5, true));
    // Clear the bit → succeeds.
    assert!(bm.cas_bit(5, false));
    assert_eq!(bm.load_word(0) & (1u64 << 5), 0);
    // Clear again → fails (already clear).
    assert!(!bm.cas_bit(5, false));
}

#[test]
fn bitmap_cas_bit_cross_word_boundary() {
    let bm = UsageBitmap::new(128);
    // Bit 63 is in word 0; bit 64 is in word 1.
    assert!(bm.cas_bit(63, true));
    assert!(bm.cas_bit(64, true));
    assert_ne!(bm.load_word(0) & (1u64 << 63), 0);
    assert_ne!(bm.load_word(1) & 1, 0);
}
