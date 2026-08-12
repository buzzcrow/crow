// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// CRC32C (crow-common / ISA-L crc32_iscsi).
// Exposes the same hardware-accelerated CRC32C the C++ engine uses
// (R34) to the Rust WAL, replacing the software `crc32c` crate. Same
// Castagnoli polynomial + reflected/seeded convention — byte-identical.

extern "C" {
    fn crow_common_crc32c(data: *const u8, len: usize) -> u32;
    fn crow_common_crc32c_update(crc: u32, data: *const u8, len: usize) -> u32;
}

/// Compute CRC32C over `data` (seed 0).
#[must_use]
pub fn crc32c(data: &[u8]) -> u32 {
    unsafe { crow_common_crc32c(data.as_ptr(), data.len()) }
}

/// Continue CRC32C from a previous `crc` value over `data`.
#[must_use]
pub fn crc32c_update(crc: u32, data: &[u8]) -> u32 {
    unsafe { crow_common_crc32c_update(crc, data.as_ptr(), data.len()) }
}
