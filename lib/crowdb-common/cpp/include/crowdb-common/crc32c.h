// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// CRC32C (Castagnoli, polynomial 0x1EDC6F41) used to checksum durable pages
// and the superblock so recovery can detect torn / corrupt writes.
// Delegates to ISA-L's crc32_iscsi which runtime-dispatches to the best
// SIMD implementation (SSE4.2+PCLMULQDQ, AVX, AVX2, AVX512 on x86; NEON
// on ARM). Same polynomial and reflected/seeded convention as the
// previous table-driven implementation — CRC values are identical.
#pragma once

#include <isa-l/crc.h>

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <limits>

namespace crowdb::common
{

[[nodiscard]] inline uint32_t crc32c_update(uint32_t crc, const uint8_t *data, size_t len)
{
    assert(len <= static_cast<size_t>(std::numeric_limits<int>::max()));
    return crc32_iscsi(const_cast<uint8_t *>(data), static_cast<int>(len), crc);
}

[[nodiscard]] inline uint32_t crc32c(const uint8_t *data, size_t len)
{
    return crc32c_update(0, data, len);
}

} // namespace crowdb::common
