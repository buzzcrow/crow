// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// C ABI wrappers for crow::common::crc32c so the Rust WAL can FFI to
// the same ISA-L crc32_iscsi backend the C++ engine uses (R34). Same
// Castagnoli polynomial + reflected/seeded convention — byte-identical
// to the crc32c crate the Rust WAL previously used.

#include "crow-common/crc32c.h"

#include <cstddef>
#include <cstdint>

extern "C" uint32_t crow_common_crc32c(const uint8_t *data, size_t len)
{
    return crow::common::crc32c(data, len);
}

extern "C" uint32_t crow_common_crc32c_update(uint32_t crc, const uint8_t *data, size_t len)
{
    return crow::common::crc32c_update(crc, data, len);
}
