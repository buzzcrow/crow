// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// C ABI wrappers for crowdb::common::crc32c so the Rust WAL can FFI to
// the same ISA-L crc32_iscsi backend the C++ engine uses (R34). Same
// Castagnoli polynomial + reflected/seeded convention — byte-identical
// to the crc32c crate the Rust WAL previously used.

#include "crowdb-common/crc32c.h"

#include <cstddef>
#include <cstdint>

extern "C" uint32_t crowdb_common_crc32c(const uint8_t *data, size_t len)
{
    return crowdb::common::crc32c(data, len);
}

extern "C" uint32_t crowdb_common_crc32c_update(uint32_t crc, const uint8_t *data, size_t len)
{
    return crowdb::common::crc32c_update(crc, data, len);
}
