// CRC32C (Castagnoli, polynomial 0x1EDC6F41) used to checksum durable pages and
// the superblock so recovery can detect torn / corrupt writes. Software
// table-driven implementation; a hardware (SSE4.2 / crc32 intrinsic) path is a
// later optimization.
#pragma once

#include <cstddef>
#include <cstdint>

namespace crowtree {

namespace detail {

// Lazily-built reflected CRC32C lookup table (256 entries).
inline const uint32_t* crc32c_table() {
  static const uint32_t* table = [] {
    static uint32_t t[256];
    for (uint32_t i = 0; i < 256; ++i)
    {
      uint32_t crc = i;
      for (int k = 0; k < 8; ++k)
      {
        crc = (crc & 1) ? (crc >> 1) ^ 0x82F63B78u : (crc >> 1);
      }
      t[i] = crc;
    }
    return t;
  }();
  return table;
}

}  // namespace detail

// Streaming CRC32C: pass the previous result as `crc` (0 to start).
inline uint32_t crc32c_update(uint32_t crc, const uint8_t* data, size_t len) {
  const uint32_t* table = detail::crc32c_table();
  crc = ~crc;
  for (size_t i = 0; i < len; ++i)
  {
    crc = table[(crc ^ data[i]) & 0xff] ^ (crc >> 8);
  }
  return ~crc;
}

inline uint32_t crc32c(const uint8_t* data, size_t len) { return crc32c_update(0, data, len); }

}  // namespace crowtree
