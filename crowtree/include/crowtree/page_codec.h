// PageCodec: the on-disk byte representation of a consolidated base page.
//
// A framed page is self-describing and checksummed so recovery validates it
// without external metadata:
//
//   frame := [u32 logical_len][u32 crc32c][body bytes][zero pad to IU]
//
// crc32c covers the body bytes [0, logical_len). The body is the packed
// leaf/inner layout (offset arrays for leaves; separators + child PIDs for
// inner). Only base pages are encoded; delta chains are folded first.
//
// Key work: leaf/inner body packing, framing + IU padding, CRC validation,
// decode into freshly-allocated pages.
#pragma once

#include "crowtree/page.h"
#include "crowtree/status.h"

#include <cstdint>
#include <vector>

namespace crowtree {

// 8-byte frame prefix ahead of the body; the rest is body + IU zero padding.
inline constexpr size_t kPageFrameHeaderSize = sizeof(uint32_t) * 2;

class PageCodec {
 public:
  // Encode `page` (kLeafBase or kInnerBase) into a framed, IU-padded buffer.
  static std::vector<uint8_t> Encode(const PageBase* page, uint32_t iu_size);

  // Decode a framed buffer into a freshly heap-allocated page (LeafBase or
  // InnerBase). On success the caller owns *out. Returns Corruption on a CRC
  // mismatch or a malformed body, kInvalidArgument on a short buffer.
  static Status Decode(const uint8_t* buf, size_t len, PageBase** out);
};

}  // namespace crowtree
