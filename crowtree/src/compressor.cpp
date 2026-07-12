#include "crowtree/compressor.h"

#include "crowtree/crc32c.h"

#include <cstring>

#if CROWTREE_HAVE_LZ4
// Use the real LZ4 header (vendored under third_party/lz4 or the system dev
// package); the build wires the include path. The block API
// (LZ4_compress_default / LZ4_decompress_safe) is all we need.
#include <lz4.h>
#endif

namespace crowtree {

namespace {
void put_u32(std::vector<uint8_t>* o, uint32_t v) {
  for (int i = 0; i < 4; ++i)
  {
    o->push_back(static_cast<uint8_t>((v >> (8 * i)) & 0xff));
  }
}
uint32_t get_u32(const uint8_t* p) {
  uint32_t v = 0;
  for (int i = 0; i < 4; ++i)
  {
    v |= static_cast<uint32_t>(p[i]) << (8 * i);
  }
  return v;
}
}  // namespace

bool lz4_available() {
#if CROWTREE_HAVE_LZ4
  return true;
#else
  return false;
#endif
}

void encode_durable_page(const uint8_t* frame, uint32_t page_bytes, compress_algo prefer,
                         std::vector<uint8_t>* out) {
  compress_algo algo = compress_algo::kNone;
  std::vector<uint8_t> stored;

#if CROWTREE_HAVE_LZ4
  if (prefer == compress_algo::kLz4)
  {
    int bound = LZ4_compressBound(static_cast<int>(page_bytes));
    std::vector<uint8_t> tmp(static_cast<size_t>(bound));
    int n = LZ4_compress_default(reinterpret_cast<const char*>(frame),
                                 reinterpret_cast<char*>(tmp.data()), static_cast<int>(page_bytes),
                                 bound);
    if (n > 0 && static_cast<uint32_t>(n) < page_bytes)
    {
      tmp.resize(static_cast<size_t>(n));
      stored = std::move(tmp);
      algo = compress_algo::kLz4;
    }
  }
#else
  (void)prefer;
#endif

  if (algo == compress_algo::kNone)
  {
    stored.assign(frame, frame + page_bytes);
  }

  uint32_t crc = crc32c(stored.data(), stored.size());
  out->clear();
  out->reserve(kDurableBlobHeader + stored.size());
  out->push_back(static_cast<uint8_t>(algo));
  put_u32(out, page_bytes);
  put_u32(out, static_cast<uint32_t>(stored.size()));
  put_u32(out, crc);
  out->insert(out->end(), stored.begin(), stored.end());
}

uint32_t durable_blob_raw_len(const uint8_t* blob, size_t blob_len) {
  if (blob_len < kDurableBlobHeader)
  {
    return 0;
  }
  return get_u32(blob + 1);
}

Status decode_durable_page(const uint8_t* blob, size_t blob_len, uint8_t* frame_out,
                           uint32_t page_bytes) {
  if (blob_len < kDurableBlobHeader)
  {
    return Status::invalid_argument("blob too short");
  }
  compress_algo algo = static_cast<compress_algo>(blob[0]);
  uint32_t raw_len = get_u32(blob + 1);
  uint32_t stored_len = get_u32(blob + 5);
  uint32_t crc = get_u32(blob + 9);
  if (raw_len != page_bytes)
  {
    return Status::corruption("blob raw_len mismatch");
  }
  if (kDurableBlobHeader + stored_len > blob_len)
  {
    return Status::corruption("blob stored_len");
  }
  const uint8_t* stored = blob + kDurableBlobHeader;
  if (crc32c(stored, stored_len) != crc)
  {
    return Status::corruption("blob CRC mismatch");
  }

  if (algo == compress_algo::kNone)
  {
    if (stored_len != page_bytes)
    {
      return Status::corruption("raw stored size");
    }
    std::memcpy(frame_out, stored, page_bytes);
    return Status::Ok();
  }
  if (algo == compress_algo::kLz4)
  {
#if CROWTREE_HAVE_LZ4
    int n = LZ4_decompress_safe(reinterpret_cast<const char*>(stored),
                                reinterpret_cast<char*>(frame_out), static_cast<int>(stored_len),
                                static_cast<int>(page_bytes));
    if (n < 0 || static_cast<uint32_t>(n) != page_bytes)
    {
      return Status::corruption("LZ4 decompress");
    }
    return Status::Ok();
#else
    return Status::not_supported("page compressed with LZ4 but LZ4 not built");
#endif
  }
  return Status::corruption("unknown compression algo");
}

}  // namespace crowtree
