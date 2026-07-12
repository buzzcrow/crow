#include "crowtree/compressor.h"

#include <cstring>

#include "crowtree/crc32c.h"

#if CROWTREE_HAVE_LZ4
// Minimal prototypes (the dev header may be absent; we link the runtime lib).
extern "C" {
int LZ4_compressBound(int inputSize);
int LZ4_compress_default(const char* src, char* dst, int srcSize, int dstCapacity);
int LZ4_decompress_safe(const char* src, char* dst, int compressedSize, int dstCapacity);
}
#endif

namespace crowtree {

namespace {
void PutU32(std::vector<uint8_t>* o, uint32_t v) {
  for (int i = 0; i < 4; ++i) o->push_back(static_cast<uint8_t>((v >> (8 * i)) & 0xff));
}
uint32_t GetU32(const uint8_t* p) {
  uint32_t v = 0;
  for (int i = 0; i < 4; ++i) v |= static_cast<uint32_t>(p[i]) << (8 * i);
  return v;
}
}  // namespace

bool Lz4Available() {
#if CROWTREE_HAVE_LZ4
  return true;
#else
  return false;
#endif
}

void EncodeDurablePage(const uint8_t* frame, uint32_t page_bytes,
                       CompressAlgo prefer, std::vector<uint8_t>* out) {
  CompressAlgo algo = CompressAlgo::kNone;
  std::vector<uint8_t> stored;

#if CROWTREE_HAVE_LZ4
  if (prefer == CompressAlgo::kLz4) {
    int bound = LZ4_compressBound(static_cast<int>(page_bytes));
    std::vector<uint8_t> tmp(static_cast<size_t>(bound));
    int n = LZ4_compress_default(reinterpret_cast<const char*>(frame),
                                 reinterpret_cast<char*>(tmp.data()),
                                 static_cast<int>(page_bytes), bound);
    if (n > 0 && static_cast<uint32_t>(n) < page_bytes) {
      tmp.resize(static_cast<size_t>(n));
      stored = std::move(tmp);
      algo = CompressAlgo::kLz4;
    }
  }
#else
  (void)prefer;
#endif

  if (algo == CompressAlgo::kNone) {
    stored.assign(frame, frame + page_bytes);
  }

  uint32_t crc = Crc32c(stored.data(), stored.size());
  out->clear();
  out->reserve(kDurableBlobHeader + stored.size());
  out->push_back(static_cast<uint8_t>(algo));
  PutU32(out, page_bytes);
  PutU32(out, static_cast<uint32_t>(stored.size()));
  PutU32(out, crc);
  out->insert(out->end(), stored.begin(), stored.end());
}

Status DecodeDurablePage(const uint8_t* blob, size_t blob_len, uint8_t* frame_out,
                         uint32_t page_bytes) {
  if (blob_len < kDurableBlobHeader) return Status::InvalidArgument("blob too short");
  CompressAlgo algo = static_cast<CompressAlgo>(blob[0]);
  uint32_t raw_len = GetU32(blob + 1);
  uint32_t stored_len = GetU32(blob + 5);
  uint32_t crc = GetU32(blob + 9);
  if (raw_len != page_bytes) return Status::Corruption("blob raw_len mismatch");
  if (kDurableBlobHeader + stored_len > blob_len) return Status::Corruption("blob stored_len");
  const uint8_t* stored = blob + kDurableBlobHeader;
  if (Crc32c(stored, stored_len) != crc) return Status::Corruption("blob CRC mismatch");

  if (algo == CompressAlgo::kNone) {
    if (stored_len != page_bytes) return Status::Corruption("raw stored size");
    std::memcpy(frame_out, stored, page_bytes);
    return Status::Ok();
  }
  if (algo == CompressAlgo::kLz4) {
#if CROWTREE_HAVE_LZ4
    int n = LZ4_decompress_safe(reinterpret_cast<const char*>(stored),
                                reinterpret_cast<char*>(frame_out),
                                static_cast<int>(stored_len),
                                static_cast<int>(page_bytes));
    if (n < 0 || static_cast<uint32_t>(n) != page_bytes) {
      return Status::Corruption("LZ4 decompress");
    }
    return Status::Ok();
#else
    return Status::NotSupported("page compressed with LZ4 but LZ4 not built");
#endif
  }
  return Status::Corruption("unknown compression algo");
}

}  // namespace crowtree
