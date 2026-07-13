// Page compression for durable page blobs.
//
// Compression is on-disk only: a frame body is compressed at write_page time and
// decompressed into a full frame at read_page time, so the buffer pool always
// holds uncompressed frames and in-memory access stays zero-copy. LZ4 is the
// default; when the LZ4 library is unavailable at build time the codec degrades
// to identity (store raw), so callers always work.
//
// Durable compressed-page blob (what a backend writes for one page):
//   [u8 algo][u32 raw_len][u32 stored_len][u32 crc32c(stored)][stored bytes...]
// crc32c is over the stored (possibly compressed) bytes so torn-write detection
// is independent of the codec.
//
// Key work: algo enum, LZ4/none codecs, durable blob encode/decode.
#pragma once

#include "crowtree/status.h"

#include <cstdint>
#include <vector>

namespace crowtree
{

enum class compress_algo : uint8_t { kNone = 0, kLz4 = 1 };

// 13-byte durable blob header (algo + raw_len + stored_len + crc).
inline constexpr size_t kDurableBlobHeader = 1 + 4 + 4 + 4;

// True if LZ4 was linked in this build.
[[nodiscard]] bool lz4_available();

// Encode `frame[0,page_bytes)` into a durable blob, compressing with `prefer`
// when it actually shrinks the page (else stored raw with algo = kNone).
void encode_durable_page(const uint8_t *frame, uint32_t page_bytes, compress_algo prefer, std::vector<uint8_t> *out);

// Read the logical (raw, uncompressed) frame length recorded in a durable
// blob's header, so a reader can size the decode buffer without knowing the
// frame geometry up front. Returns 0 if the blob is shorter than the header.
[[nodiscard]] uint32_t durable_blob_raw_len(const uint8_t *blob, size_t blob_len);

// Read the logical (unpadded) on-disk blob length = header + stored payload.
// This is the value the manifest records and store_unloaded re-tags. Returns 0
// if the blob is shorter than the header.
[[nodiscard]] uint32_t durable_blob_logical_len(const uint8_t *blob, size_t blob_len);

// Decode a durable blob back into exactly `page_bytes` at `frame_out`. Verifies
// the stored-bytes CRC and the decompressed length. Returns corruption on any
// mismatch, kInvalidArgument on a short/garbled header.
Status decode_durable_page(const uint8_t *blob, size_t blob_len, uint8_t *frame_out, uint32_t page_bytes);

} // namespace crowtree
