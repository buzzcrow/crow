// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// On-disk mapping-table formats: segment image + segment directory
// (plan-tree #14c).
//
// Both formats are backend-neutral byte blobs (no fixed file region) --
// callers allocate/write/read them through PageStore like any other durable
// blob. The commit anchor (§7.3) that ties a snapshot generation to a
// segment-directory address is encoded/decoded in persist.cpp, reusing (and
// extending) the existing superblock A/B machinery rather than duplicating
// it here.
//
// Segment image: a dirty MappingSegment's full packed-word array, verbatim
// (crowdb::tree::slot_word encoding, mapping_slot.h) -- recovery installs the
// words into slots[] with zero decode (design's point: the mapping table IS
// the persistent structure).
//
// Segment directory: the list of every *live* segment's latest generation +
// image location, rewritten in full whenever any segment's generation
// changes. Small (O(live segments), not O(resident pages)).
#pragma once

#include "crowdb-tree/status.h"

#include <cstdint>
#include <vector>

namespace crowdb::tree
{

// ── Segment image ─────────────────────────────────────────────

struct SegmentImageHeader
{
    uint32_t seg_idx    = 0;
    uint64_t generation = 0;
    uint32_t slot_count = 0;
    uint32_t live_count = 0;
    // Decode-only: the body CRC decode_segment_image already validated
    // internally, surfaced so a caller holding a DirEntry can additionally
    // cross-check DirEntry::image_crc against the image it actually read
    // (catches a directory pointing at the wrong/stale image address, which
    // a bare "did this blob decode" check can't). Left 0 by
    // encode_segment_image (only decode fills it in).
    uint32_t body_crc = 0;
};

// Encodes `words` (exactly `hdr.slot_count` packed slot words, mapping_slot.h
// encoding) as a self-describing, CRC-protected image. Appends to `*out`
// (does not clear it first) so callers can size/allocate from the result.
// `out_body_crc`, if non-null, receives the body-only CRC (over the packed
// words, not the header) -- callers persisting a segment directory entry
// need exactly this value (DirEntry::image_crc) without re-reading the
// image back from the store.
void encode_segment_image(const SegmentImageHeader &hdr, const std::vector<uint64_t> &words, std::vector<uint8_t> *out,
                          uint32_t *out_body_crc = nullptr);

// Decodes an image produced by encode_segment_image. Validates the header
// CRC, the magic, and the body CRC; `buf`/`len` may be longer than the
// logical image (e.g. IU-padded) -- only the first
// `header + slot_count*8 + 4` bytes are consumed.
Status decode_segment_image(const uint8_t *buf, size_t len, SegmentImageHeader *hdr_out,
                            std::vector<uint64_t> *words_out);

// Exact encoded size for a `slot_count`-word image (header + body + trailer).
// Callers use this to size the allocation before encode_segment_image runs.
[[nodiscard]] size_t segment_image_encoded_size(uint32_t slot_count);

// ── Segment directory ──────────────────────────────────────────

struct DirEntry
{
    uint32_t seg_idx    = 0;
    uint64_t generation = 0;
    uint64_t image_addr = 0;
    uint32_t image_len  = 0; // logical (unpadded) length of the segment image
    uint32_t image_crc  = 0; // the image's own body_crc, duplicated here so a
                             // directory-only scan can sanity-check without
                             // re-reading every image
};

// Encodes the full directory (every live segment). Appends to `*out`.
void encode_segment_directory(const std::vector<DirEntry> &entries, std::vector<uint8_t> *out);

Status decode_segment_directory(const uint8_t *buf, size_t len, std::vector<DirEntry> *entries_out);

} // namespace crowdb::tree
