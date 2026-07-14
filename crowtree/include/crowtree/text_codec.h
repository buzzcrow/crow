// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// Text codecs for Crowtree durable objects (anchor, segment image, segment
// directory). Each codec encodes a binary blob to human-readable text and
// decodes it back to the exact original bytes (round-trip).
//
// Used by TextPageStore (Task 4) for the debug file backend. The text format
// is self-describing with a magic header line, annotated fields, and a raw
// hex trailer for exact round-trip.
#pragma once

#include "crowtree/status.h"

#include <cstdint>
#include <string>
#include <vector>

namespace crowtree
{

// ── Anchor text codec ─────────────────────────────────────────────
// The anchor is encoded in persist.cpp's CommitAnchor struct. The text codec
// works on the raw binary blob (the encoded anchor bytes, not the struct) so
// it can be used by TextPageStore without pulling in persist.cpp internals.

// Encode an anchor blob (binary bytes from encode_anchor) to text.
std::string encode_anchor_text(const uint8_t *buf, size_t len);

// Decode anchor text back to the exact binary blob.
Status decode_anchor_text(const std::string &text, std::vector<uint8_t> *out);

// ── Segment image text codec ──────────────────────────────────────

// Encode a segment image blob to text.
std::string encode_seg_image_text(const uint8_t *buf, size_t len);

// Decode segment image text back to the exact binary blob.
Status decode_seg_image_text(const std::string &text, std::vector<uint8_t> *out);

// ── Segment directory text codec ──────────────────────────────────

// Encode a segment directory blob to text.
std::string encode_segdir_text(const uint8_t *buf, size_t len);

// Decode segment directory text back to the exact binary blob.
Status decode_segdir_text(const std::string &text, std::vector<uint8_t> *out);

} // namespace crowtree
