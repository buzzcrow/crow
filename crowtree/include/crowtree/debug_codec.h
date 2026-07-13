// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// Readable debug codec for page frames.
//
// Renders a page frame to a human-readable text form and back to the exact
// bytes (round-trip), for unaligned / file-debug media and for eyeballing the
// on-disk structure. The text carries annotated header + per-slot fields for
// readability plus a `raw` hex line that decode uses to reconstruct the exact
// frame bytes (so the round-trip is exact for any page type, including
// compressed-on-disk pages once they are decoded into a frame).
//
// Key work: frame -> annotated text, text -> exact bytes, hex helpers.
#pragma once

#include "crowtree/status.h"

#include <cstdint>
#include <string>
#include <vector>

namespace crowtree
{

// Encode a `plen`-byte frame into annotated, human-readable text.
std::string encode_frame_text(const uint8_t *frame, uint32_t plen);

// Decode text produced by encode_frame_text back into the exact frame bytes.
// Returns corruption / invalid_argument on a malformed or inconsistent stream.
Status decode_frame_text(const std::string &text, std::vector<uint8_t> *out);

} // namespace crowtree
