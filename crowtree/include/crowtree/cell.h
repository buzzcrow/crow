// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// Slot-aware value cell.
//
//   cell_payload := [slot: u64 LE][flags: u8][value bytes...]
//   flags bit0 = tombstone (1 -> value bytes empty)
//   flags bit1..7 reserved
//
// This is the real on-the-wire byte format (semantics depend on it), used both
// in MemTable and in leaf/delta storage. Highest-slot-wins is resolved via the
// slot field.
#pragma once

#include "crowtree/buffer.h"
#include "crowtree/slice.h"

#include <array>
#include <cstdint>
#include <cstring>
#include <string>

namespace crowtree
{

enum class OpKind : uint8_t { kPut = 0, kDelete = 1 };

inline constexpr uint8_t kFlagTombstone  = 0x1;
inline constexpr uint8_t kFlagOverflow   = 0x2;                                // value spilled to an overflow chain
inline constexpr size_t  kCellHeaderSize = sizeof(uint64_t) + sizeof(uint8_t); // 9
// Overflow pointer cell body: [slot u64][flags u8][head_page_id u64][total_len u64].
inline constexpr size_t kOverflowCellSize = kCellHeaderSize + (sizeof(uint64_t) * 2); // 25

// Encode a cell payload into `out` (appends). Tombstone cells carry no value.
inline void encode_cell_into(std::string *out, uint64_t slot, OpKind kind, Slice value)
{
    uint8_t                           flags = (kind == OpKind::kDelete) ? kFlagTombstone : 0;
    std::array<char, kCellHeaderSize> hdr{};
    for (int i = 0; i < 8; ++i) {
        hdr[i] = static_cast<char>((slot >> (8 * i)) & 0xff);
    }
    hdr[8] = static_cast<char>(flags);
    out->append(hdr.data(), kCellHeaderSize);
    if (kind != OpKind::kDelete && !value.empty()) {
        out->append(value.data(), value.size());
    }
}

[[nodiscard]] inline std::string encode_cell(uint64_t slot, OpKind kind, Slice value = Slice())
{
    std::string s;
    encode_cell_into(&s, slot, kind, value);
    return s;
}

// Encode an overflow pointer cell: a live value whose bytes live in an overflow
// page chain headed at `head_page_id`, totaling `total_len` bytes.
[[nodiscard]] inline std::string encode_overflow_cell(uint64_t slot, uint64_t head_page_id, uint64_t total_len)
{
    std::string s;
    for (int i = 0; i < 8; ++i) {
        s.push_back(static_cast<char>((slot >> (8 * i)) & 0xff));
    }
    s.push_back(static_cast<char>(kFlagOverflow));
    for (int i = 0; i < 8; ++i) {
        s.push_back(static_cast<char>((head_page_id >> (8 * i)) & 0xff));
    }
    for (int i = 0; i < 8; ++i) {
        s.push_back(static_cast<char>((total_len >> (8 * i)) & 0xff));
    }
    return s;
}

// buffer variants of the encoders (plan-tree #5 B2a): the encoded cell is one
// contiguous allocation — the 9-byte header is written into the reserved prefix and
// the value copied right after it, so there is no separate cell-header allocation.
// Byte-for-byte identical to the std::string encoders above; `CellView` reads the
// resulting `buffer::slice()` unchanged.
[[nodiscard]] inline buffer encode_cell_buf(uint64_t slot, OpKind kind, Slice value = Slice())
{
    uint8_t  flags = (kind == OpKind::kDelete) ? kFlagTombstone : 0;
    size_t   vlen  = (kind != OpKind::kDelete) ? value.size() : 0;
    buffer   b     = buffer::alloc(/*capacity=*/vlen, /*header_reserve=*/kCellHeaderSize);
    uint8_t *p     = b.data(); // [0,9) header, [9, 9+vlen) value — one block
    for (int i = 0; i < 8; ++i) {
        p[i] = static_cast<uint8_t>((slot >> (8 * i)) & 0xff);
    }
    p[8] = flags;
    if (vlen > 0) {
        std::memcpy(p + kCellHeaderSize, value.data(), vlen);
    }
    return b; // size() is already kCellHeaderSize + vlen
}

[[nodiscard]] inline buffer encode_overflow_cell_buf(uint64_t slot, uint64_t head_page_id, uint64_t total_len)
{
    buffer   b = buffer::alloc(kOverflowCellSize); // 25 bytes: all metadata, no value
    uint8_t *p = b.data();
    for (int i = 0; i < 8; ++i) {
        p[i] = static_cast<uint8_t>((slot >> (8 * i)) & 0xff);
    }
    p[8] = kFlagOverflow;
    for (int i = 0; i < 8; ++i) {
        p[kCellHeaderSize + i] = static_cast<uint8_t>((head_page_id >> (8 * i)) & 0xff);
    }
    for (int i = 0; i < 8; ++i) {
        p[kCellHeaderSize + 8 + i] = static_cast<uint8_t>((total_len >> (8 * i)) & 0xff);
    }
    return b;
}

// Decoded, non-owning view over an encoded cell payload.
class CellView
{
  public:
    CellView() = default;

    explicit CellView(Slice raw) : raw_(raw)
    {
    }

    [[nodiscard]] bool valid() const
    {
        return raw_.size() >= kCellHeaderSize;
    }

    [[nodiscard]] uint64_t slot() const
    {
        uint64_t       s = 0;
        const uint8_t *p = raw_.bytes();
        for (int i = 0; i < 8; ++i) {
            s |= static_cast<uint64_t>(p[i]) << (8 * i);
        }
        return s;
    }

    [[nodiscard]] uint8_t flags() const
    {
        return raw_.bytes()[8];
    }

    [[nodiscard]] bool is_tombstone() const
    {
        return (flags() & kFlagTombstone) != 0;
    }

    [[nodiscard]] bool is_overflow() const
    {
        return (flags() & kFlagOverflow) != 0;
    }

    [[nodiscard]] OpKind kind() const
    {
        return is_tombstone() ? OpKind::kDelete : OpKind::kPut;
    }

    // Inline value bytes. Only valid for a normal (non-overflow) cell; overflow
    // cells store a pointer instead (see overflow_head/overflow_len).
    [[nodiscard]] Slice value() const
    {
        if (is_overflow() || raw_.size() <= kCellHeaderSize) {
            return {};
        }
        return {raw_.data() + kCellHeaderSize, raw_.size() - kCellHeaderSize};
    }

    // Overflow pointer accessors (only meaningful when is_overflow()).
    [[nodiscard]] uint64_t overflow_head() const
    {
        const uint8_t *p = raw_.bytes() + kCellHeaderSize;
        uint64_t       v = 0;
        for (int i = 0; i < 8; ++i) {
            v |= static_cast<uint64_t>(p[i]) << (8 * i);
        }
        return v;
    }

    [[nodiscard]] uint64_t overflow_len() const
    {
        const uint8_t *p = raw_.bytes() + (kCellHeaderSize + 8);
        uint64_t       v = 0;
        for (int i = 0; i < 8; ++i) {
            v |= static_cast<uint64_t>(p[i]) << (8 * i);
        }
        return v;
    }

    [[nodiscard]] Slice raw() const
    {
        return raw_;
    }

  private:
    Slice raw_;
};

// Highest-slot-wins: returns true if `a` shadows `b` (a is the resolved cell).
// On equal slots the cells must be identical writes (same batch); we keep `a`.
[[nodiscard]] inline bool cell_wins(const CellView &a, const CellView &b)
{
    return a.slot() >= b.slot();
}

} // namespace crowtree
