// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// Packed 64-bit mapping-table slot word (plan-tree #14a).
//
// The *same* encoding is used in
// memory and on disk, so a persisted segment image is literally the array of
// packed words and recovery installs them with zero decode:
//
//   word == 0                    -> empty (dead or never-allocated PID)
//   (word & 1) == 0 (and != 0)   -> resident: PageBase* (8-byte aligned) [memory only]
//   (word & 1) == 1              -> unloaded descriptor:
//                                     bits [63:24] iu_index  (durable PageAddr, in IUs)
//                                     bits [23:1]  iu_count   (page length, in IUs)
//                                     bit  [0]     tag = 1
//
// Resident pointers are never persisted; on disk a slot is only `0` or a tagged
// unloaded descriptor. Adopted by the live `MappingTable` (mapping_table.h).
#pragma once

#include "crowtree/page_types.h" // PageBase

#include <cstdint>

namespace crowtree::slot_word
{

// Layout (see header comment). 40 + 23 + 1 = 64 bits, no overlap.
inline constexpr uint64_t kEmpty       = 0;
inline constexpr uint64_t kUnloadedTag = 1;

inline constexpr int kIuCountBits  = 23;
inline constexpr int kIuIndexBits  = 40;
inline constexpr int kIuCountShift = 1;
inline constexpr int kIuIndexShift = 24;

inline constexpr uint64_t kMaxIuIndex = (uint64_t{1} << kIuIndexBits) - 1;
inline constexpr uint32_t kMaxIuCount = (uint32_t{1} << kIuCountBits) - 1;

static_assert(kIuIndexShift == kIuCountShift + kIuCountBits, "iu_count/iu_index adjacent");
static_assert(kIuIndexShift + kIuIndexBits == 64, "packed word is exactly 64 bits");

// ── Classification ────────────────────────────────────────────────
[[nodiscard]] constexpr bool is_empty(uint64_t w)
{
    return w == kEmpty;
}

[[nodiscard]] constexpr bool is_unloaded(uint64_t w)
{
    return (w & kUnloadedTag) != 0;
}

[[nodiscard]] constexpr bool is_resident(uint64_t w)
{
    return w != kEmpty && (w & kUnloadedTag) == 0;
}

// ── Unloaded descriptor ───────────────────────────────────────────
// Precondition: iu_index <= kMaxIuIndex && iu_count <= kMaxIuCount.
[[nodiscard]] constexpr uint64_t pack_unloaded(uint64_t iu_index, uint32_t iu_count)
{
    return (iu_index << kIuIndexShift) | (uint64_t{iu_count} << kIuCountShift) | kUnloadedTag;
}

[[nodiscard]] constexpr uint64_t unloaded_iu_index(uint64_t w)
{
    return w >> kIuIndexShift;
}

[[nodiscard]] constexpr uint32_t unloaded_iu_count(uint64_t w)
{
    return static_cast<uint32_t>((w >> kIuCountShift) & kMaxIuCount);
}

// True when the value fits the unloaded descriptor fields.
[[nodiscard]] constexpr bool fits_unloaded(uint64_t iu_index, uint32_t iu_count)
{
    return iu_index <= kMaxIuIndex && iu_count <= kMaxIuCount;
}

// ── Resident pointer (in-memory only; never persisted) ────────────
[[nodiscard]] inline uint64_t pack_resident(PageBase *p)
{
    return reinterpret_cast<uint64_t>(p); // 8-byte aligned => low bit clear
}

[[nodiscard]] inline PageBase *resident_ptr(uint64_t w)
{
    return reinterpret_cast<PageBase *>(w); // NOLINT(performance-no-int-to-ptr)
}

} // namespace crowtree::slot_word
