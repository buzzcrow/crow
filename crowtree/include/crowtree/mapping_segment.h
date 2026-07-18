// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// Standalone mapping-table segment struct (plan-tree #14a).
//
// Holds `slot_count` packed
// slot words (crowtree::slot_word encoding, see mapping_slot.h) plus the
// per-segment bookkeeping segment recycling (#14b, done) and incremental
// persistence (#14c/#14d) need: a live-slot count (0 => recyclable), a
// generation counter (bumped when persisted, for image versioning), the
// image's last-persisted durable location, and a write-vs-persisted
// sequence pair that tells the snapshot path whether this segment has any
// change not yet captured in `image_addr` (see is_dirty()).
//
// `write_seq`/`persisted_seq` replace a plain dirty bool because
// prepare-then-commit snapshotting (persist.cpp) has a gap between
// capturing a segment's image and durably committing it, during which a
// concurrent write can dirty the segment again; a single bool can't tell
// "still exactly the state we imaged" apart from "changed again since". See
// mapping_table.h's commit_segment_persist().
//
// Live in `MappingTable` (mapping_table.h).
#pragma once

#include <atomic>
#include <cstdint>
#include <limits>
#include <memory>

namespace crowtree
{

// Sentinel image_addr meaning "this segment has never been persisted".
inline constexpr uint64_t kNoSegmentImageAddr = std::numeric_limits<uint64_t>::max();

struct MappingSegment
{
    explicit MappingSegment(uint32_t slot_count)
        : slots(std::make_unique<std::atomic<uint64_t>[]>(slot_count)), // NOLINT(modernize-avoid-c-arrays)
          slot_count(slot_count)
    {
        for (uint32_t i = 0; i < slot_count; ++i) {
            slots[i].store(0, std::memory_order_relaxed); // 0 == empty (see mapping_slot.h)
        }
    }

    MappingSegment(const MappingSegment &)            = delete;
    MappingSegment &operator=(const MappingSegment &) = delete;

    // True if any slot has changed since the last successful persist.
    [[nodiscard]] bool is_dirty() const
    {
        return write_seq.load(std::memory_order_relaxed) != persisted_seq.load(std::memory_order_relaxed);
    }

    // NOLINTNEXTLINE(modernize-avoid-c-arrays) runtime-sized array of atomics
    std::unique_ptr<std::atomic<uint64_t>[]> slots; // packed slot words (mapping_slot.h)
    uint32_t                                 slot_count;
    std::atomic<uint32_t>                    live_count{0}; // non-empty slots; 0 => recyclable
    std::atomic<uint64_t>                    generation{0}; // bumped when persisted (image version)

    // Bumped by the writer on every slot mutation (store/store_word/clear).
    std::atomic<uint64_t> write_seq{0};
    // Snapshot of write_seq as of the last successfully *committed* image
    // persist. is_dirty() == (write_seq != persisted_seq).
    std::atomic<uint64_t> persisted_seq{0};

    // Durable location of the image matching `persisted_seq`/`generation`.
    // Writer-only bookkeeping (persist.cpp); no reader ever touches these.
    uint64_t image_addr = kNoSegmentImageAddr;
    uint32_t image_len  = 0;
    uint32_t image_crc  = 0;
};

} // namespace crowtree
