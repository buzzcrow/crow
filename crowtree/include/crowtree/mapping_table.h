// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// Mapping table.
//
// PID -> atomic<uint64_t> packed slot word (plan-tree #14b). All structural
// references (root, sibling links, inner children) are PIDs, so a page can be
// replaced by swapping one slot. Readers do a lock-free atomic load; the single
// writer (flusher) does a plain atomic store (no CAS, per D2). PID allocation
// is mutex-guarded.
//
// Slot word encoding: see mapping_slot.h (crowtree::slot_word).
//   word == 0                 -> empty (dead or never-allocated PID)
//   (word & 1) == 0 (and !=0) -> resident: PageBase* (8-byte aligned)
//   (word & 1) == 1           -> unloaded: iu_index/iu_count packed descriptor
//
// The unloaded descriptor is inline in the word (no heap allocation), unlike the
// old tagged-pointer scheme that allocated an unloaded_page struct.
#pragma once

#include "crowtree/epoch.h"
#include "crowtree/mapping_segment.h"
#include "crowtree/mapping_slot.h"
#include "crowtree/page.h"

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <vector>

namespace crowtree
{

class MappingTable
{
  public:
    static constexpr uint64_t kSegmentSize = 1024;     // PIDs per segment
    static constexpr uint64_t kMaxSegments = 1U << 16; // -> 64M PIDs

    MappingTable();
    ~MappingTable();

    MappingTable(const MappingTable &)            = delete;
    MappingTable &operator=(const MappingTable &) = delete;

    // Reader: lock-free atomic load of the raw packed slot word.
    // Use slot_word::is_empty / is_resident / is_unloaded to classify.
    [[nodiscard]] uint64_t get_word(uint64_t page_id) const;

    // Convenience: returns the resident PageBase* if the slot is resident,
    // nullptr for empty or unloaded slots.
    [[nodiscard]] PageBase *get_resident(uint64_t page_id) const;

    // Writer: store a resident page pointer (packs it into a slot word).
    void store(uint64_t page_id, PageBase *page);

    // Writer: store a pre-packed slot word (e.g. an unloaded descriptor from
    // slot_word::pack_unloaded, or slot_word::kEmpty to clear).
    void store_word(uint64_t page_id, uint64_t word);

    // Install an unloaded (on-disk, not-resident) descriptor for `page_id`
    // (recovery / eviction). Converts raw (addr, plen) to a packed word using
    // the store's indivisible unit size `iu`.
    void store_unloaded(uint64_t page_id, uint64_t addr, uint32_t plen, uint32_t iu);

    // Clear a slot to empty (slot_word::kEmpty).
    void clear(uint64_t page_id);

    // Wire the epoch manager used to safely reclaim segments that recycle to
    // empty. Call once,
    // before any concurrent readers can observe the table. If never called,
    // recycled segments are `delete`d directly on the writer thread -- fine
    // for a standalone table with no concurrent readers (e.g. unit tests),
    // unsafe otherwise.
    void set_epoch_manager(EpochManager *epoch)
    {
        epoch_ = epoch;
    }

    // Allocate a fresh PID. Monotonic -- PIDs are never recycled (plan-tree
    // #14 D1: a reused PID could be seen by a stale reader as the new page,
    // i.e. silent wrong data).
    uint64_t allocate_page_id();

    // Number of segments currently allocated (diagnostics).
    [[nodiscard]] size_t segments_allocated() const;

    // Recovery: resume fresh PID allocation past the highest persisted PID.
    void                   set_next_page_id(uint64_t next);
    [[nodiscard]] uint64_t next_page_id() const;

    // -- #14c/#14d: segment-image persistence (persist.cpp) --------------
    //
    // Raw atomic load of the top-level segment pointer; nullptr if `seg_idx`
    // was never allocated or has been recycled (#14b). The snapshot path
    // (persist.cpp) uses this to enumerate every present segment (bounded by
    // kMaxSegments, cheap) and inspect is_dirty()/slots[] directly -- no
    // separate iteration API needed since MappingSegment's fields are
    // public.
    [[nodiscard]] MappingSegment *segment_at(uint64_t seg_idx) const;

    // Commit phase for a snapshot's segment-image write (mirrors the page
    // write identity check in commit_prepared_snapshot). Call once the
    // segment's image bytes are durable. `expected` and `seen_write_seq` are
    // whatever prepare captured *before* releasing write_mutex_ for the I/O
    // phase (see MappingSegment's doc comment). Returns false (no-op) if
    // either changed in the meantime -- the segment either recycled away or
    // was written to again, so the image we just persisted is stale; the
    // segment stays dirty and the next snapshot re-images it. Never loses
    // data: at worst it wastes one redundant re-image later.
    bool commit_segment_persist(uint64_t seg_idx, MappingSegment *expected, uint64_t seen_write_seq,
                                uint64_t new_generation, uint64_t new_image_addr, uint32_t new_image_len,
                                uint32_t new_image_crc);

    // Recovery: install a segment's full slot-word array plus its
    // bookkeeping directly (no live_count/write_seq transition logic --
    // recovery has no concurrent readers yet). `words.size()` must equal
    // kSegmentSize (the only slot count this table currently supports; see
    // Options::mapping_segment_slots's TODO).
    void install_recovered_segment(uint64_t seg_idx, uint64_t generation, uint32_t live_count,
                                   const std::vector<uint64_t> &words, uint64_t image_addr, uint32_t image_len,
                                   uint32_t image_crc);

  private:
    MappingSegment *ensure_segment(uint64_t seg_idx);

    // Called by the writer when a slot's last live entry in `seg` goes empty
    // (plan-tree #14b). CAS's the top-level slot to nullptr -- final and safe
    // because PIDs are monotonic (D1), so a freed segment's PID range is dead
    // forever and no allocation will ever revisit `seg_idx` -- then hands the
    // segment to the epoch manager (if wired) so any in-flight lock-free
    // reader that already loaded `seg` keeps a valid pointer until its guard
    // drains.
    void recycle_segment_if_empty(uint64_t seg_idx, MappingSegment *seg);

    std::vector<std::atomic<MappingSegment *>> segments_; // fixed-size top-level array

    mutable std::mutex alloc_mu_;
    uint64_t           next_page_id_ = 0;

    EpochManager *epoch_ = nullptr; // set via set_epoch_manager; not owned
};

} // namespace crowtree
