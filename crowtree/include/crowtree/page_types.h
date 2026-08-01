// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// Shared page primitives: the page-type tag, the
// common chain header, and the leaf entry. Split out of page.h so the frame
// format (frame_page.h) and the page classes (page.h) can both depend on these
// without a circular include.
#pragma once

#include "crowtree/buffer.h"

#include <atomic>
#include <cstdint>
#include <string>

namespace crowtree
{

inline constexpr uint64_t kInvalidPageId = ~0ULL;

enum class page_type : uint8_t {
    kLeafBase      = 1,
    kInnerBase     = 2,
    kBatchDelta    = 3,
    kOverflowFrame = 4, // a chunk of a large value spilled out of a leaf (PT11)
};

inline bool is_base(page_type t)
{
    return t == page_type::kLeafBase || t == page_type::kInnerBase;
}

// Common header for every node reachable through the mapping table. Delta nodes
// chain in front of a base via `next`; bases have next == nullptr.
struct PageBase
{
    explicit PageBase(page_type t) : type(t)
    {
    }

    virtual ~PageBase() = default;

    page_type type;
    PageBase *next        = nullptr;        // chain link: delta -> ... -> base
    uint32_t  delta_len   = 0;              // # of deltas above the base (0 on a base)
    size_t    chain_bytes = 0;              // approx bytes of this node + below (triggers)
    uint64_t  page_id     = kInvalidPageId; // logical page id (set on install)

    // Durable backing of THIS base page's current frame bytes (PT6d). `~0ull`
    // (== kNoAddr in buffer_pool.h) means dirty/anonymous: the live frame is not
    // yet durable. Set on demand-load (clean) and by snapshot after a write;
    // a freshly built page leaves it dirty. A page is snapshot-clean (and thus
    // evictable) iff it is a base, has no deltas above it, and
    // durable_addr != ~0ull. Meaningful only for base pages.
    uint64_t durable_addr = ~0ULL;
    uint32_t durable_plen = 0;

    // Logical-clock stamp of this page's last `Crowtree::resident()` touch
    // (plan-tree #17), used to rank eviction candidates by real recency
    // instead of arbitrary DFS order. Updated with a single relaxed atomic
    // store on every access -- no lock, so the lock-free read path stays
    // lock-free. `0` (never touched since construction) sorts oldest/most
    // evictable, which is correct: a demand-loaded-but-not-yet-re-read page
    // should be at least as evictable as one that's actually been used.
    std::atomic<uint64_t> last_touch_tick{0};

    // R6: cross-thread pin state for handoff paths (get_async slow path,
    // snapshot_view → PinnedSnapshot). Composes with EBR: EBR protects the
    // same-thread walk; pin_state_ extends page lifetime across threads after
    // the walk hands off a borrowed Slice or pinned snapshot. NOT touched on
    // the sync get/scan hot path (EBR only). Bits 0-30 = pin count; bit 31 =
    // kRetiredBit (set by the epoch deleter when EBR has drained). See
    // retire_with_pins() / unpin() for the no-double-free protocol.
    std::atomic<uint32_t>     pin_state_{0};
    static constexpr uint32_t kRetiredBit = 1U << 31;

    // Pin this page for a cross-thread handoff. Only call while the slot is
    // still resident (caller's invariant: the pin path captures the pointer
    // under an epoch guard before the writer can clear the slot).
    void pin()
    {
        pin_state_.fetch_add(1, std::memory_order_acquire);
    }

    // Unpin (drop one cross-thread reference). If this is the last unpin of a
    // retired page, free the page. Safe to call from any thread.
    void unpin()
    {
        uint32_t prev = pin_state_.fetch_sub(1, std::memory_order_acq_rel);
        uint32_t now  = prev - 1;
        if (now == kRetiredBit) {
            delete this;
        }
    }

    // Called by the epoch deleter (after EBR drains). Sets kRetiredBit. If no
    // pins are outstanding (count was 0), frees immediately. Otherwise the
    // last unpin() frees. Safe because after kRetiredBit is set, no new pin()
    // can start (the slot was cleared before retire, so the pin path can't
    // reach this page).
    void retire_with_pins()
    {
        uint32_t prev = pin_state_.fetch_or(kRetiredBit, std::memory_order_acq_rel);
        if (prev == 0) {
            delete this;
        }
    }
};

// One leaf entry: key + encoded slot-aware cell payload (see cell.h). The key is
// std::string (copyable/SSO); the cell is a move-only `buffer` (single-allocation,
// SBO-inline for small cells) so it moves end-to-end from the MemTable into the
// leaf frame with no intermediate copy (plan-tree #5 B2c).
struct leaf_entry
{
    std::string key;
    buffer      cell; // encoded CellView payload
};

} // namespace crowtree
