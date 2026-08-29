// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// Epoch-based reclamation (plan-tree #12: lock-free reader fast path).
//
// Readers take a Guard for the duration of a lock-free page walk. The single
// writer retire()s pages it replaces; a retired page is freed only once no
// Guard that could still reference it remains open.
//
// Reader path (enter / Guard::release) is LOCK-FREE: each thread publishes its
// entry epoch into a per-thread, cache-line-padded participant slot via atomics
// only — no mutex on get()/scan(). Reclamation is writer-driven: retire() and
// try_reclaim() scan the participant slots to find the oldest epoch any reader
// might still see and free everything strictly older. The retired list and the
// scan are serialized by reclaim_mu_ (writer-side only, off the hot path).
//
// Memory ordering (crossbeam/folly-style EBR): enter's global-epoch load and
// slot publish, plus retire's epoch bump and the reclaim scan, are all seq_cst,
// giving a single total order in which either the reclaimer observes a reader's
// entry epoch (and keeps the object) or the reader observes the writer's new
// mapping (and never touches the retired object). The participant list itself
// uses acquire/release: a brand-new participant can only be missed by a
// concurrent reclaim for objects retired *before* it entered, which it provably
// cannot reference (the mapping slot was already swapped).
#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <mutex>
#include <vector>

namespace crowdb::tree
{

class EpochManager
{
  private:
    // Per-thread participant slot. Owned by the manager (allocated lazily on a
    // thread's first enter(), linked into participants_, freed at destruction).
    // Cache-line padded so readers on different threads don't false-share.
    struct alignas(64) Participant
    {
        std::atomic<uint64_t> local_epoch{0}; // 0 = inactive (not inside a guard)
        Participant          *next{nullptr};  // intrusive list; published w/ release
        uint32_t              nest{0};        // reentrancy depth (owner thread only)
    };

  public:
    using Deleter = std::function<void(void *)>;

    // RAII reader guard. Holds an epoch open until destroyed. Thread-bound: a
    // Guard must be released on the thread that created it (do not move across
    // threads).
    class Guard
    {
      public:
        Guard() = default;

        explicit Guard(Participant *p) : p_(p)
        {
        }

        Guard(Guard &&o) noexcept : p_(o.p_)
        {
            o.p_ = nullptr;
        }

        Guard &operator=(Guard &&o) noexcept
        {
            if (this != &o) {
                release();
                p_   = o.p_;
                o.p_ = nullptr;
            }
            return *this;
        }

        Guard(const Guard &)            = delete;
        Guard &operator=(const Guard &) = delete;

        ~Guard()
        {
            release();
        }

      private:
        void         release();
        Participant *p_ = nullptr;
    };

    EpochManager();
    ~EpochManager();

    EpochManager(const EpochManager &)            = delete;
    EpochManager &operator=(const EpochManager &) = delete;

    // Open a reader guard at the current epoch (lock-free).
    Guard enter();

    // Defer deletion of `ptr` until no guard opened at-or-before now remains.
    void retire(void *ptr, Deleter deleter);

    // Convenience: retire a typed pointer with `delete`.
    template <class T> void retire_object(T *p)
    {
        retire(p, [](void *x) { delete static_cast<T *>(x); });
    }

    // Force a reclamation sweep. Returns the number of objects freed.
    size_t try_reclaim();

    // Diagnostics.
    [[nodiscard]] size_t pending_retired();
    [[nodiscard]] size_t active_guards();

  private:
    friend class Guard;
    Participant *participant_for_this_thread();
    size_t       reclaim_locked();   // caller holds reclaim_mu_
    uint64_t     min_active_epoch(); // oldest epoch any reader might still see

    struct Retired
    {
        uint64_t epoch;
        void    *ptr;
        Deleter  deleter;
    };

    const uint64_t             id_; // stable key for per-thread cache
    std::atomic<uint64_t>      global_epoch_{1};
    std::atomic<Participant *> participants_{nullptr}; // lock-free push list head

    // Recursive: a retired object's deleter can legitimately trigger another
    // retire() on this *same* EpochManager before this call's reclaim_locked()
    // returns (e.g. retire_orphaned_page's deferred mapping_.clear() can, as
    // its very last live slot empties, recycle the owning MappingSegment via
    // recycle_segment_if_empty() -> epoch.retire_object(seg) -- crowdb-tree.cpp/
    // mapping_table.cpp share this one EpochManager). A plain mutex would
    // deadlock on that same-thread re-entry.
    std::recursive_mutex reclaim_mu_; // guards retired_ (writer side)
    std::vector<Retired> retired_;
};

} // namespace crowdb::tree
