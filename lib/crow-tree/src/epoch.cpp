// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#include "crow-tree/epoch.h"

#include <utility>
#include <vector>

namespace crow::tree
{

namespace
{
// Monotonic id source so a thread's per-manager cache keys never collide, even
// if a manager's heap address is reused after destruction.
std::atomic<uint64_t> g_epoch_mgr_id{1};
} // namespace

// ── Reader hot path (lock-free) ───────────────────────────────────

EpochManager::Participant *EpochManager::participant_for_this_thread()
{
    // Process-global per-thread cache of (manager id -> this thread's slot). A
    // thread that touches several managers keeps one entry each; entries for a
    // destroyed manager (unique id) are simply never matched again.
    static thread_local std::vector<std::pair<uint64_t, Participant *>> cache;
    for (auto &e : cache) {
        if (e.first == id_) {
            return e.second;
        }
    }
    // First use on this thread: allocate our slot and push it (lock-free) onto the
    // manager's participant list. The node lives until the manager is destroyed.
    auto        *p    = new Participant();
    Participant *head = participants_.load(std::memory_order_relaxed);
    do {
        p->next = head;
    } while (!participants_.compare_exchange_weak(head, p, std::memory_order_release, std::memory_order_relaxed));
    cache.emplace_back(id_, p);
    return p;
}

EpochManager::Guard EpochManager::enter()
{
    Participant *p = participant_for_this_thread();
    // Reentrant: only the outermost enter publishes the entry epoch.
    if (p->nest++ == 0) {
        // seq_cst pair: publish our entry epoch before any subsequent shared load,
        // so a concurrent reclaimer either sees this epoch (keeps the object) or we
        // see its swapped-in mapping (never touch the retired object).
        uint64_t e = global_epoch_.load(std::memory_order_seq_cst);
        p->local_epoch.store(e, std::memory_order_seq_cst);
    }
    return Guard(p);
}

void EpochManager::Guard::release()
{
    if (p_ != nullptr) {
        // Only the outermost guard clears the slot. Owner-thread only, so `nest` is
        // a plain field. No reclamation here — that is writer-driven.
        if (--p_->nest == 0) {
            p_->local_epoch.store(0, std::memory_order_release);
        }
        p_ = nullptr;
    }
}

// ── Writer side (reclaim_mu_) ─────────────────────────────────────

EpochManager::EpochManager() : id_(g_epoch_mgr_id.fetch_add(1, std::memory_order_relaxed))
{
}

EpochManager::~EpochManager()
{
    // By destruction time no guards must remain. Free anything still pending, then
    // free the participant nodes. Drain in a loop (not once): a deleter can
    // retire something else on this same manager (see reclaim_locked()'s doc
    // comment), which re-populates retired_ -- keep detaching+running until
    // nothing's left, so a nested retirement's deleter still runs instead of
    // being silently dropped.
    {
        std::lock_guard<std::recursive_mutex> lk(reclaim_mu_);
        while (!retired_.empty()) {
            std::vector<Retired> pending;
            pending.swap(retired_);
            for (auto &r : pending) {
                try {
                    r.deleter(r.ptr);
                }
                catch (...) { // NOLINT(bugprone-empty-catch)
                    // Destructors must not throw; swallow deleter exceptions.
                }
            }
        }
    }
    Participant *p = participants_.load(std::memory_order_acquire);
    while (p != nullptr) {
        Participant *next = p->next;
        delete p;
        p = next;
    }
}

uint64_t EpochManager::min_active_epoch()
{
    // Oldest epoch any active reader might still be inside. seq_cst loads pair with
    // enter()'s publish for the total-order argument above.
    uint64_t min_active = global_epoch_.load(std::memory_order_seq_cst);
    for (Participant *p = participants_.load(std::memory_order_acquire); p != nullptr; p = p->next) {
        uint64_t e = p->local_epoch.load(std::memory_order_seq_cst);
        if (e != 0 && e < min_active) {
            min_active = e;
        }
    }
    return min_active;
}

void EpochManager::retire(void *ptr, Deleter deleter)
{
    std::lock_guard<std::recursive_mutex> lk(reclaim_mu_);
    // New retirements belong to the current epoch; bump so a fresh guard entering
    // after this cannot claim to predate the retirement.
    retired_.push_back(
        {.epoch = global_epoch_.load(std::memory_order_relaxed), .ptr = ptr, .deleter = std::move(deleter)});
    global_epoch_.fetch_add(1, std::memory_order_seq_cst);
    reclaim_locked();
}

size_t EpochManager::reclaim_locked()
{
    uint64_t min_active = min_active_epoch();
    // Detach the current retired list *before* running any deleter: a
    // deleter can legitimately call retire() again on this same manager
    // (e.g. retire_orphaned_page's deferred mapping_.clear() recycling its
    // now-empty MappingSegment via epoch.retire_object(seg)) -- reclaim_mu_
    // being recursive lets that nested call proceed on the same thread, but
    // it would still corrupt an in-progress `for (auto &r : retired_)` (the
    // nested call's own push_back/swap reallocates or replaces the very
    // vector this loop is iterating). Iterating a local, already-swapped-out
    // copy instead means a nested call only ever touches `retired_` itself
    // (empty at that point, plus whatever this loop has re-added so far),
    // never this loop's iteration state.
    std::vector<Retired> pending;
    pending.swap(retired_);
    size_t freed = 0;
    for (auto &r : pending) {
        if (r.epoch < min_active) {
            r.deleter(r.ptr);
            ++freed;
        }
        else {
            retired_.push_back(std::move(r));
        }
    }
    return freed;
}

size_t EpochManager::try_reclaim()
{
    std::lock_guard<std::recursive_mutex> lk(reclaim_mu_);
    return reclaim_locked();
}

size_t EpochManager::pending_retired()
{
    std::lock_guard<std::recursive_mutex> lk(reclaim_mu_);
    return retired_.size();
}

size_t EpochManager::active_guards()
{
    // Threads currently inside a guard (per-thread, not per-nested-guard).
    size_t n = 0;
    for (Participant *p = participants_.load(std::memory_order_acquire); p != nullptr; p = p->next) {
        if (p->local_epoch.load(std::memory_order_seq_cst) != 0) {
            ++n;
        }
    }
    return n;
}

} // namespace crow::tree
