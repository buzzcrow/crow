// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#include "crow-tree/skip_list.h"

#include <algorithm>
#include <array>
#include <cstring>
#include <new>

namespace crow::tree
{

namespace
{
// Branching probability for skip-list height (p=0.25, RocksDB/LevelDB default).
constexpr uint32_t kBranching = 4; // 1/4 probability of height increase
} // namespace

// --- Node allocation ---

Node *ConcurrentSkipList::alloc_node(uint32_t height, Slice key)
{
    size_t sz = Node::alloc_size(height, key.size());
    void  *p  = ::operator new(sz);
    // Construct the Node base (cell_, deleted_, height_, key_len_) with
    // placement new — the atomics need proper initialization, not just raw
    // memory. The tower (next_ptr) is constructed separately below since it
    // lives beyond sizeof(Node).
    Node *n     = new (p) Node{};
    n->height_  = height;
    n->key_len_ = static_cast<uint32_t>(key.size());
    // Construct the tower (next_[0..height-1]) with placement new.
    for (uint32_t i = 0; i < height; ++i) {
        new (n->next_ptr(i)) std::atomic<Node *>(nullptr);
    }
    // Copy the key inline.
    if (!key.empty()) {
        std::memcpy(const_cast<char *>(n->key_data()), key.data(), key.size());
    }
    return n;
}

void ConcurrentSkipList::free_node(void *p)
{
    Node *n = static_cast<Node *>(p);
    // Destroy the atomic objects in the tower, then the Node base, then free.
    for (uint32_t i = 0; i < n->height_; ++i) {
        n->next_ptr(i)->~atomic();
    }
    n->~Node();
    ::operator delete(static_cast<void *>(n));
}

// --- ConcurrentSkipList ---

ConcurrentSkipList::ConcurrentSkipList()
{
    // Head sentinel: max height, empty key. Never deleted.
    head_ = alloc_node(kMaxHeight, Slice());
    max_height_.store(1, std::memory_order_relaxed);
}

ConcurrentSkipList::~ConcurrentSkipList()
{
    // Free all nodes (no readers at destruction). The caller (MemTable) is
    // responsible for retiring nodes via epoch during normal operation; at
    // destruction the list is single-owner again.
    Node *n = head_->next(0);
    while (n != nullptr) {
        Node        *next = n->next(0);
        CellVersion *cv   = n->cell_.load(std::memory_order_relaxed);
        delete cv; // frees the cell buffer (fires drop_fn if kExternal)
        free_node(n);
        n = next;
    }
    free_node(head_);
}

uint32_t ConcurrentSkipList::random_height()
{
    uint32_t h = 1;
    while (h < kMaxHeight && (rng_() % kBranching) == 0) {
        ++h;
    }
    return h;
}

Node *ConcurrentSkipList::find_ge(Slice key, Node **prev) const
{
    Node *x = head_;
    int   h = static_cast<int>(max_height_.load(std::memory_order_relaxed)) - 1;
    while (h >= 0) {
        Node *next = x->next(h);
        while (next != nullptr && next->key_slice().compare(key) < 0) {
            x    = next;
            next = x->next(h);
        }
        if (prev != nullptr) {
            prev[h] = x;
        }
        --h;
    }
    return x->next(0); // first node with key >= `key`, or nullptr
}

bool ConcurrentSkipList::upsert(Slice key, CellVersion *cv, CellVersion **out_old)
{
    SpinlockGuard guard(spinlock_);

    std::array<Node *, kMaxHeight> prev{};
    Node                          *existing = find_ge(key, prev.data());

    if (existing != nullptr && existing->key_slice().compare(key) == 0 &&
        !existing->deleted_.load(std::memory_order_relaxed)) {
        // Overwrite: highest-slot-wins.
        CellVersion *old = existing->cell_.load(std::memory_order_relaxed);
        if (cv->slot <= old->slot) {
            *out_old = nullptr;
            return false; // reject: existing has a >= slot
        }
        existing->cell_.store(cv, std::memory_order_release);
        *out_old = old; // caller retires the old version
        return true;
    }

    // New insert.
    uint32_t h = random_height();
    if (h > max_height_.load(std::memory_order_relaxed)) {
        for (int i = static_cast<int>(max_height_.load(std::memory_order_relaxed)); i < static_cast<int>(h); ++i) {
            prev[i] = head_;
        }
        max_height_.store(h, std::memory_order_relaxed);
    }

    Node *n = alloc_node(h, key);
    n->cell_.store(cv, std::memory_order_release);

    for (uint32_t i = 0; i < h; ++i) {
        n->set_next(i, prev[i]->next(i));
        prev[i]->set_next(i, n);
    }

    count_.fetch_add(1, std::memory_order_relaxed);
    *out_old = nullptr;
    return true;
}

const CellVersion *ConcurrentSkipList::find(Slice key) const
{
    Node *x = head_;
    int   h = static_cast<int>(max_height_.load(std::memory_order_acquire)) - 1;
    while (h >= 0) {
        Node *next = x->next(h); // acquire
        while (next != nullptr && next->key_slice().compare(key) < 0) {
            x    = next;
            next = x->next(h);
        }
        --h;
    }
    Node *cand = x->next(0); // acquire
    if (cand != nullptr && cand->key_slice().compare(key) == 0 && !cand->deleted_.load(std::memory_order_acquire)) {
        return cand->cell_.load(std::memory_order_acquire);
    }
    return nullptr;
}

ConcurrentSkipList::Cursor ConcurrentSkipList::cursor(Slice start_after) const
{
    if (start_after.empty()) {
        // First live node.
        Node *n = head_->next(0);
        while (n != nullptr && n->deleted_.load(std::memory_order_acquire)) {
            n = n->next(0);
        }
        return Cursor(n);
    }
    // Find first node with key > start_after.
    Node *x = head_;
    int   h = static_cast<int>(max_height_.load(std::memory_order_acquire)) - 1;
    while (h >= 0) {
        Node *next = x->next(h);
        while (next != nullptr && next->key_slice().compare(start_after) <= 0) {
            x    = next;
            next = x->next(h);
        }
        --h;
    }
    Node *n = x->next(0);
    while (n != nullptr && n->deleted_.load(std::memory_order_acquire)) {
        n = n->next(0);
    }
    return Cursor(n);
}

void ConcurrentSkipList::Cursor::advance()
{
    if (cur_ == nullptr) {
        return;
    }
    const Node *n = cur_->next(0); // acquire
    while (n != nullptr && n->deleted_.load(std::memory_order_acquire)) {
        n = n->next(0);
    }
    cur_ = n;
}

std::vector<ConcurrentSkipList::DrainedEntry> ConcurrentSkipList::drain_up_to(uint64_t cs)
{
    SpinlockGuard             guard(spinlock_);
    std::vector<DrainedEntry> out;

    // Walk level 0 in key order, unlinking nodes with slot <= cs.
    Node *n = head_->next(0);
    while (n != nullptr) {
        Node *next = n->next(0);
        if (n->deleted_.load(std::memory_order_relaxed)) {
            n = next;
            continue;
        }
        CellVersion *cv = n->cell_.load(std::memory_order_relaxed);
        if (cv->slot > cs) {
            n = next;
            continue;
        }
        // Unlink this node at every level.
        n->deleted_.store(true, std::memory_order_release);
        // Find the predecessors at each level by re-searching from head_.
        // (Simple and correct under spinlock; the list is small enough that
        // re-search is fine — drain is off the hot path.)
        std::array<Node *, kMaxHeight> p{};
        (void)find_ge(n->key_slice(), p.data());
        for (uint32_t i = 0; i < n->height_; ++i) {
            p[i]->set_next(i, n->next(i));
        }
        // Collect the entry.
        out.push_back({.key = n->key_slice().to_string(), .cv = cv, .node = n, .slot = cv->slot});
        count_.fetch_sub(1, std::memory_order_relaxed);
        n = next;
    }
    return out;
}

std::vector<ConcurrentSkipList::DrainedEntry> ConcurrentSkipList::drain_all()
{
    return drain_up_to(UINT64_MAX);
}

} // namespace crow::tree
