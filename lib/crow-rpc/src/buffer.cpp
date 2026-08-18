// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#include "crow-rpc/buffer.h"

#include <cassert>
#include <cstdlib>

namespace crow::rpc
{

// ── Buffer ───────────────────────────────────────────────────────

void Buffer::write(const void *src, uint32_t n)
{
    assert(n <= capacity && "Buffer::write exceeds capacity");
    assert(len == 0 && "Buffer::write called twice (write-once precondition)");
    if (n > 0 && src != nullptr) {
        std::memcpy(data, src, n);
    }
    len = n;
}

Buffer *Buffer::ref_clone()
{
    assert(ref != nullptr && "Buffer::ref_clone on released buffer");
    ref->fetch_add(1, std::memory_order_relaxed);
    return this;
}

void Buffer::release()
{
    if (ref == nullptr) {
        return; // already released or never allocated
    }
    if (ref->fetch_sub(1, std::memory_order_acq_rel) == 1) {
        // Last reference — recycle to pool.
        if (pool != nullptr) {
            pool->recycle(this);
        }
    }
}

// ── SystemBufferPool ──────────────────────────────────────────────

SystemBufferPool::SystemBufferPool(uint32_t max_buffers) : max_buffers_(max_buffers)
{
}

SystemBufferPool::~SystemBufferPool()
{
    // Free all recycled buffers in the free list. Outstanding buffers
    // (not yet released) are leaked — acceptable for shutdown; the caller
    // should release all buffers before pool destruction.
    std::lock_guard<std::mutex> lock(mu_);
    for (auto &[_, vec] : free_list_) {
        for (Buffer *buf : vec) {
            std::free(buf->data);
            delete buf->ref;
            delete buf;
        }
    }
    free_list_.clear();
}

// Round capacity up to the next power-of-2 bucket so alloc/recycle reuse
// matches are frequent (a 130-byte request recycles a 256-byte buffer).
static uint32_t bucket_capacity(uint32_t capacity)
{
    if (capacity == 0) {
        return 1;
    }
    --capacity;
    capacity |= capacity >> 1;
    capacity |= capacity >> 2;
    capacity |= capacity >> 4;
    capacity |= capacity >> 8;
    capacity |= capacity >> 16;
    return capacity + 1;
}

Buffer *SystemBufferPool::alloc_fresh(uint32_t capacity)
{
    if (outstanding_.load(std::memory_order_relaxed) >= max_buffers_) {
        return nullptr; // pool exhausted
    }
    outstanding_.fetch_add(1, std::memory_order_relaxed);
    total_alloc_.fetch_add(1, std::memory_order_relaxed);

    auto *buf     = new Buffer;
    buf->capacity = capacity;
    buf->type     = BufferType::System;
    buf->pool     = this;
    buf->ref      = new std::atomic<int32_t>(1);

    // posix_memalign for cache-line alignment (64 bytes).
    void *ptr = nullptr;
    if (posix_memalign(&ptr, 64, capacity) != 0) {
        delete buf->ref;
        delete buf;
        outstanding_.fetch_sub(1, std::memory_order_relaxed);
        return nullptr;
    }
    buf->data = static_cast<uint8_t *>(ptr);
    buf->len  = 0;
    return buf;
}

Buffer *SystemBufferPool::alloc(uint32_t capacity)
{
    uint32_t bucket = bucket_capacity(capacity);

    // Try the free list first (recycled buffer of the same bucket).
    {
        std::lock_guard<std::mutex> lock(mu_);
        auto                        it = free_list_.find(bucket);
        if (it != free_list_.end() && !it->second.empty()) {
            Buffer *buf = it->second.back();
            it->second.pop_back();
            buf->len = 0;
            buf->ref->store(1, std::memory_order_relaxed);
            // Track as outstanding (in-use) again.
            outstanding_.fetch_add(1, std::memory_order_relaxed);
            return buf;
        }
    }

    // No recycled buffer — allocate fresh. The actual capacity is the
    // bucket size (>= requested), so the caller gets at least what they
    // asked for.
    return alloc_fresh(bucket);
}

void SystemBufferPool::recycle(Buffer *buf)
{
    {
        std::lock_guard<std::mutex> lock(mu_);
        uint32_t                    bucket = bucket_capacity(buf->capacity);
        free_list_[bucket].push_back(buf);
    }
    outstanding_.fetch_sub(1, std::memory_order_relaxed);
    total_recycle_.fetch_add(1, std::memory_order_relaxed);
}

} // namespace crow::rpc
