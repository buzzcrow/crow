// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-rpc/buffer.h"

#include <cassert>
#include <cstdlib>

namespace crowdb::rpc
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
    if (ref->fetch_sub(1, std::memory_order_acq_rel) == 1) {
        // Last reference — recycle to pool, call external free, or free
        // standalone.
        if (free_cb != nullptr) {
            // External buffer (created via crowdb_rpc_buffer_create_external):
            // the data is owned by an external allocator (e.g. a Rust Vec).
            // free_cb drops the external owner; data itself is NOT freed
            // here (the external owner's destructor handles that).
            free_cb(free_ctx);
            delete ref;
            delete this;
        }
        else if (pool != nullptr) {
            pool->recycle(this);
        }
        else {
            // Standalone buffer (created via crowdb_rpc_buffer_create or
            // frame_to_c_handles): free data, refcount, and Buffer struct.
            // Callers must not access the Buffer after calling release().
            std::free(data);
            delete ref;
            delete this;
        }
    }
}

// ── SystemBufferPool ──────────────────────────────────────────────
//
// Direct heap allocation — no free-list, no mutex. Each alloc does
// posix_memalign + new (Buffer + refcount); each recycle does free +
// delete. glibc per-thread arenas handle small-allocation recycling
// efficiently without a userspace pool.

SystemBufferPool::SystemBufferPool(uint32_t max_buffers) : max_buffers_(max_buffers)
{
}

Buffer *SystemBufferPool::alloc(uint32_t capacity)
{
    if (outstanding_.load(std::memory_order_relaxed) >= max_buffers_) {
        return nullptr; // bound exceeded
    }
    outstanding_.fetch_add(1, std::memory_order_relaxed);

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

void SystemBufferPool::recycle(Buffer *buf)
{
    std::free(buf->data);
    delete buf->ref;
    delete buf;
    outstanding_.fetch_sub(1, std::memory_order_relaxed);
}

} // namespace crowdb::rpc
