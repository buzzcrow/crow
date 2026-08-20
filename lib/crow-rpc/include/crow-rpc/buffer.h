// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include <atomic>
#include <cstdint>
#include <cstring>

namespace crow::rpc
{

// Buffer type determines the allocation strategy and memory registration.
// System: aligned system malloc (posix_memalign), recycled to a
// SystemBufferPool. Registered: ibv_reg_mr'd for RDMA, recycled to an
// RdmaBufferPool (Linux + CROW_RPC_HAVE_RDMA only).
enum class BufferType : uint8_t {
    System,
    Registered,
};

// A ref-counted, pool-owned byte buffer. The buffer is allocated from a
// BufferPool, written once, then shared across consumers via ref(). Each
// consumer calls release() when done; the buffer recycles to the pool when
// the last reference drops. The refcount is a separate pool-allocated
// atomic (not embedded in this struct) so multiple Buffer* handles to the
// same allocation share one refcount slot.
//
// Lifecycle: alloc → write once → ref-count down across consumers →
// recycle to pool when ref == 0.
struct Buffer
{
    uint8_t              *data     = nullptr;
    uint32_t              len      = 0; // bytes written (payload length)
    uint32_t              capacity = 0; // allocated capacity
    BufferType            type     = BufferType::System;
    class BufferPool     *pool     = nullptr;
    std::atomic<int32_t> *ref      = nullptr; // shared refcount slot (pool-allocated)

    // Copy src into data, set len. Called once per buffer.
    void write(const void *src, uint32_t n);

    // Increment refcount, return a new Buffer* pointing at the same
    // allocation. Each consumer that needs to hold the buffer calls ref().
    Buffer *ref_clone();

    // Decrement refcount. On ref == 0, recycle to pool.
    void release();
};

// BufferPool allocates and recycles Buffer objects. The pool reuses
// recycled allocations (same capacity bucket) to amortize malloc cost.
// Capacity is bounded; alloc when exhausted returns nullptr.
class BufferPool
{
  public:
    virtual ~BufferPool() = default;

    // Returns a Buffer* with ref == 1, len == 0. nullptr if the pool is
    // exhausted (capacity bound reached).
    virtual Buffer *alloc(uint32_t capacity) = 0;

    // Called by Buffer::release when ref == 0. Returns the buffer to the
    // free list for reuse.
    virtual void recycle(Buffer *buf) = 0;
};

// SystemBufferPool: direct heap allocation (posix_memalign + new/delete),
// no free-list recycling. Matches buzz-cpp's approach — glibc per-thread
// arenas handle small-allocation recycling efficiently without a userspace
// pool. The max_buffers bound limits outstanding allocations; alloc returns
// nullptr when exceeded. Default pool for TCP transport.
class SystemBufferPool : public BufferPool
{
  public:
    // max_buffers: outstanding allocation bound (safety valve).
    SystemBufferPool(uint32_t max_buffers = 8192);
    ~SystemBufferPool() override = default;

    Buffer *alloc(uint32_t capacity) override;
    void    recycle(Buffer *buf) override;

  private:
    uint32_t              max_buffers_;
    std::atomic<uint32_t> outstanding_{0}; // in-use buffers (allocated, not yet freed)
};

} // namespace crow::rpc
