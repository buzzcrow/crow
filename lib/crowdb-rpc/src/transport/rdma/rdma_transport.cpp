// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-rpc/transport/rdma/rdma_transport.h"

#ifdef CROWDB_RPC_HAVE_RDMA

#    include <unistd.h>

#    include <cassert>
#    include <cstring>

namespace crowdb::rpc
{

// ── RdmaBufferPool ────────────────────────────────────────────────

RdmaBufferPool::RdmaBufferPool(struct ibv_pd *pd, uint32_t mr_size, uint32_t max_buffers)
    : pd_(pd),
      mr_size_(mr_size),
      max_buffers_(max_buffers)
{
    // Allocate and register a large memory region.
    void *buf = std::malloc(mr_size);
    if (buf == nullptr) {
        return;
    }
    mr_ = ibv_reg_mr(pd_, buf, mr_size,
                     IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ |
                         IBV_ACCESS_REMOTE_ATOMIC);
    if (mr_ == nullptr) {
        std::free(buf);
        return;
    }
}

RdmaBufferPool::~RdmaBufferPool()
{
    std::lock_guard<std::mutex> lock(mu_);
    for (auto &[_, vec] : free_list_) {
        for (Buffer *buf : vec) {
            delete buf->ref;
            delete buf;
        }
    }
    free_list_.clear();
    if (mr_ != nullptr) {
        void *buf = mr_->addr;
        ibv_dereg_mr(mr_);
        std::free(buf);
    }
}

static uint32_t bucket_capacity(uint32_t capacity)
{
    if (capacity == 0)
        return 1;
    --capacity;
    capacity |= capacity >> 1;
    capacity |= capacity >> 2;
    capacity |= capacity >> 4;
    capacity |= capacity >> 8;
    capacity |= capacity >> 16;
    return capacity + 1;
}

Buffer *RdmaBufferPool::alloc_fresh(uint32_t capacity)
{
    if (outstanding_ >= max_buffers_ || mr_ == nullptr) {
        return nullptr;
    }
    ++outstanding_;

    auto *buf     = new Buffer;
    buf->capacity = capacity;
    buf->type     = BufferType::Registered;
    buf->pool     = this;
    buf->ref      = new std::atomic<int32_t>(1);

    // Carve out from the registered memory region.
    // TODO: proper slab allocation from the MR. For now, malloc + reg
    // per buffer (simpler, slower — will be optimized).
    void *ptr = std::malloc(capacity);
    if (ptr == nullptr) {
        delete buf->ref;
        delete buf;
        --outstanding_;
        return nullptr;
    }
    // Register this sub-region.
    struct ibv_mr *sub_mr = ibv_reg_mr(pd_, ptr, capacity, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
    if (sub_mr == nullptr) {
        std::free(ptr);
        delete buf->ref;
        delete buf;
        --outstanding_;
        return nullptr;
    }
    buf->data = static_cast<uint8_t *>(ptr);
    buf->len  = 0;
    // Store the MR pointer in the buffer's pool field (cast — the Buffer
    // struct doesn't have an mr field yet; this is a TODO for the full
    // RDMA implementation).
    return buf;
}

Buffer *RdmaBufferPool::alloc(uint32_t capacity)
{
    uint32_t bucket = bucket_capacity(capacity);
    {
        std::lock_guard<std::mutex> lock(mu_);
        auto                        it = free_list_.find(bucket);
        if (it != free_list_.end() && !it->second.empty()) {
            Buffer *buf = it->second.back();
            it->second.pop_back();
            buf->len = 0;
            buf->ref->store(1, std::memory_order_relaxed);
            return buf;
        }
    }
    return alloc_fresh(bucket);
}

void RdmaBufferPool::recycle(Buffer *buf)
{
    {
        std::lock_guard<std::mutex> lock(mu_);
        uint32_t                    bucket = bucket_capacity(buf->capacity);
        free_list_[bucket].push_back(buf);
    }
    --outstanding_;
}

// ── RdmaTransport ─────────────────────────────────────────────────

RdmaTransport::RdmaTransport()
{
    // TODO: open the IB device, allocate a PD, create the buffer pools.
    // This is the stub — the full implementation requires Linux hardware.
}

RdmaTransport::~RdmaTransport()
{
    shutdown();
}

bool RdmaTransport::submit(Connection * /*conn*/, OutFrame * /*frame*/)
{
    // TODO: build a send WR with the control + data MRs, ibv_post_send.
    return false; // stub
}

Buffer *RdmaTransport::register_buffer(Buffer *buf)
{
    if (buf->type == BufferType::Registered) {
        return buf; // already registered
    }
    // Copy into the send pool (System → Registered).
    // TODO: alloc from send_pool_, copy, release the original.
    return buf; // stub
}

void RdmaTransport::shutdown()
{
    stop();
}

std::shared_ptr<Connection> RdmaTransport::connect(const std::string & /*addr*/, int /*port*/)
{
    // TODO: rdma_create_id, rdma_resolve_addr, rdma_resolve_route,
    // rdma_connect, rdma_create_qp.
    return nullptr; // stub
}

bool RdmaTransport::listen(const std::string & /*addr*/, int /*port*/)
{
    // TODO: rdma_create_id, rdma_bind_addr, rdma_listen.
    return false; // stub
}

void RdmaTransport::start()
{
    running_.store(true, std::memory_order_relaxed);
    // TODO: spawn CQ poll thread.
}

void RdmaTransport::stop()
{
    if (!running_.exchange(false, std::memory_order_acq_rel)) {
        return;
    }
    if (cq_thread_.joinable()) {
        cq_thread_.join();
    }
}

void RdmaTransport::cq_loop()
{
    // TODO: ibv_poll_cq, dispatch send/recv completions.
    // This is the RDMA equivalent of the epoll/kqueue event loop.
}

} // namespace crowdb::rpc

#endif // CROWDB_RPC_HAVE_RDMA
