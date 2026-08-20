// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "crow-rpc/buffer.h"
#include "crow-rpc/connection.h"
#include "crow-rpc/transport.h"

#ifdef CROW_RPC_HAVE_RDMA

#    include <infiniband/verbs.h>
#    include <rdma/rdma_cma.h>

#    include <atomic>
#    include <memory>
#    include <mutex>
#    include <string>
#    include <thread>
#    include <unordered_map>
#    include <vector>

namespace crow::rpc
{

// RdmaBufferPool: allocates buffers from ibv_mr-registered memory.
// The pool pre-registers a large memory region and carves out buffers
// from it, avoiding per-buffer ibv_reg_mr calls (which are expensive).
// Each buffer's `mr` field points to the sub-region's MR.
class RdmaBufferPool : public BufferPool
{
  public:
    // mr_size: total registered memory size. max_buffers: pool capacity.
    RdmaBufferPool(struct ibv_pd *pd, uint32_t mr_size = 1 << 20, uint32_t max_buffers = 1024);
    ~RdmaBufferPool() override;

    Buffer *alloc(uint32_t capacity) override;
    void    recycle(Buffer *buf) override;

  private:
    struct ibv_pd *pd_;
    struct ibv_mr *mr_;
    uint32_t       mr_size_;
    uint32_t       max_buffers_;
    uint32_t       outstanding_ = 0;

    std::mutex                                          mu_;
    std::unordered_map<uint32_t, std::vector<Buffer *>> free_list_;

    Buffer *alloc_fresh(uint32_t capacity);
};

// RdmaTransport: RDMA transport using libibverbs + librdmacm.
// Implements the Transport interface with ibv_post_send for submit and
// ibv_poll_cq for the completion loop. Connection setup uses librdmacm.
class RdmaTransport : public Transport
{
  public:
    RdmaTransport();
    ~RdmaTransport() override;

    bool    submit(Connection *conn, OutFrame *frame) override;
    Buffer *register_buffer(Buffer *buf) override;
    void    shutdown() override;

    // RDMA-specific: connect to a peer endpoint.
    // Returns a new Connection on success, nullptr on failure.
    std::shared_ptr<Connection> connect(const std::string &addr, int port);

    // RDMA-specific: listen on an endpoint (server side).
    bool listen(const std::string &addr, int port);

    // Start the CQ poll loop on worker threads.
    void start();
    void stop();

  private:
    struct ibv_context        *context_    = nullptr;
    struct ibv_pd             *pd_         = nullptr;
    struct rdma_event_channel *cm_channel_ = nullptr;
    struct rdma_cm_id         *listen_id_  = nullptr;

    std::unique_ptr<RdmaBufferPool> send_pool_;
    std::unique_ptr<RdmaBufferPool> recv_pool_;

    std::atomic<bool> running_{false};
    std::thread       cq_thread_;

    void cq_loop();
};

} // namespace crow::rpc

#endif // CROW_RPC_HAVE_RDMA
