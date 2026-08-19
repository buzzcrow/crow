// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "crow-rpc/buffer.h"
#include "crow-rpc/framing.h"

#include <atomic>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace crow::rpc
{

// Forward declaration — Connection::try_send records latency here.
struct TransportStats;

// ── OutFrame: a frame queued for sending ──────────────────────────
//
// The send queue holds OutFrame*. The worker drains up to BATCH_MAX per
// drain cycle and sends them via scatter-gather (writev). request_id is
// assigned by RpcClient::call; 0 for one-way messages.
struct OutFrame
{
    uint64_t request_id = 0;
    Header   header;
    Buffer  *control     = nullptr; // pool-allocated; released after send
    Buffer  *data        = nullptr; // pool-allocated; nullptr if control-only
    uint32_t sent_offset = 0;       // bytes already sent (partial write tracking)
    uint64_t create_nano = 0;       // steady_clock ns at submit/submit_inline
};

constexpr int BATCH_MAX = 64;

// ── Connection: a single peer link ────────────────────────────────
//
// One instance per TCP connection or RDMA QP pair. Transport-agnostic:
// holds the send queue, parser, and pending-request state. The
// transport-specific I/O handle (socket fd for TCP, QP pointer for RDMA)
// is stored as a type-erased transport_handle that only the transport
// interprets.
//
// on_frame_callback is set by the caller (RpcClient on the client side,
// RpcServer on the server side) to dispatch received frames.
class Connection
{
  public:
    using OnFrameCallback = std::function<void(Frame *, Connection *)>;
    using OnCloseCallback = std::function<void(Connection *)>;

    Connection(int64_t id, std::string name, BufferPool *pool, uint32_t max_data_size = 4 << 20);

    int64_t id() const
    {
        return id_;
    }

    const std::string &name() const
    {
        return name_;
    }

    bool is_open() const
    {
        return open_.load(std::memory_order_relaxed);
    }

    // Push a frame to the send queue (called by Transport::submit).
    // Returns true on success, false if the queue is full (backpressure).
    bool enqueue_send(OutFrame *frame);

    // Drain up to max frames from the send queue. Caller owns the returned
    // pointers (must release their buffers after send completes).
    int drain_send_queue(OutFrame **out, int max);

    // Try to send all queued frames via writev directly on the caller's
    // thread (buzz model). Uses in_send_ to serialize: only one thread
    // does writev at a time; others just offer to the queue and return.
    // Returns true if all data was sent, false if partial/EAGAIN (the
    // I/O worker will retry via arm_write).
    bool try_send(int fd, TransportStats *stats);

    // Check if the send queue has pending frames (without draining).
    bool has_pending_send() const
    {
        std::lock_guard<std::mutex> lock(send_mu_);
        return !send_queue_.empty();
    }

    // Close the connection, fail pending requests, signal reconnect.
    void close();

    // Called by the transport's reader when a complete frame arrives.
    // Dispatches to on_frame_callback_.
    void on_frame(Frame *frame);

    // Callbacks (set by RpcClient / RpcServer).
    void set_on_frame(OnFrameCallback cb)
    {
        on_frame_callback_ = std::move(cb);
    }

    void set_on_close(OnCloseCallback cb)
    {
        on_close_callback_ = std::move(cb);
    }

    // Send queue capacity (backpressure bound).
    uint32_t send_queue_capacity() const
    {
        return send_queue_capacity_;
    }

    void set_send_queue_capacity(uint32_t cap)
    {
        send_queue_capacity_ = cap;
    }

    // Transport-specific I/O handle. TcpTransport casts to int (socket fd);
    // RdmaTransport casts to ibv_qp*. The connection itself never uses this.
    uint64_t transport_handle = 0;

    // Back-pointer to the owning SocketEngine (set by Worker::add_connection).
    // SocketTransport::submit uses this to arm write on the correct engine
    // when caller-thread writev hits EAGAIN. Type-erased (void*) to avoid
    // a layering dependency on socket_transport.h.
    void *io_engine = nullptr;

    // User data slot (for caller-side bookkeeping).
    void *user_data = nullptr;

    // The parser for this connection's receive stream.
    FrameParser &parser()
    {
        return parser_;
    }

    // Buffer pool for receive-side allocations (control/data buffers).
    BufferPool *pool() const
    {
        return pool_;
    }

  private:
    int64_t     id_;
    std::string name_;
    BufferPool *pool_;
    FrameParser parser_;

    std::atomic<bool> open_{true};

    // Send queue (mutex-protected for v1; the design's lock-free MPSC is a
    // future optimization). Capacity-bounded for backpressure.
    mutable std::mutex     send_mu_;
    std::deque<OutFrame *> send_queue_;
    uint32_t               send_queue_capacity_ = 256;

    // Caller-thread direct-write flag (buzz model). Only one thread
    // does writev at a time; others just offer to the queue and return.
    std::atomic<bool> in_send_{false};

    OnFrameCallback on_frame_callback_;
    OnCloseCallback on_close_callback_;
};

// ── Transport interface ───────────────────────────────────────────
//
// Isolates the I/O loop divergence between TCP (epoll/kqueue) and RDMA.
// Framing, correlation, pooling, and handler dispatch are shared.
class Transport
{
  public:
    virtual ~Transport() = default;

    // Submit an OutFrame on a connection (non-blocking). Pushes to the
    // send queue and wakes the worker. Returns true on success, false if
    // the queue is full (backpressure) or the connection is closed.
    // The caller (RpcClient) builds the OutFrame with request_id,
    // header, and pool-allocated control/data buffers already set.
    virtual bool submit(Connection *conn, OutFrame *frame) = 0;

    // Register a buffer for this transport. TCP: noop (returns same ptr).
    // RDMA: ibv_reg_mr, returns the MR-backed Buffer.
    virtual Buffer *register_buffer(Buffer *buf) = 0;

    // Shutdown the transport.
    virtual void shutdown() = 0;
};

} // namespace crow::rpc
