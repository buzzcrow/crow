// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "crow-common/mpsc_queue.h"
#include "crow-rpc/buffer.h"
#include "crow-rpc/framing.h"
#include "crow-rpc/transport.h"

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>

namespace crow::rpc
{

// Lock-free bounded MPSC ring buffer for the per-connection send queue.
// Alias of the shared crow-common primitive specialized for OutFrame*.
using SendQueue = crow::common::MpscQueue<OutFrame *>;

// Forward declaration — Connection::try_send records latency here.
struct TransportStats;

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
    // thread. Uses in_send_ to serialize: only one thread
    // does writev at a time; others just offer to the queue and return.
    // Returns true if all data was sent, false if partial/EAGAIN (the
    // I/O worker will retry via arm_write).
    bool try_send(int fd, TransportStats *stats);

    // Check if the send queue has pending frames (without draining).
    // Lock-free; conservative — may return true when a producer has
    // claimed a slot but not yet filled it.
    bool has_pending_send() const
    {
        return send_queue_.has_pending();
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

    // Send queue capacity (backpressure bound, fixed at construction).
    uint32_t send_queue_capacity() const
    {
        return send_queue_.capacity();
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

    // Lock-free bounded MPSC ring buffer (crow::common::MpscQueue). Multiple
    // producer threads push via enqueue_send (Transport::submit); the single
    // consumer (whichever thread holds in_send_) drains via drain_send_queue
    // for writev. Defined in crow-common/cpp/include/crow-common/mpsc_queue.h.
    SendQueue send_queue_;

    // Caller-thread direct-write flag. Only one thread
    // does writev at a time; others just offer to the queue and return.
    std::atomic<bool> in_send_{false};

    OnFrameCallback on_frame_callback_;
    OnCloseCallback on_close_callback_;
};

} // namespace crow::rpc
