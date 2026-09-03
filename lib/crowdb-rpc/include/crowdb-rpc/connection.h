// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "crowdb-common/mpsc_queue.h"
#include "crowdb-rpc/buffer.h"
#include "crowdb-rpc/framing.h"
#include "crowdb-rpc/transport.h"

#include <sys/uio.h>

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>

namespace crowdb::rpc
{

// Lock-free bounded MPSC ring buffer for the per-connection send queue.
// Alias of the shared crowdb-common primitive specialized for OutFrame*.
using SendQueue = crowdb::common::MpscQueue<OutFrame *>;

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

    Connection(int64_t id, std::string name, BufferPool *pool, uint32_t max_data_size = 4 << 20,
               uint32_t send_queue_capacity = 1024);

    ~Connection();

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

    // Unified writev: drains the MPSC send queue into the persistent iovec
    // buffer (partials from a previous EAGAIN are at front), writev's, and
    // keeps unsent frames in the buffer. The in_send_ CAS flag serializes
    // concurrent callers — only one does writev; others just enqueue and
    // return (the winner picks up their frames). Returns true if all data
    // sent, false if partial/EAGAIN (caller arms EPOLLOUT for retry).
    // Called from submit() (cross-thread), post-event flush, and on_writable.
    bool try_send(int fd, TransportStats *stats);

    // Check if the connection has pending send data (queue or partials).
    bool has_pending_send() const
    {
        return send_queue_.has_pending() || overflow_.has_pending() || pending_count_ > 0;
    }

    // Push a frame to the overflow queue (called when send_queue is full).
    // Returns true on success, false if overflow is also full.
    bool enqueue_overflow(OutFrame *frame)
    {
        return overflow_.try_push(frame);
    }

    // Drain up to max frames from the overflow queue.
    int drain_overflow(OutFrame **out, int max)
    {
        return overflow_.drain(out, max);
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
    // For TCP with dup'd read/write fds, this is the read fd.
    uint64_t transport_handle = 0;

    // For TCP with dup'd read/write fds (buzz-cpp pattern): the write fd
    // is a dup() of the read fd, registered separately with epoll so
    // EPOLLONESHOT on read and write are independent. Arming write does
    // not re-arm read, preventing multi-worker races. 0 if not used.
    int write_fd = -1;

    // Back-pointer to the owning SocketEngine (set by Worker::add_connection).
    // SocketTransport::submit uses this to arm write on the correct engine
    // when caller-thread writev hits EAGAIN. Type-erased (void*) to avoid
    // a layering dependency on socket_transport.h.
    void *io_engine = nullptr;

    // Back-pointer to the owning Worker (set by Worker::add_connection).
    // SocketTransport::submit uses this to find the worker's cross-thread
    // pending list for deferred writev. Type-erased (void*) to avoid a
    // layering dependency on socket_transport.h.
    void *io_worker = nullptr;

    // Linux: re-arm TCP_QUICKACK after each read to break the
    // Nagle + delayed-ACK deadlock. Set when Nagle is enabled
    // (tcp_nodelay_ == false). No-op on non-Linux.
    bool quickack = false;

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

    // Lock-free bounded MPSC ring buffer (crowdb::common::MpscQueue). Multiple
    // producer threads push via enqueue_send (Transport::submit); the I/O
    // worker drains via drain_send_queue for writev (Path B).
    SendQueue send_queue_;

    // Overflow queue: frames that couldn't be enqueued to send_queue_
    // (backpressure). Drained first in try_send() so retry responses
    // are sent before new frames. Same lock-free MPSC, smaller capacity.
    SendQueue overflow_{256};

    // in_send_ CAS lock serializes concurrent writev — only one thread
    // drains+sends at a time; others just enqueue and return. The winner
    // picks up all queued frames.
    std::atomic<bool> in_send_{false};

    // Pending frames from a previous EAGAIN. Stored (NOT re-enqueued to
    // the MPSC queue) to preserve order vs concurrent enqueues. The next
    // try_send sends these first, then drains the queue for new frames.
    OutFrame *pending_frames_[BATCH_MAX];
    int       pending_count_{0};

    OnFrameCallback on_frame_callback_;
    OnCloseCallback on_close_callback_;
};

} // namespace crowdb::rpc
