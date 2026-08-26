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
#include <mutex>
#include <string>
#include <sys/uio.h>

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

    Connection(int64_t id, std::string name, BufferPool *pool, uint32_t max_data_size = 4 << 20,
               uint32_t send_queue_capacity = 1024);

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

    // Path A: direct writev with mutex. Sends a single frame immediately,
    // keeping any partial in direct_partial_ for the next call. Thread-safe
    // via send_mu_. Returns true if all data sent, false if partial/EAGAIN
    // (caller arms EPOLLOUT for retry).
    bool send_direct(int fd, OutFrame *frame, TransportStats *stats);

    // Path B: worker-thread flush. Drains the MPSC queue into the flat
    // iovec array (partials from a previous EAGAIN are already at front),
    // writev's, and keeps any remaining partials at front for next time.
    // No lock — only the I/O worker calls this. Returns true if all data
    // sent, false if partial/EAGAIN (caller arms EPOLLOUT for retry).
    bool flush_send(int fd, TransportStats *stats);

    // Check if the connection has pending send data (queue or partials).
    bool has_pending_send() const
    {
        return send_queue_.has_pending() || flush_iov_count_ > 0;
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
    // producer threads push via enqueue_send (Transport::submit); the I/O
    // worker drains via drain_send_queue for writev (Path B).
    SendQueue send_queue_;

    // Path A: direct writev state. send_mu_ serializes concurrent writev
    // across threads. direct_partial_ holds a single partially-sent frame
    // (nullptr when no partial). On the next send_direct call, the partial's
    // remaining iovecs are prepended before the new frame's iovecs.
    std::mutex  send_mu_;
    OutFrame   *direct_partial_{nullptr};

    // Path B: flat iovec array for worker-thread flush. Partials from a
    // previous EAGAIN stay at the front (flush_iov_count_ > 0 with adjusted
    // iov_base/iov_len). New frames from the MPSC queue are appended after.
    // flush_frames_[] tracks the OutFrame* backing each group of iovecs so
    // they can be released after full send. flush_hdrs_[] holds the
    // serialized header bytes for each frame (the header is not stored in
    // the frame itself — it must be serialized into stable storage).
    iovec     flush_iovs_[3 * BATCH_MAX];
    OutFrame *flush_frames_[BATCH_MAX];
    uint8_t   flush_hdrs_[BATCH_MAX][HEADER_SIZE];
    int       flush_iov_count_{0};
    int       flush_frame_count_{0};

    OnFrameCallback on_frame_callback_;
    OnCloseCallback on_close_callback_;
};

} // namespace crow::rpc
