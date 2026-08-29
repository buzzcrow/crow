// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "crowdb-rpc/buffer.h"
#include "crowdb-rpc/framing.h"

#include <cstdint>

namespace crowdb::rpc
{

// Forward declaration — defined in connection.h. Transport::submit takes
// a Connection* but does not need its full definition.
class Connection;

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

} // namespace crowdb::rpc
