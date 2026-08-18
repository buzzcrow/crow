// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "crow-rpc/buffer.h"
#include "crow-rpc/framing.h"
#include "crow-rpc/transport.h"

#include <atomic>
#include <cstdint>
#include <functional>
#include <mutex>
#include <unordered_map>

namespace crow::rpc
{

// Error codes for the RPC layer.
enum class RpcError : uint8_t {
    Ok = 0,
    ConnectionClosed,
    Timeout,
    SendQueueFull,
    ConnectionError,
    RegistrationFailed,
    AllDown,
};

// Completion callback for request-response calls. The callback receives
// the response frame (nullptr on error) and the error code.
using CompletionCallback = std::function<void(Frame *response, RpcError err)>;

// RemoteCaller manages request/response correlation. Each call allocates
// a monotonic request_id, inserts a callback into the pending map, and
// submits the frame. When the response arrives (via Connection::on_frame),
// on_response looks up the request_id, invokes the callback, and removes
// the entry.
//
// The pending map is a plain std::unordered_map protected by a mutex for
// v1. The design calls for folly::ConcurrentHashMap (lock-free) on the
// consensus hot path; that optimization is deferred until the FFI layer
// is wired and benchmarks show contention. The mutex is fine for v1
// because each connection is owned by one worker thread — the worker's
// on_response lookup is contention-free; only cross-thread submit and
// timeout removal contend, and those are not the hot path.
class RemoteCaller
{
  public:
    RemoteCaller();

    // Submit a request-response call. The request_id is provided by the
    // caller (it must match the id embedded in the flatbuffer control
    // message so the server can echo it back for correlation). Returns
    // the request_id, or 0 on error (submit failed — callback already
    // invoked with the error).
    uint64_t call(Transport *transport, Connection *conn, uint64_t request_id, Buffer *control, Buffer *data,
                  uint16_t msg_type, CompletionCallback on_complete);

    // Submit a one-way message (no response expected, no callback).
    // Returns true on success, false on submit error.
    bool call_one_way(Transport *transport, Connection *conn, Buffer *control, Buffer *data, uint16_t msg_type);

    // Attach this caller to a connection's on_frame callback so responses
    // are routed to on_response. Call this once per connection before
    // sending requests through it.
    void attach(Connection *conn);

    // Called by the on_frame callback (set by attach) when a response
    // arrives. Looks up request_id, invokes callback, removes entry.
    void on_response(uint64_t request_id, Frame *response);

    // Called by Connection::close to fail all pending requests.
    void fail_all(RpcError err);

    // Number of pending requests (for diagnostics).
    size_t pending_count();

    // Generate the next request_id (for callers that need to embed it
    // in the flatbuffer control message before calling call()).
    uint64_t next_request_id()
    {
        return next_request_id_.fetch_add(1, std::memory_order_relaxed);
    }

  private:
    std::atomic<uint64_t> next_request_id_{1};

    std::mutex                                       pending_mu_;
    std::unordered_map<uint64_t, CompletionCallback> pending_;

    // Build an OutFrame for submission. The caller (RemoteCaller) owns
    // the OutFrame; the transport takes it and releases buffers after send.
    OutFrame *build_frame(uint64_t request_id, Buffer *control, Buffer *data, uint16_t msg_type, uint8_t flags);
};

} // namespace crow::rpc
