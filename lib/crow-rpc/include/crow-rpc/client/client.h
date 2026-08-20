// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "crow-rpc/buffer.h"
#include "crow-rpc/c_api.h"
#include "crow-rpc/connection.h"
#include "crow-rpc/framing.h"
#include "crow-rpc/transport.h"

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>

#if CROW_RPC_HAVE_FOLLY
#    include <folly/concurrency/ConcurrentHashMap.h>
#endif

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

// Slab slot states for the callback-based completion pool.
// FREE → PENDING (submitter sets before submit)
// PENDING → DONE (I/O worker sets before invoking callback)
// DONE → FREE (callback clears after processing)
constexpr uint8_t SLOT_FREE    = 0;
constexpr uint8_t SLOT_PENDING = 1;
constexpr uint8_t SLOT_DONE    = 2;

// A pre-allocated completion slot for the callback-based call path.
// Indexed by request_id & pool_mask (pool size = power of two). This
// replaces the folly map + per-call heap allocation for high-throughput
// callers (bench). The slot is written by the submitter thread and read
// by the I/O worker thread; the atomic state serializes access.
struct CompletionSlot
{
    std::atomic<uint8_t> state{SLOT_FREE};
    uint64_t             request_id{0}; // set when PENDING
    crow_rpc_on_complete cb{nullptr};   // C ABI callback
    void                *user_data{nullptr};
};

// RpcClient manages request/response correlation. Each call allocates
// a monotonic request_id, inserts a callback into the pending map, and
// submits the frame. When the response arrives (via Connection::on_frame),
// on_response looks up the request_id, invokes the callback, and removes
// the entry.
//
// The pending map uses folly::ConcurrentHashMap (striped locks) when
// folly is available, falling back to std::unordered_map + mutex
// otherwise. The striped-lock map eliminates the single-mutex bottleneck
// on the response hot path — at high TPS (benchmarks, consensus), the
// I/O worker's on_response lookup no longer contends with cross-thread
// submit/timeout removal on a single lock.
class RpcClient
{
  public:
    RpcClient();

    // Submit a request-response call. The request_id is provided by the
    // caller (it must match the id embedded in the flatbuffer control
    // message so the server can echo it back for correlation). Returns
    // the request_id, or 0 on error (submit failed — callback already
    // invoked with the error).
    uint64_t call(Transport *transport, Connection *conn, uint64_t request_id, Buffer *control, Buffer *data,
                  uint16_t msg_type, CompletionCallback on_complete);

    // Callback-based call (Gap2+Gap3): reserves a slab slot by index
    // (request_id & pool_mask), stores the C ABI callback + user_data,
    // and submits. O(1) index lookup, zero per-call heap allocation.
    // The callback is invoked directly on the I/O worker thread by
    // on_response — no oneshot channel, no scheduler round-trip.
    // The pool must be sized (set_completion_pool_size) before use.
    // Returns true on success, false on submit error (callback NOT invoked).
    bool call_callback(Transport *transport, Connection *conn, uint64_t request_id, Buffer *control, Buffer *data,
                       uint16_t msg_type, crow_rpc_on_complete cb, void *user_data);

    // Submit a one-way message (no response expected, no callback).
    // Returns true on success, false on submit error.
    bool call_one_way(Transport *transport, Connection *conn, Buffer *control, Buffer *data, uint16_t msg_type);

    // Attach this caller to a connection's on_frame callback so responses
    // are routed to on_response. Call this once per connection before
    // sending requests through it.
    void attach(Connection *conn);

    // Called by the on_frame callback (set by attach) when a response
    // arrives. Looks up request_id, invokes callback, removes entry.
    // Dispatches to the slab pool (callback model) first, then falls
    // back to the folly map (oneshot model).
    void on_response(uint64_t request_id, Frame *response);

    // Called by Connection::close to fail all pending requests.
    void fail_all(RpcError err);

    // Number of pending requests (for diagnostics).
    size_t pending_count();

    // Size the callback completion pool. Must be a power of two; the
    // caller passes the max in-flight (the next power of two is used).
    // Must be called before any call_callback(). No-op if already sized.
    void set_completion_pool_size(size_t max_in_flight);

    // Generate the next request_id (for callers that need to embed it
    // in the flatbuffer control message before calling call()).
    uint64_t next_request_id()
    {
        return next_request_id_.fetch_add(1, std::memory_order_relaxed);
    }

  private:
    std::atomic<uint64_t> next_request_id_{1};

    // Slab-based completion pool (callback model). Indexed by
    // request_id & pool_mask_. Null by default (callback model opt-in).
    // unique_ptr<[]> because CompletionSlot has a non-movable atomic.
    std::unique_ptr<CompletionSlot[]> completion_pool_;
    size_t                            pool_size_{0};
    size_t                            pool_mask_{0}; // pool_size - 1 (power of two)

#if CROW_RPC_HAVE_FOLLY
    folly::ConcurrentHashMap<uint64_t, CompletionCallback> pending_;
#else
    std::mutex                                       pending_mu_;
    std::unordered_map<uint64_t, CompletionCallback> pending_;
#endif

    // Build an OutFrame for submission. The RpcClient owns the OutFrame;
    // the transport takes it and releases buffers after send.
    OutFrame *build_frame(uint64_t request_id, Buffer *control, Buffer *data, uint16_t msg_type, uint8_t flags);

  public:
    // Perf counters for debugging response correlation.
    struct Counters
    {
        std::atomic<uint64_t> submit_ok{0};     // call_callback succeeded
        std::atomic<uint64_t> submit_fail{0};   // call_callback submit failed
        std::atomic<uint64_t> resp_matched{0};  // on_response matched a slab slot
        std::atomic<uint64_t> resp_mismatch{0}; // on_response: slot state != PENDING
        std::atomic<uint64_t> resp_wrong_id{0}; // on_response: slot PENDING but request_id mismatch
        std::atomic<uint64_t> resp_dropped{0};  // on_response: no slab + no map entry
    };

    Counters &counters()
    {
        return counters_;
    }

  private:
    Counters counters_;
};

} // namespace crow::rpc
