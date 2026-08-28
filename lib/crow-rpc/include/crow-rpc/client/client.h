// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "crow-rpc/buffer.h"
#include "crow-rpc/c_api.h"
#include "crow-rpc/connection.h"
#include "crow-rpc/framing.h"
#include "crow-rpc/transport.h"

#include <folly/concurrency/ConcurrentHashMap.h>

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <utility>

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
// FREE → PENDING_CLAIMED (submitter CAS — slot reserved, fields not yet written)
// PENDING_CLAIMED → PENDING_READY (submitter store-release — fields published)
// PENDING_READY → DONE (on_response CAS — fields read before CAS) or
//   FREE (reaper/fail_all CAS — fields read before CAS)
// DONE → PENDING_CLAIMED (next send() reuses the slot)
// The two-phase PENDING split eliminates the write-before-CAS race: the
// loser of the CAS falls to the map before touching any slot fields, so
// the winner's fields are never corrupted. PENDING_CLAIMED is invisible
// to on_response and the reaper (they only act on PENDING_READY), so a
// slot whose fields are still being written is never timed out or
// dispatched prematurely. on_response/reaper read fields BEFORE the CAS
// (while the slot is PENDING_READY, no concurrent writer can touch it),
// then CAS directly to DONE/FREE — claim + release in one op, no extra
// store. The callback uses locals, so a rapid DONE→PENDING_CLAIMED cycle
// by the callback's send() cannot corrupt the already-read fields.
constexpr uint8_t SLOT_FREE            = 0;
constexpr uint8_t SLOT_PENDING_READY   = 1;
constexpr uint8_t SLOT_DONE            = 2;
constexpr uint8_t SLOT_PENDING_CLAIMED = 3;

// A pre-allocated completion slot for the callback-based call path.
// Indexed by request_id & pool_mask (pool size = power of two). This
// replaces the folly map + per-call heap allocation for high-throughput
// callers (bench). The slot is written by the submitter thread and read
// by the I/O worker thread; the atomic state serializes access.
// deadline_ns: 0 = no timeout (bench mode); >0 = steady-clock nanoseconds
// at which the reaper should fail this slot. Written by the submitter
// before state.store(PENDING, release); read by the reaper after
// state.load(acquire) == PENDING — the release-acquire pair on state
// provides visibility without atomics on deadline_ns itself (but the
// atomic wrapper avoids TSan false positives).
struct CompletionSlot
{
    std::atomic<uint8_t>  state{SLOT_FREE};
    uint64_t              request_id{0}; // set when PENDING
    crow_rpc_on_complete  cb{nullptr};   // C ABI callback
    void                 *user_data{nullptr};
    std::atomic<uint64_t> deadline_ns{0};
    Connection           *conn{nullptr}; // for per-connection fail_all
};

// Map entry for the pending map (oneshot call() path + slab fallback).
// deadline_ns = 0 means no timeout (oneshot path — Rust handles its own
// timeout via oneshot channels). deadline_ns > 0 means the C++ reaper
// should fail this entry (slab fallback path).
struct PendingEntry
{
    CompletionCallback cb;
    uint64_t           deadline_ns{0};
    Connection        *conn{nullptr}; // for per-connection fail_all
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
    ~RpcClient();

    // Send a request with a C ABI completion callback. Reserves a slab
    // slot by index (request_id & pool_mask) via CAS FREE→PENDING; if
    // the slot is occupied, falls back to the pending map. The callback
    // is invoked directly on the I/O worker thread when the response
    // arrives — no oneshot channel, no scheduler round-trip. The pool
    // must be sized (set_completion_pool_size) before use. If the reaper
    // is active (start_reaper), each call gets a deadline = now +
    // timeout_ns; timed-out entries are failed with Timeout. Returns
    // true on success, false on submit error (callback NOT invoked).
    bool send(Transport *transport, Connection *conn, uint64_t request_id, Buffer *control, Buffer *data,
              uint16_t msg_type, crow_rpc_on_complete cb, void *user_data);

    // Attach this caller to a connection's on_frame callback. The
    // callback tries response routing first (on_response); if the
    // frame's request_id is not in the pending map, it dispatches as
    // a request via dispatch_request. Call this once per connection
    // before sending requests through it.
    void attach(Connection *conn);

    // Try to route a frame as a response to a previously-sent request.
    // Returns true if request_id was found in the pending map (frame
    // consumed, callback invoked). Returns false if not found (frame
    // NOT consumed — caller owns it and must dispatch or delete it).
    bool on_response(uint64_t request_id, Frame *response);

    // Dispatch an incoming request frame (server→client direction) to
    // a registered handler by msg_type. If no handler is found, sends
    // UnknownMessage (if transport set) or drops the frame.
    void dispatch_request(Frame *frame, Connection *conn);

    // Register a handler for an incoming request msg_type. The handler
    // is a C callback (same type as server-side handlers) that receives
    // the request fields + conn_handle and submits the response later
    // via crow_rpc_server_submit_response.
    void register_handler(uint16_t msg_type, crow_rpc_handler_fn callback, void *user_data);

    // Set the transport for submitting UnknownMessage responses when
    // no handler matches an incoming request msg_type. If not set,
    // unmatched request frames are dropped.
    void set_transport(Transport *t)
    {
        transport_ = t;
    }

    // Fail pending requests. If conn is non-null, only requests sent on
    // that connection are failed (per-connection scoping for connection
    // close). If conn is null, all pending requests are failed (used by
    // shutdown / destructor).
    void fail_all(Connection *conn, RpcError err);

    // Number of pending requests (for diagnostics).
    size_t pending_count();

    // Size the callback completion pool. Must be a power of two; the
    // caller passes the max in-flight (the next power of two is used).
    // Must be called before any send(). No-op if already sized.
    void set_completion_pool_size(size_t max_in_flight);

    // Start the timeout reaper thread. Scans the slab pool + pending map
    // every scan_interval_ns for entries past their deadline (timeout_ns
    // from submit time). Timed-out entries are failed with RpcError::Timeout
    // and their slots/entries are reclaimed. Must be called after
    // set_completion_pool_size. No-op if already running.
    void start_reaper(uint64_t timeout_ns, uint64_t scan_interval_ns);

    // Stop the timeout reaper thread. Called automatically by the destructor.
    // No-op if not running.
    void stop_reaper();

  private:
    // Handler registry for incoming requests (server→client direction).
    // Maps msg_type → (C callback, user_data). Same trampoline pattern
    // as the server-side handler dispatch.
    std::mutex                                                           handler_mu_;
    std::unordered_map<uint16_t, std::pair<crow_rpc_handler_fn, void *>> request_handlers_;

    // Transport for submitting UnknownMessage responses when no handler
    // matches an incoming request msg_type. Set via set_transport.
    Transport *transport_{nullptr};

    // Slab-based completion pool (callback model). Indexed by
    // request_id & pool_mask_. Null by default (callback model opt-in).
    // unique_ptr<[]> because CompletionSlot has a non-movable atomic.
    std::unique_ptr<CompletionSlot[]> completion_pool_;
    size_t                            pool_size_{0};
    size_t                            pool_mask_{0}; // pool_size - 1 (power of two)
    // Guards the one-time pool allocation in set_completion_pool_size.
    // Only contended during init (idempotent after first call).
    std::mutex pool_mu_;

    // Pending map: oneshot call() entries (deadline=0) + slab-fallback
    // entries (deadline>0). folly::ConcurrentHashMap (striped locks)
    // eliminates the single-mutex bottleneck on the response hot path.
    folly::ConcurrentHashMap<uint64_t, PendingEntry> pending_;

    // Reaper thread: scans slab + map for timed-out entries.
    std::thread             reaper_thread_;
    std::atomic<bool>       reaper_running_{false};
    std::condition_variable reaper_cv_;
    std::mutex              reaper_mu_;
    uint64_t                reaper_interval_ns_{0};
    std::atomic<uint64_t>   default_timeout_ns_{0}; // 0 = no timeout (bench)

    // Build an OutFrame for submission. The RpcClient owns the OutFrame;
    // the transport takes it and releases buffers after send.
    OutFrame *build_frame(uint64_t request_id, Buffer *control, Buffer *data, uint16_t msg_type, uint8_t flags);

    // Reaper loop: scans slab pool + pending map for timed-out entries.
    void reaper_loop();

    // Compute steady-clock nanoseconds (monotonic).
    static uint64_t steady_now_ns();
};

} // namespace crow::rpc
