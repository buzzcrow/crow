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
// PENDING → DONE (I/O worker or reaper sets before invoking callback)
// DONE → FREE (callback clears after processing, or reaper clears after timeout)
constexpr uint8_t SLOT_FREE    = 0;
constexpr uint8_t SLOT_PENDING = 1;
constexpr uint8_t SLOT_DONE    = 2;

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
};

// Map entry for the pending map (oneshot call() path + slab fallback).
// deadline_ns = 0 means no timeout (oneshot path — Rust handles its own
// timeout via oneshot channels). deadline_ns > 0 means the C++ reaper
// should fail this entry (slab fallback path).
struct PendingEntry
{
    CompletionCallback cb;
    uint64_t           deadline_ns{0};
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
    // Submit a request-response call. The request_id is provided by the
    // caller (it must match the id embedded in the flatbuffer control
    // message so the server can echo it back for correlation). Returns
    // the request_id, or 0 on error (submit failed — callback already
    // invoked with the error).
    uint64_t call(Transport *transport, Connection *conn, uint64_t request_id, Buffer *control, Buffer *data,
                  uint16_t msg_type, CompletionCallback on_complete);

    // Callback-based call (Gap2+Gap3): tries to reserve a slab slot by
    // index (request_id & pool_mask) via CAS FREE→PENDING. If the slot
    // is occupied (slow request holding it), falls back to the pending
    // map (one heap alloc for the std::function — the overload path).
    // The callback is invoked directly on the I/O worker thread by
    // on_response — no oneshot channel, no scheduler round-trip.
    // The pool must be sized (set_completion_pool_size) before use.
    // If the reaper is active (start_reaper), each call gets a deadline
    // = now + timeout_ns; timed-out entries are failed with Timeout.
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

    // Start the timeout reaper thread. Scans the slab pool + pending map
    // every scan_interval_ns for entries past their deadline (timeout_ns
    // from submit time). Timed-out entries are failed with RpcError::Timeout
    // and their slots/entries are reclaimed. Must be called after
    // set_completion_pool_size. No-op if already running.
    void start_reaper(uint64_t timeout_ns, uint64_t scan_interval_ns);

    // Stop the timeout reaper thread. Called automatically by the destructor.
    // No-op if not running.
    void stop_reaper();

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

  public:
    // Perf counters for debugging response correlation.
    struct Counters
    {
        std::atomic<uint64_t> submit_ok{0};        // call_callback succeeded (slab or map)
        std::atomic<uint64_t> submit_fail{0};      // call_callback submit failed
        std::atomic<uint64_t> resp_matched{0};     // on_response matched a slab slot
        std::atomic<uint64_t> resp_mismatch{0};    // on_response: slab miss + map miss (late/dup)
        std::atomic<uint64_t> resp_wrong_id{0};    // on_response: slab PENDING wrong id + map miss
        std::atomic<uint64_t> resp_dropped{0};     // on_response: no slab + no map entry
        std::atomic<uint64_t> slab_fallback{0};    // call_callback fell back to map (slab full)
        std::atomic<uint64_t> resp_map_matched{0}; // on_response matched in map
        std::atomic<uint64_t> reaped_slab{0};      // reaper timed out a slab slot
        std::atomic<uint64_t> reaped_map{0};       // reaper timed out a map entry
        std::atomic<int64_t>  map_in_flight{0};    // live: current entries in pending map
        std::atomic<int64_t>  slab_in_flight{0};   // live: current PENDING slab slots
    };

    Counters &counters()
    {
        return counters_;
    }

  private:
    Counters counters_;
};

} // namespace crow::rpc
