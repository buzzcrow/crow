// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#include "crow-rpc/client/client.h"

#include "crow-rpc/c_api_internal.h"
#include "crow-rpc/client/rpc_client_metrics.h"
#include "crow-rpc/server/message.h"

#include <cassert>
#include <chrono>
#include <vector>

namespace crow::rpc
{

RpcClient::RpcClient() = default;

RpcClient::~RpcClient()
{
    stop_reaper();
}

uint64_t RpcClient::steady_now_ns()
{
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
            .count());
}

void RpcClient::set_completion_pool_size(size_t max_in_flight)
{
    if (completion_pool_ != nullptr) {
        return; // already sized
    }
    // Round up to the next power of two for fast bitmask modulo.
    size_t n = 1;
    while (n < max_in_flight) {
        n <<= 1;
    }
    completion_pool_ = std::make_unique<CompletionSlot[]>(n);
    pool_size_       = n;
    pool_mask_       = n - 1;
}

OutFrame *RpcClient::build_frame(uint64_t request_id, Buffer *control, Buffer *data, uint16_t msg_type, uint8_t flags)
{
    auto *frame            = new OutFrame;
    frame->request_id      = request_id;
    frame->header.msg_type = msg_type;
    frame->header.flags    = flags;
    frame->control         = control;
    frame->data            = data;
    if (control != nullptr) {
        frame->header.msg_size = static_cast<uint16_t>(control->len);
    }
    if (data != nullptr) {
        frame->header.data_size = data->len;
    }
    return frame;
}

// Callback-based call: tries slab first (CAS FREE→PENDING), falls back
// to the pending map if the slot is occupied (slow request holding it).
// The slab path is O(1) + zero heap alloc; the map path is one heap alloc
// (std::function) — the overload fallback.
// Flow: doc/design/rpc/rpc-echo-flow-analysis.md § "Flow".
bool RpcClient::send(Transport *transport, Connection *conn, uint64_t request_id, Buffer *control, Buffer *data,
                     uint16_t msg_type, crow_rpc_on_complete cb, void *user_data)
{
    assert(completion_pool_ != nullptr && "send() without set_completion_pool_size");

    uint64_t timeout  = default_timeout_ns_.load(std::memory_order_relaxed);
    uint64_t deadline = (timeout > 0) ? steady_now_ns() + timeout : 0;

    size_t idx  = request_id & pool_mask_;
    auto  &slot = completion_pool_[idx];

    // Check if the slot is reusable (FREE or DONE) before writing fields.
    // This prevents corrupting a PENDING slot's callback/user_data when
    // the slot is held by a slow request. DONE means the response was
    // delivered and the callback already ran — the slot is safe to reuse
    // (the coroutine model leaves slots in DONE after each response).
    // The remaining race — two submitters both seeing FREE/DONE and both
    // writing before one CAS wins — is extremely rare and only affects
    // the general send() path, not the coroutine model (which
    // assigns fixed slots per coroutine).
    uint8_t st = slot.state.load(std::memory_order_acquire);
    if (st == SLOT_FREE || st == SLOT_DONE) {
        slot.request_id = request_id;
        slot.cb         = cb;
        slot.user_data  = user_data;
        slot.deadline_ns.store(deadline, std::memory_order_relaxed);
        uint8_t expected = st;
        if (slot.state.compare_exchange_strong(expected, SLOT_PENDING, std::memory_order_acq_rel)) {
            // Slab path — zero heap alloc.
            OutFrame *frame = build_frame(request_id, control, data, msg_type, 0);
            if (!transport->submit(conn, frame)) {
                slot.state.store(SLOT_FREE, std::memory_order_release);
                if (frame->control != nullptr)
                    frame->control->release();
                if (frame->data != nullptr)
                    frame->data->release();
                delete frame;
                rpc_submit_fail().inc();
                return false;
            }
            rpc_submit_ok().inc();
            return true;
        }
        // CAS failed — rare race (another submitter grabbed the slot
        // between our load and CAS). Fall through to map fallback.
    }

    // Slab slot occupied (or rare CAS race) — fall back to the pending
    // map. One heap alloc for the std::function capture (overload path,
    // not the hot path).
    rpc_slab_fallback().inc();
    auto wrapped = [cb, user_data, request_id](Frame *resp, RpcError err) {
        invoke_c_complete(cb, user_data, request_id, resp, err);
    };
    pending_.insert_or_assign(request_id, PendingEntry{std::move(wrapped), deadline});

    OutFrame *frame = build_frame(request_id, control, data, msg_type, 0);
    if (!transport->submit(conn, frame)) {
        // Submit failed — remove from map. Callback NOT invoked (caller
        // handles the error, same as the slab path).
        pending_.erase(request_id);
        if (frame->control != nullptr)
            frame->control->release();
        if (frame->data != nullptr)
            frame->data->release();
        delete frame;
        rpc_submit_fail().inc();
        return false;
    }
    rpc_submit_ok().inc();
    return true;
}

void RpcClient::attach(Connection *conn)
{
    // Set the on_frame callback to route response frames to on_response.
    // request_id is extracted during parse.
    conn->set_on_frame([this](Frame *frame, Connection * /*conn*/) { on_response(frame->request_id, frame); });
}

void RpcClient::on_response(uint64_t request_id, Frame *response)
{
    // Slab path (callback model): O(1) index lookup, no hash. Check the
    // slab pool first — if the slot is PENDING for this request_id, CAS
    // PENDING→DONE to claim it. The CAS prevents double-invoke: if the
    // reaper already timed out the slot, the CAS fails and we fall through
    // to the map path (which won't find it → resp_missed).
    if (completion_pool_ != nullptr) {
        size_t  idx  = request_id & pool_mask_;
        auto   &slot = completion_pool_[idx];
        uint8_t st   = slot.state.load(std::memory_order_acquire);
        if (st == SLOT_PENDING) {
            if (slot.request_id == request_id) {
                uint8_t expected = SLOT_PENDING;
                if (slot.state.compare_exchange_strong(expected, SLOT_DONE, std::memory_order_acq_rel)) {
                    // Do NOT reset to FREE after the callback — the callback
                    // may reuse this slot (via send() → SLOT_PENDING)
                    // for the next request. Resetting to FREE here would
                    // overwrite that PENDING.
                    auto cb = slot.cb;
                    auto ud = slot.user_data;
                    rpc_resp_matched().inc();
                    invoke_c_complete(cb, ud, request_id, response, RpcError::Ok);
                    return;
                }
                // CAS failed — reaper timed out this slot. Fall through
                // to map (won't find it → resp_dropped).
            }
            else {
                // Slot is PENDING but for a different request_id — a stale
                // response arrived after the slot was reused for a new
                // request. Fall through to map; if map also misses, this
                // is counted as resp_missed.
            }
        }
        // Slot not PENDING (FREE or DONE) — late response, duplicate, or
        // a map-fallback response. Fall through to map.
    }

    // Map path: oneshot call() entries + slab-fallback entries.
    auto it = pending_.find(request_id);
    if (it == pending_.end()) {
        // Late response after timeout or duplicate — discard.
        rpc_resp_missed().inc();
        delete response;
        return;
    }
    CompletionCallback cb = std::move(it->second.cb);
    pending_.erase(it);
    rpc_resp_matched().inc();
    if (cb) {
        cb(response, RpcError::Ok);
    }
    else {
        delete response;
    }
}

void RpcClient::fail_all(RpcError err)
{
    // Fail slab-pending requests (callback model). CAS PENDING→DONE to
    // avoid double-invoke with the reaper or on_response.
    for (size_t i = 0; i < pool_size_; ++i) {
        auto   &slot     = completion_pool_[i];
        uint8_t expected = SLOT_PENDING;
        if (slot.state.compare_exchange_strong(expected, SLOT_DONE, std::memory_order_acq_rel)) {
            auto cb = slot.cb;
            auto ud = slot.user_data;
            invoke_c_complete(cb, ud, slot.request_id, nullptr, err);
            slot.state.store(SLOT_FREE, std::memory_order_release);
        }
    }

    // Fail map-pending requests (oneshot + slab-fallback). folly has no
    // swap; iterate + erase + invoke. Each erase is safe during iteration
    // (striped-lock map).
    auto it = pending_.begin();
    while (it != pending_.end()) {
        CompletionCallback cb  = std::move(it->second.cb);
        auto               key = it->first;
        ++it;
        pending_.erase(key);
        if (cb) {
            cb(nullptr, err);
        }
    }
}

size_t RpcClient::pending_count()
{
    size_t n = 0;
    for (size_t i = 0; i < pool_size_; ++i) {
        if (completion_pool_[i].state.load(std::memory_order_relaxed) == SLOT_PENDING) {
            ++n;
        }
    }
    n += pending_.size();
    return n;
}

void RpcClient::start_reaper(uint64_t timeout_ns, uint64_t scan_interval_ns)
{
    if (reaper_running_.load(std::memory_order_relaxed)) {
        return; // already running
    }
    default_timeout_ns_.store(timeout_ns, std::memory_order_relaxed);
    reaper_interval_ns_ = scan_interval_ns;
    reaper_running_.store(true, std::memory_order_release);
    reaper_thread_ = std::thread([this] { reaper_loop(); });
}

void RpcClient::stop_reaper()
{
    if (!reaper_running_.load(std::memory_order_relaxed)) {
        return;
    }
    {
        std::lock_guard<std::mutex> lock(reaper_mu_);
        reaper_running_.store(false, std::memory_order_release);
    }
    reaper_cv_.notify_all();
    if (reaper_thread_.joinable()) {
        reaper_thread_.join();
    }
}

void RpcClient::reaper_loop()
{
    while (reaper_running_.load(std::memory_order_acquire)) {
        uint64_t now = steady_now_ns();

        // Scan slab pool for timed-out slots. CAS PENDING→DONE to claim;
        // if the CAS fails, on_response already handled it.
        for (size_t i = 0; i < pool_size_; ++i) {
            auto &slot = completion_pool_[i];
            if (slot.state.load(std::memory_order_acquire) != SLOT_PENDING) {
                continue;
            }
            uint64_t dl = slot.deadline_ns.load(std::memory_order_relaxed);
            if (dl == 0 || now < dl) {
                continue; // no timeout or not yet expired
            }
            uint8_t expected = SLOT_PENDING;
            if (slot.state.compare_exchange_strong(expected, SLOT_DONE, std::memory_order_acq_rel)) {
                auto cb = slot.cb;
                auto ud = slot.user_data;
                rpc_reaped().inc();
                invoke_c_complete(cb, ud, slot.request_id, nullptr, RpcError::Timeout);
                slot.state.store(SLOT_FREE, std::memory_order_release);
            }
        }

        // Scan pending map for timed-out entries (slab-fallback only —
        // oneshot call() entries have deadline 0 and are skipped).
        std::vector<uint64_t> expired;
        for (auto it = pending_.begin(); it != pending_.end(); ++it) {
            uint64_t dl = it->second.deadline_ns;
            if (dl > 0 && now >= dl) {
                expired.push_back(it->first);
            }
        }
        for (uint64_t key : expired) {
            auto it = pending_.find(key);
            if (it == pending_.end()) {
                continue; // on_response already handled it
            }
            CompletionCallback cb = std::move(it->second.cb);
            pending_.erase(it);
            rpc_reaped().inc();
            if (cb) {
                cb(nullptr, RpcError::Timeout);
            }
        }

        // Sleep until next scan or stop.
        std::unique_lock<std::mutex> lock(reaper_mu_);
        reaper_cv_.wait_for(lock, std::chrono::nanoseconds(reaper_interval_ns_),
                            [this] { return !reaper_running_.load(std::memory_order_relaxed); });
    }
}

} // namespace crow::rpc
