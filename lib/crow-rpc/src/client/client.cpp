// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#include "crow-rpc/client/client.h"

#include "crow-common/log.h"
#include "crow-rpc/c_api_internal.h"
#include "crow-rpc/client/rpc_client_metrics.h"
#include "crow-rpc/server/handler.h"
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
    std::scoped_lock lock(pool_mu_);
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
        frame->header.msg_size = control->len;
    }
    if (data != nullptr) {
        frame->header.data_size = data->len;
    }
    return frame;
}

// Callback-based call: tries slab first (CAS FREE/DONE→PENDING_CLAIMED),
// writes fields, then publishes (PENDING_CLAIMED→PENDING_READY). Falls
// back to the pending map if the slot is occupied (slow request holding
// it). The slab path is O(1) + zero heap alloc; the map path is one heap
// alloc (std::function) — the overload fallback.
// Flow: doc/design/rpc/rpc-echo-flow-analysis.md § "Flow".
bool RpcClient::send(Transport *transport, Connection *conn, uint64_t request_id, Buffer *control, Buffer *data,
                     uint16_t msg_type, crow_rpc_on_complete cb, void *user_data)
{
    try {
        uint64_t timeout  = default_timeout_ns_.load(std::memory_order_relaxed);
        uint64_t deadline = (timeout > 0) ? steady_now_ns() + timeout : 0;

        // If no slab pool is sized, go directly to the map path. This is
        // used by the oneshot call() path when the caller opts out of the
        // slab (e.g. when per-call heap alloc already makes the slab's
        // zero-alloc advantage marginal).
        if (completion_pool_ == nullptr) {
            goto map_path;
        }

        {
            size_t idx  = request_id & pool_mask_;
            auto  &slot = completion_pool_[idx];

            // CAS first to claim the slot — the loser falls to the map before
            // touching any slot fields, so the winner's fields are never
            // corrupted. This eliminates the write-before-CAS race.
            uint8_t st = slot.state.load(std::memory_order_acquire);
            if (st == SLOT_FREE || st == SLOT_DONE) {
                uint8_t expected = st;
                if (slot.state.compare_exchange_strong(expected, SLOT_PENDING_CLAIMED, std::memory_order_acq_rel)) {
                    // We own the slot — write fields, then publish.
                    slot.request_id = request_id;
                    slot.cb         = cb;
                    slot.user_data  = user_data;
                    slot.conn       = conn;
                    slot.deadline_ns.store(deadline, std::memory_order_relaxed);
                    slot.state.store(SLOT_PENDING_READY, std::memory_order_release);

                    OutFrame *frame = build_frame(request_id, control, data, msg_type, 0);
                    if (!transport->submit(conn, frame)) {
                        slot.state.store(SLOT_FREE, std::memory_order_release);
                        if (frame->control != nullptr)
                            frame->control->release();
                        if (frame->data != nullptr)
                            frame->data->release();
                        delete frame;
                        rpc_submit_fail().inc();
                        CR_LOG_WARN("send: submit failed (slab) request_id={} conn_id={}",
                                    static_cast<unsigned long long>(request_id), static_cast<long long>(conn->id()));
                        return false;
                    }
                    rpc_submit_ok().inc();
                    return true;
                }
                // CAS failed — another submitter grabbed the slot between our
                // load and CAS. Fall through to map fallback.
            }
        } // end slab block

    map_path:
        // Slab slot occupied (PENDING_CLAIMED/PENDING_READY/PROCESSING), rare
        // CAS race, or no slab pool sized — fall back to the pending map. One
        // heap alloc for the std::function capture (overload path, not the
        // hot path).
        rpc_slab_fallback().inc();
        auto wrapped = [cb, user_data, request_id](Frame *resp, RpcError err) {
            invoke_c_complete(cb, user_data, request_id, resp, err);
        };
        pending_.insert_or_assign(request_id, PendingEntry{std::move(wrapped), deadline, conn});

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
            CR_LOG_WARN("send: submit failed (map) request_id={} conn_id={}",
                        static_cast<unsigned long long>(request_id), static_cast<long long>(conn->id()));
            return false;
        }
        rpc_submit_ok().inc();
        return true;
    }
    catch (const std::exception &e) {
        return false;
    }
    catch (...) {
        return false;
    }
}

void RpcClient::attach(Connection *conn)
{
    // Set the on_frame callback to combined routing: try response
    // routing first (on_response); if the request_id is not in the
    // pending map, dispatch as a request via dispatch_request.
    conn->set_on_frame([this](Frame *frame, Connection *conn) {
        if (!on_response(frame->request_id, frame)) {
            dispatch_request(frame, conn);
        }
    });
}

void RpcClient::register_handler(uint16_t msg_type, crow_rpc_handler_fn callback, void *user_data)
{
    std::lock_guard<std::mutex> lock(handler_mu_);
    request_handlers_[msg_type] = {callback, user_data};
}

void RpcClient::dispatch_request(Frame *frame, Connection *conn)
{
    uint16_t msg_type   = frame->header.msg_type;
    bool     is_one_way = (frame->header.flags & FLAG_ONE_WAY) != 0;

    crow_rpc_handler_fn cb        = nullptr;
    void               *user_data = nullptr;
    {
        std::lock_guard<std::mutex> lock(handler_mu_);
        auto                        it = request_handlers_.find(msg_type);
        if (it != request_handlers_.end()) {
            cb        = it->second.first;
            user_data = it->second.second;
        }
    }

    if (cb != nullptr) {
        // Same trampoline as server-side dispatch: extract fields,
        // invoke C callback, delete frame. The callback submits the
        // response later via crow_rpc_server_submit_response.
        invoke_c_handler(cb, user_data, frame, conn);
        return;
    }

    // No handler registered for this msg_type.
    if (is_one_way) {
        delete frame;
        return;
    }
    if (transport_ != nullptr) {
        // Send UnknownMessage response (same as server-side behavior).
        OutFrame *resp = handle_unknown(frame, conn);
        delete frame;
        if (resp != nullptr) {
            transport_->submit(conn, resp);
        }
    }
    else {
        // No transport — drop the frame. The server's request will
        // time out; the retry/WSCritical logic handles it.
        delete frame;
    }
}

bool RpcClient::on_response(uint64_t request_id, Frame *response)
{
    // Slab path (callback model): O(1) index lookup, no hash. Check the
    // slab pool first — if the slot is PENDING_READY for this request_id,
    // read fields BEFORE the CAS (safe: no concurrent writer can touch a
    // PENDING_READY slot — send() only claims FREE/DONE), then CAS
    // PENDING_READY→DONE to claim + release in one op. The callback uses
    // locals, so a rapid DONE→PENDING_CLAIMED cycle by the callback's
    // send() cannot corrupt the already-read fields.
    if (completion_pool_ != nullptr) {
        size_t  idx  = request_id & pool_mask_;
        auto   &slot = completion_pool_[idx];
        uint8_t st   = slot.state.load(std::memory_order_acquire);
        if (st == SLOT_PENDING_READY && slot.request_id == request_id) {
            auto    cb       = slot.cb;
            auto    ud       = slot.user_data;
            uint8_t expected = SLOT_PENDING_READY;
            if (slot.state.compare_exchange_strong(expected, SLOT_DONE, std::memory_order_acq_rel)) {
                rpc_resp_matched().inc();
                invoke_c_complete(cb, ud, request_id, response, RpcError::Ok);
                return true;
            }
            // CAS failed — reaper already claimed the slot. Fall through
            // to map (won't find it → resp_missed).
        }
        // Slot not PENDING_READY for this request_id — late response,
        // duplicate, or a map-fallback response. Fall through to map.
    }

    // Map path: oneshot call() entries + slab-fallback entries.
    auto it = pending_.find(request_id);
    if (it == pending_.end()) {
        // Late response after timeout or duplicate — not in pending map.
        // Return false so the caller can dispatch it as a request (or
        // delete it). Do NOT delete the frame here — the caller owns it.
        rpc_resp_missed().inc();
        return false;
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
    return true;
}

void RpcClient::fail_all(Connection *conn, RpcError err)
{
    // Fail slab-pending requests (callback model). Read fields before CAS
    // (slot is PENDING_READY, no concurrent writer), then CAS PENDING_READY→FREE
    // to claim + release in one op. Same read-before-CAS pattern as on_response.
    // If conn is non-null, only fail entries sent on that connection
    // (per-connection scoping). If conn is null, fail all (shutdown).
    for (size_t i = 0; i < pool_size_; ++i) {
        auto   &slot     = completion_pool_[i];
        uint8_t expected = SLOT_PENDING_READY;
        if (slot.state.load(std::memory_order_acquire) == SLOT_PENDING_READY) {
            if (conn != nullptr && slot.conn != conn) {
                continue;
            }
            auto cb  = slot.cb;
            auto ud  = slot.user_data;
            auto rid = slot.request_id;
            if (slot.state.compare_exchange_strong(expected, SLOT_FREE, std::memory_order_acq_rel)) {
                invoke_c_complete(cb, ud, rid, nullptr, err);
            }
        }
    }

    // Fail map-pending requests (oneshot + slab-fallback). folly has no
    // swap; iterate + erase + invoke. Each erase is safe during iteration
    // (striped-lock map). Filter by conn if non-null.
    auto it = pending_.begin();
    while (it != pending_.end()) {
        if (conn != nullptr && it->second.conn != conn) {
            ++it;
            continue;
        }
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
        uint8_t s = completion_pool_[i].state.load(std::memory_order_relaxed);
        if (s == SLOT_PENDING_READY || s == SLOT_PENDING_CLAIMED) {
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

        // Scan slab pool for timed-out slots. Only act on PENDING_READY —
        // PENDING_CLAIMED means the submitter is still writing fields
        // (deadline not yet set). Read fields before CAS (slot is
        // PENDING_READY, no concurrent writer), then CAS PENDING_READY→FREE
        // to claim + release in one op. If the CAS fails, on_response
        // already handled it.
        for (size_t i = 0; i < pool_size_; ++i) {
            auto &slot = completion_pool_[i];
            if (slot.state.load(std::memory_order_acquire) != SLOT_PENDING_READY) {
                continue;
            }
            uint64_t dl = slot.deadline_ns.load(std::memory_order_relaxed);
            if (dl == 0 || now < dl) {
                continue; // no timeout or not yet expired
            }
            auto    cb       = slot.cb;
            auto    ud       = slot.user_data;
            auto    rid      = slot.request_id;
            uint8_t expected = SLOT_PENDING_READY;
            if (slot.state.compare_exchange_strong(expected, SLOT_FREE, std::memory_order_acq_rel)) {
                rpc_reaped().inc();
                invoke_c_complete(cb, ud, rid, nullptr, RpcError::Timeout);
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
