// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#include "crow-rpc/client/client.h"

#include "crow-rpc/c_api_internal.h"
#include "crow-rpc/server/message.h"

#include <cassert>

namespace crow::rpc
{

RpcClient::RpcClient() = default;

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

uint64_t RpcClient::call(Transport *transport, Connection *conn, uint64_t request_id, Buffer *control, Buffer *data,
                         uint16_t msg_type, CompletionCallback on_complete)
{
    // Insert into pending map before submit (so on_response can find it
    // even if the response arrives before submit returns — unlikely but
    // possible on loopback).
    pending_.insert_or_assign(request_id, std::move(on_complete));

    OutFrame *frame = build_frame(request_id, control, data, msg_type, 0);
    if (!transport->submit(conn, frame)) {
        // Submit failed — remove from pending, invoke callback with error.
        CompletionCallback cb;
#if CROW_RPC_HAVE_FOLLY
        auto it = pending_.find(request_id);
        if (it != pending_.end()) {
            cb = std::move(it->second);
            pending_.erase(it);
        }
#else
        {
            std::lock_guard<std::mutex> lock(pending_mu_);
            auto                        it = pending_.find(request_id);
            if (it != pending_.end()) {
                cb = std::move(it->second);
                pending_.erase(it);
            }
        }
#endif
        if (cb) {
            cb(nullptr, RpcError::SendQueueFull);
        }
        // Release the buffers (caller transferred ownership to us).
        if (frame->control != nullptr)
            frame->control->release();
        if (frame->data != nullptr)
            frame->data->release();
        delete frame;
        return 0;
    }

    return request_id;
}

// Callback-based call: slab slot + inline callback, no oneshot channel.
// Flow: doc/working/rpc-echo-flow-analysis.md § "Echo Flow — Callback Model".
bool RpcClient::call_callback(Transport *transport, Connection *conn, uint64_t request_id, Buffer *control,
                              Buffer *data, uint16_t msg_type, crow_rpc_on_complete cb, void *user_data)
{
    assert(completion_pool_ != nullptr && "call_callback without set_completion_pool_size");
    size_t idx  = request_id & pool_mask_;
    auto  &slot = completion_pool_[idx];

    // The slot must be FREE — the caller guarantees at most pool_size
    // in-flight, so no two in-flight requests share a slot.
    slot.request_id = request_id;
    slot.cb         = cb;
    slot.user_data  = user_data;
    slot.state.store(SLOT_PENDING, std::memory_order_release);

    OutFrame *frame = build_frame(request_id, control, data, msg_type, 0);
    if (!transport->submit(conn, frame)) {
        // Submit failed — mark slot FREE so it can be reused. The
        // callback is NOT invoked (caller handles the error).
        slot.state.store(SLOT_FREE, std::memory_order_release);
        if (frame->control != nullptr)
            frame->control->release();
        if (frame->data != nullptr)
            frame->data->release();
        delete frame;
        return false;
    }
    return true;
}

bool RpcClient::call_one_way(Transport *transport, Connection *conn, Buffer *control, Buffer *data, uint16_t msg_type)
{
    // request_id 0 signals one-way (no pending entry, no callback).
    OutFrame *frame = build_frame(0, control, data, msg_type, FLAG_ONE_WAY);
    if (!transport->submit(conn, frame)) {
        if (frame->control != nullptr)
            frame->control->release();
        if (frame->data != nullptr)
            frame->data->release();
        delete frame;
        return false;
    }
    return true;
}

void RpcClient::attach(Connection *conn)
{
    // Set the on_frame callback to route response frames to on_response.
    // The request_id is extracted from the flatbuffer control message.
    conn->set_on_frame([this](Frame *frame, Connection * /*conn*/) {
        uint64_t req_id = extract_request_id(frame->control, frame->control_len);
        on_response(req_id, frame);
    });
}

void RpcClient::on_response(uint64_t request_id, Frame *response)
{
    // Slab path (callback model): O(1) index lookup, no hash. Check the
    // slab pool first — if the slot is PENDING for this request_id, it's
    // a callback-model request. The atomic state serializes submitter
    // vs I/O worker access.
    if (completion_pool_ != nullptr) {
        size_t idx  = request_id & pool_mask_;
        auto  &slot = completion_pool_[idx];
        if (slot.state.load(std::memory_order_acquire) == SLOT_PENDING && slot.request_id == request_id) {
            // Set DONE, then invoke the callback. Do NOT reset to FREE
            // after the callback — the callback may reuse this slot
            // (via call_callback → SLOT_PENDING) for the next request.
            // Resetting to FREE here would overwrite that PENDING.
            slot.state.store(SLOT_DONE, std::memory_order_release);
            auto cb = slot.cb;
            auto ud = slot.user_data;
            invoke_c_complete(cb, ud, request_id, response, RpcError::Ok);
            return;
        }
    }

    // Map path (oneshot model): folly ConcurrentHashMap or mutex+map.
    CompletionCallback cb;
#if CROW_RPC_HAVE_FOLLY
    auto it = pending_.find(request_id);
    if (it == pending_.end()) {
        // Late response after timeout or duplicate — discard.
        delete response;
        return;
    }
    cb = std::move(it->second);
    pending_.erase(it);
#else
    {
        std::lock_guard<std::mutex> lock(pending_mu_);
        auto                        it = pending_.find(request_id);
        if (it == pending_.end()) {
            // Late response after timeout or duplicate — discard.
            delete response;
            return;
        }
        cb = std::move(it->second);
        pending_.erase(it);
    }
#endif
    if (cb) {
        cb(response, RpcError::Ok);
    }
    else {
        delete response;
    }
}

void RpcClient::fail_all(RpcError err)
{
    // Fail slab-pending requests (callback model).
    for (size_t i = 0; i < pool_size_; ++i) {
        auto &slot = completion_pool_[i];
        if (slot.state.load(std::memory_order_acquire) == SLOT_PENDING) {
            slot.state.store(SLOT_DONE, std::memory_order_release);
            auto cb = slot.cb;
            auto ud = slot.user_data;
            invoke_c_complete(cb, ud, slot.request_id, nullptr, err);
            slot.state.store(SLOT_FREE, std::memory_order_release);
        }
    }

    // Fail map-pending requests (oneshot model).
#if CROW_RPC_HAVE_FOLLY
    // folly::ConcurrentHashMap has no swap; iterate + erase + invoke.
    // Each erase is safe during iteration (striped-lock map).
    auto it = pending_.begin();
    while (it != pending_.end()) {
        CompletionCallback cb  = std::move(it->second);
        auto               key = it->first;
        ++it;
        pending_.erase(key);
        if (cb) {
            cb(nullptr, err);
        }
    }
#else
    std::unordered_map<uint64_t, CompletionCallback> to_fail;
    {
        std::lock_guard<std::mutex> lock(pending_mu_);
        to_fail.swap(pending_);
    }
    for (auto &[_, cb] : to_fail) {
        if (cb) {
            cb(nullptr, err);
        }
    }
#endif
}

size_t RpcClient::pending_count()
{
    size_t n = 0;
    for (size_t i = 0; i < pool_size_; ++i) {
        if (completion_pool_[i].state.load(std::memory_order_relaxed) == SLOT_PENDING) {
            ++n;
        }
    }
#if CROW_RPC_HAVE_FOLLY
    n += pending_.size();
#else
    {
        std::lock_guard<std::mutex> lock(pending_mu_);
        n += pending_.size();
    }
#endif
    return n;
}

} // namespace crow::rpc
