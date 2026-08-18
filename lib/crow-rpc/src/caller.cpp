// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#include "crow-rpc/caller.h"

#include <cassert>

namespace crow::rpc
{

RemoteCaller::RemoteCaller() = default;

OutFrame *RemoteCaller::build_frame(uint64_t request_id, Buffer *control, Buffer *data, uint16_t msg_type,
                                    uint8_t flags)
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

uint64_t RemoteCaller::call(Transport *transport, Connection *conn, Buffer *control, Buffer *data, uint16_t msg_type,
                            CompletionCallback on_complete)
{
    uint64_t request_id = next_request_id_.fetch_add(1, std::memory_order_relaxed);

    // Insert into pending map before submit (so on_response can find it
    // even if the response arrives before submit returns — unlikely but
    // possible on loopback).
    {
        std::lock_guard<std::mutex> lock(pending_mu_);
        pending_[request_id] = std::move(on_complete);
    }

    OutFrame *frame = build_frame(request_id, control, data, msg_type, 0);
    if (!transport->submit(conn, frame)) {
        // Submit failed — remove from pending, invoke callback with error.
        CompletionCallback cb;
        {
            std::lock_guard<std::mutex> lock(pending_mu_);
            auto                        it = pending_.find(request_id);
            if (it != pending_.end()) {
                cb = std::move(it->second);
                pending_.erase(it);
            }
        }
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

bool RemoteCaller::call_one_way(Transport *transport, Connection *conn, Buffer *control, Buffer *data,
                                uint16_t msg_type)
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

void RemoteCaller::on_response(uint64_t request_id, Frame *response)
{
    CompletionCallback cb;
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
    if (cb) {
        cb(response, RpcError::Ok);
    }
    else {
        delete response;
    }
}

void RemoteCaller::fail_all(RpcError err)
{
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
}

size_t RemoteCaller::pending_count()
{
    std::lock_guard<std::mutex> lock(pending_mu_);
    return pending_.size();
}

} // namespace crow::rpc
