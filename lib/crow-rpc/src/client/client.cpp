// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#include "crow-rpc/client/client.h"

#include "crow-rpc/server/message.h"

#include <cassert>

namespace crow::rpc
{

RpcClient::RpcClient() = default;

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
#if CROW_RPC_HAVE_FOLLY
    return pending_.size();
#else
    std::lock_guard<std::mutex> lock(pending_mu_);
    return pending_.size();
#endif
}

} // namespace crow::rpc
