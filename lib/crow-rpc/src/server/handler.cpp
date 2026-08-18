// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#include "crow-rpc/server/handler.h"

#include "common_msg_generated.h"
#include "crow-rpc/server/message.h"
#include "msg_type_generated.h"
#include "ret_code_generated.h"

#include <chrono>
#include <cstring>

namespace crow::rpc
{

OutFrame *handle_ping(Frame *request, Connection *conn)
{
    // Parse the ping request to get request_id + rpc_create_nano.
    uint64_t req_id      = 0;
    uint64_t create_nano = 0;
    if (request->control != nullptr && request->control_len > 0) {
        auto *ping = ::flatbuffers::GetRoot<proto::ConnectionPingRequest>(request->control);
        if (ping != nullptr) {
            req_id      = ping->id();
            create_nano = ping->rpc_create_nano();
        }
    }

    BufferPool *pool      = conn->pool();
    Buffer     *resp_ctrl = build_ping_response(pool, req_id, create_nano);

    delete request;

    auto *out =
        build_out_frame(req_id, static_cast<uint16_t>(proto::FBMsgType_EConnectionPingResponse), resp_ctrl, nullptr);
    return out;
}

OutFrame *handle_unknown(Frame *request, Connection *conn)
{
    uint64_t req_id      = extract_request_id(request->control, request->control_len);
    uint64_t create_nano = 0;
    if (request->control != nullptr && request->control_len > 0) {
        auto *ping = ::flatbuffers::GetRoot<proto::ConnectionPingRequest>(request->control);
        if (ping != nullptr) {
            create_nano = ping->rpc_create_nano();
        }
    }

    BufferPool *pool      = conn->pool();
    Buffer     *resp_ctrl = build_unknown_response(pool, req_id, create_nano);

    auto msg_type = static_cast<uint16_t>(proto::FBMsgType_EUnknownResponse);
    delete request;

    return build_out_frame(req_id, msg_type, resp_ctrl, nullptr);
}

} // namespace crow::rpc
