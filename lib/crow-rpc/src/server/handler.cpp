// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#include "crow-rpc/server/handler.h"

#include "crow-rpc/server/message.h"
#include "msg_type_generated.h"

#include <cstring>

namespace crow::rpc
{

OutFrame *handle_ping(Frame *request, Connection *conn)
{
    // request_id + rpc_create_nano extracted during parse (buzz-cpp style).
    uint64_t req_id      = request->request_id;
    uint64_t create_nano = request->rpc_create_nano;

    BufferPool *pool      = conn->pool();
    Buffer     *resp_ctrl = build_ping_response(pool, req_id, create_nano);

    delete request;

    auto *out =
        build_out_frame(req_id, static_cast<uint16_t>(proto::FBMsgType_EConnectionPingResponse), resp_ctrl, nullptr);
    return out;
}

OutFrame *handle_unknown(Frame *request, Connection *conn)
{
    uint64_t req_id      = request->request_id;
    uint64_t create_nano = request->rpc_create_nano;

    BufferPool *pool      = conn->pool();
    Buffer     *resp_ctrl = build_unknown_response(pool, req_id, create_nano);

    auto msg_type = static_cast<uint16_t>(proto::FBMsgType_EUnknownResponse);
    delete request;

    return build_out_frame(req_id, msg_type, resp_ctrl, nullptr);
}

} // namespace crow::rpc
