// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#include "crow-rpc/server/message.h"

#include "common_msg_generated.h"
#include "msg_type_generated.h"
#include "ret_code_generated.h"

#include <flatbuffers/flatbuffers.h>

#include <cassert>
#include <cstring>

namespace crow::rpc
{

uint64_t extract_request_id(const uint8_t *control, uint32_t len)
{
    if (control == nullptr || len == 0) {
        return 0;
    }
    // All common messages have `id` at VT_ID=4. We try ping request first,
    // then ping response, then unknown — they all share the same layout
    // for the id field.
    auto *ping_req = ::flatbuffers::GetRoot<proto::ConnectionPingRequest>(control);
    if (ping_req != nullptr) {
        return ping_req->id();
    }
    auto *ping_resp = ::flatbuffers::GetRoot<proto::ConnectionPingResponse>(control);
    if (ping_resp != nullptr) {
        return ping_resp->id();
    }
    auto *unknown = ::flatbuffers::GetRoot<proto::UnknownMessage>(control);
    if (unknown != nullptr) {
        return unknown->id();
    }
    return 0;
}

// Helper: serialize a flatbuffer offset into a pool Buffer.
static Buffer *finish_to_buffer(BufferPool *pool, flatbuffers::FlatBufferBuilder &fbb)
{
    uint32_t size = fbb.GetSize();
    Buffer  *buf  = pool->alloc(size);
    if (buf == nullptr) {
        return nullptr;
    }
    std::memcpy(buf->data, fbb.GetBufferPointer(), size);
    buf->write(buf->data, size);
    return buf;
}

Buffer *build_ping_request(BufferPool *pool, uint64_t request_id, uint64_t rpc_create_nano)
{
    flatbuffers::FlatBufferBuilder fbb(64);
    auto                           off = proto::CreateConnectionPingRequest(fbb, request_id, rpc_create_nano);
    fbb.Finish(off);
    return finish_to_buffer(pool, fbb);
}

Buffer *build_ping_response(BufferPool *pool, uint64_t request_id, uint64_t rpc_create_nano)
{
    flatbuffers::FlatBufferBuilder fbb(64);
    auto off = proto::CreateConnectionPingResponse(fbb, request_id, rpc_create_nano, proto::FBRetCode_Success);
    fbb.Finish(off);
    return finish_to_buffer(pool, fbb);
}

Buffer *build_unknown_response(BufferPool *pool, uint64_t request_id, uint64_t rpc_create_nano)
{
    flatbuffers::FlatBufferBuilder fbb(64);
    auto                           off = proto::CreateUnknownMessage(fbb, request_id, rpc_create_nano);
    fbb.Finish(off);
    return finish_to_buffer(pool, fbb);
}

Buffer *build_raw_control(BufferPool *pool, const uint8_t *data, uint32_t len)
{
    Buffer *buf = pool->alloc(len);
    if (buf == nullptr) {
        return nullptr;
    }
    buf->write(data, len);
    return buf;
}

OutFrame *build_out_frame(uint64_t request_id, uint16_t msg_type, Buffer *control, Buffer *data, uint8_t flags)
{
    auto *out             = new OutFrame;
    out->request_id       = request_id;
    out->header.msg_type  = msg_type;
    out->header.msg_size  = control != nullptr ? static_cast<uint16_t>(control->len) : 0;
    out->header.data_size = data != nullptr ? data->len : 0;
    out->header.flags     = flags;
    out->control          = control;
    out->data             = data;
    return out;
}

} // namespace crow::rpc
