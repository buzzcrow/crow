// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-rpc/server/message.h"

#include "common_msg_generated.h"
#include "msg_type_generated.h"
#include "ret_code_generated.h"

#include <flatbuffers/flatbuffers.h>

#include <cassert>
#include <cstring>

namespace crowdb::rpc
{

void extract_control_fields(const uint8_t *control, uint32_t len, uint64_t &out_request_id,
                            uint64_t &out_rpc_create_nano, uint64_t &out_response_create_nano)
{
    out_request_id           = 0;
    out_rpc_create_nano      = 0;
    out_response_create_nano = 0;
    if (control == nullptr || len == 0) {
        return;
    }
    // All common messages (ConnectionPingRequest/Response, UnknownMessage)
    // share the same flatbuffer layout for id + rpc_create_nano.
    // ConnectionPingResponse + UnknownMessage also have response_create_nano.
    // Use Verifier to safely access fields on untrusted input.
    ::flatbuffers::Verifier verifier(control, len);
    if (verifier.VerifyBuffer<proto::ConnectionPingResponse>()) {
        auto *resp = ::flatbuffers::GetRoot<proto::ConnectionPingResponse>(control);
        if (resp != nullptr) {
            out_request_id           = resp->id();
            out_rpc_create_nano      = resp->rpc_create_nano();
            out_response_create_nano = resp->response_create_nano();
        }
        return;
    }
    if (verifier.VerifyBuffer<proto::ConnectionPingRequest>()) {
        auto *req = ::flatbuffers::GetRoot<proto::ConnectionPingRequest>(control);
        if (req != nullptr) {
            out_request_id      = req->id();
            out_rpc_create_nano = req->rpc_create_nano();
        }
        return;
    }
    if (verifier.VerifyBuffer<proto::UnknownMessage>()) {
        auto *unk = ::flatbuffers::GetRoot<proto::UnknownMessage>(control);
        if (unk != nullptr) {
            out_request_id           = unk->id();
            out_rpc_create_nano      = unk->rpc_create_nano();
            out_response_create_nano = unk->response_create_nano();
        }
    }
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

Buffer *build_ping_response(BufferPool *pool, uint64_t request_id, uint64_t rpc_create_nano,
                            uint64_t response_create_nano)
{
    flatbuffers::FlatBufferBuilder fbb(64);
    auto off = proto::CreateConnectionPingResponse(fbb, request_id, rpc_create_nano, response_create_nano,
                                                   proto::FBRetCode_Success);
    fbb.Finish(off);
    return finish_to_buffer(pool, fbb);
}

Buffer *build_unknown_response(BufferPool *pool, uint64_t request_id, uint64_t rpc_create_nano,
                               uint64_t response_create_nano)
{
    flatbuffers::FlatBufferBuilder fbb(64);
    auto off = proto::CreateUnknownMessage(fbb, request_id, rpc_create_nano, response_create_nano);
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
    out->header.msg_size  = control != nullptr ? control->len : 0;
    out->header.data_size = data != nullptr ? data->len : 0;
    out->header.flags     = flags;
    out->control          = control;
    out->data             = data;
    return out;
}

} // namespace crowdb::rpc
