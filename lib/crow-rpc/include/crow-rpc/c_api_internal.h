// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "crow-rpc/c_api.h"
#include "crow-rpc/client/client.h"
#include "crow-rpc/framing.h"

namespace crow::rpc
{

// Internal helpers shared between RpcClient (slab path) and the C API
// adapter. Implemented in c_api.cpp where the crow_rpc_buffer_s concrete
// struct is defined.

// Convert a response Frame into C ABI buffer handles. Takes ownership of
// the frame's control/data pointers (nulls them so the Frame destructor
// doesn't free them), then deletes the Frame. On error (frame == nullptr),
// both handles are set to nullptr.
void frame_to_c_handles(Frame *frame, crow_rpc_buffer_t *out_ctrl, crow_rpc_buffer_t *out_data);

// Invoke a C ABI completion callback with a response Frame. Converts the
// frame to handles, maps the RpcError to a crow_rpc_status, and calls cb.
// Takes ownership of the frame (frees it after invoking the callback).
void invoke_c_complete(crow_rpc_on_complete cb, void *user_data, uint64_t request_id, Frame *frame, RpcError err);

} // namespace crow::rpc
