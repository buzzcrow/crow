// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "crow-rpc/c_api.h"
#include "crow-rpc/client/client.h"
#include "crow-rpc/co_client.h"
#include "crow-rpc/framing.h"
#include "crow-rpc/server/server.h"

// Concrete struct definitions for the C API opaque handles.
// Defined here so both c_api.cpp and co_client.cpp can access them.
struct crow_rpc_buffer_s
{
    crow::rpc::Buffer *buf;
};

struct crow_rpc_conn_s
{
    std::shared_ptr<crow::rpc::Connection> conn;
};

struct crow_rpc_client_s
{
    crow::rpc::RpcClient *client;
    // Aggregated stats from crow_rpc_co_spawn (coroutine client).
    crow_rpc_co_stats_t co_stats{};
};

struct crow_rpc_server_s
{
    crow::rpc::RpcServer *server;
};

namespace crow::rpc
{

// Internal helpers shared between RpcClient (slab path) and the C API
// adapter. Implemented in c_api.cpp where the crow_rpc_buffer_s concrete
// struct is defined.

// Convert a response Frame into C ABI buffer handles. Takes ownership of
// the frame (deletes it). The data buffer is ref_clone'd so the handle
// owns its own reference. Control is nullptr (fields extracted during parse).
// On error (frame == nullptr), both handles are set to nullptr.
void frame_to_c_handles(Frame *frame, crow_rpc_buffer_t *out_ctrl, crow_rpc_buffer_t *out_data);

// Invoke a C ABI completion callback with a response Frame. Converts the
// frame to handles, maps the RpcError to a crow_rpc_status, and calls cb.
// Takes ownership of the frame (frees it after invoking the callback).
void invoke_c_complete(crow_rpc_on_complete cb, void *user_data, uint64_t request_id, Frame *frame, RpcError err);

} // namespace crow::rpc
