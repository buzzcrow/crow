// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "crowdb-rpc/c_api.h"
#include "crowdb-rpc/client/client.h"
#include "crowdb-rpc/co_client.h"
#include "crowdb-rpc/framing.h"
#include "crowdb-rpc/server/server.h"

// Concrete struct definitions for the C API opaque handles.
// Defined here so both c_api.cpp and co_client.cpp can access them.
struct crowdb_rpc_buffer_s
{
    crowdb::rpc::Buffer *buf;
};

struct crowdb_rpc_conn_s
{
    std::shared_ptr<crowdb::rpc::Connection> conn;
};

struct crowdb_rpc_client_s
{
    crowdb::rpc::RpcClient *client;
    // Aggregated stats from crowdb_rpc_co_spawn (coroutine client).
    crowdb_rpc_co_stats_t co_stats{};
};

struct crowdb_rpc_server_s
{
    crowdb::rpc::RpcServer *server;
};

namespace crowdb::rpc
{

// Internal helpers shared between RpcClient (slab path) and the C API
// adapter. Implemented in c_api.cpp where the crowdb_rpc_buffer_s concrete
// struct is defined.

// Convert a response Frame into C ABI buffer handles. Takes ownership of
// the frame (deletes it). The data buffer is ref_clone'd so the handle
// owns its own reference. Control is nullptr (fields extracted during parse).
// On error (frame == nullptr), both handles are set to nullptr.
void frame_to_c_handles(Frame *frame, crowdb_rpc_buffer_t *out_ctrl, crowdb_rpc_buffer_t *out_data);

// Invoke a C ABI completion callback with a response Frame. Converts the
// frame to handles, maps the RpcError to a crowdb_rpc_status, and calls cb.
// Takes ownership of the frame (frees it after invoking the callback).
void invoke_c_complete(crowdb_rpc_on_complete cb, void *user_data, uint64_t request_id, Frame *frame, RpcError err);

// Shared handler trampoline: extracts request fields from the frame,
// invokes the C dispatch callback, and deletes the frame. Used by both
// server-side and client-side handler dispatch. The callback submits the
// response later via crowdb_rpc_server_submit_response (async pattern).
void invoke_c_handler(crowdb_rpc_handler_fn callback, void *user_data, Frame *request, Connection *conn);

} // namespace crowdb::rpc
