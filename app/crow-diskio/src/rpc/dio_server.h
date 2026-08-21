// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// Diskio RPC server: wires disk I/O requests (write/read/fsync) from
// crow-rpc frames to DiskSet + IoEngine, and sends responses back via
// the transport when I/O completes.
#pragma once

#include "disk/disk_set.h"
#include "disk/types.h"
#include "engine/io_engine.h"

#include "crow-rpc/buffer.h"
#include "crow-rpc/connection.h"
#include "crow-rpc/framing.h"
#include "crow-rpc/server/handler.h"
#include "crow-rpc/server/server.h"
#include "crow-rpc/transport.h"

#include <cstdint>
#include <memory>

namespace crow::diskio
{

// DiskioServer holds the DiskSet + IoEngine and provides handler
// functions for the three diskio msg_types (write/read/fsync).
// The handlers are async: they return nullptr and submit the response
// via transport->submit_inline when the I/O completes.
class DiskioServer
{
  public:
    DiskioServer(std::shared_ptr<DiskSet> disk_set, std::shared_ptr<IoEngine> engine,
                 crow::rpc::SocketTransport *transport);

    // Handler functions (registered with RpcServer::register_handler).
    // Each parses the flatbuffer control from the Frame, looks up the
    // disk in DiskSet, and submits the I/O to the engine. The response
    // is sent asynchronously via submit_inline when the I/O completes.
    crow::rpc::OutFrame *handle_write(crow::rpc::Frame *request, crow::rpc::Connection *conn);
    crow::rpc::OutFrame *handle_read(crow::rpc::Frame *request, crow::rpc::Connection *conn);
    crow::rpc::OutFrame *handle_fsync(crow::rpc::Frame *request, crow::rpc::Connection *conn);

    // Register all three handlers on a server.
    void register_handlers(crow::rpc::RpcServer &server);

  private:
    std::shared_ptr<DiskSet>     disk_set_;
    std::shared_ptr<IoEngine>    engine_;
    crow::rpc::SocketTransport  *transport_;

    // Build a response control buffer for a diskio response msg_type.
    // ret_code is a proto::FBDiskIoRetCode value (int16_t to avoid
    // pulling the generated header into this .h).
    crow::rpc::Buffer *build_response_ctrl(crow::rpc::BufferPool *pool, uint64_t request_id,
                                           uint64_t rpc_create_nano, int16_t ret_code, uint16_t msg_type);

    // Send an error response synchronously (disk not found, parse error).
    void send_error_response(crow::rpc::Connection *conn, uint64_t request_id, uint64_t rpc_create_nano,
                             uint16_t msg_type, int16_t ret_code);
};

} // namespace crow::diskio
