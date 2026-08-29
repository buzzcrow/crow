// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// Diskio RPC server: wires disk I/O requests (write/read/fsync) from
// crowdb-rpc frames to DiskSet + IoEngine, and sends responses back via
// the transport when I/O completes.
#pragma once

#include "crowdb-rpc/buffer.h"
#include "crowdb-rpc/connection.h"
#include "crowdb-rpc/framing.h"
#include "crowdb-rpc/server/handler.h"
#include "crowdb-rpc/server/server.h"
#include "crowdb-rpc/transport.h"
#include "disk/disk_set.h"
#include "disk/types.h"
#include "engine/io_engine.h"

#include <cstdint>
#include <memory>

namespace crowdb::diskio
{

// DiskioServer holds the DiskSet and provides handler functions for
// the three diskio msg_types (write/read/fsync). Each disk owns its
// engine (shared uring/blocking, or a wrapper for dummy disks); the
// handler resolves the disk and calls disk->engine()->submit_*.
// The handlers are async: they return nullptr and submit the response
// via transport->submit_inline when the I/O completes.
class DiskioServer
{
  public:
    DiskioServer(std::shared_ptr<DiskSet> disk_set, crowdb::rpc::SocketTransport *transport);

    // Handler functions (registered with RpcServer::register_handler).
    // Each parses the flatbuffer control from the Frame, looks up the
    // disk in DiskSet, and submits the I/O to the engine. The response
    // is sent asynchronously via submit_inline when the I/O completes.
    crowdb::rpc::OutFrame *handle_write(crowdb::rpc::Frame *request, crowdb::rpc::Connection *conn);
    crowdb::rpc::OutFrame *handle_read(crowdb::rpc::Frame *request, crowdb::rpc::Connection *conn);
    crowdb::rpc::OutFrame *handle_fsync(crowdb::rpc::Frame *request, crowdb::rpc::Connection *conn);

    // Register all three handlers on a server.
    void register_handlers(crowdb::rpc::RpcServer &server);

  private:
    std::shared_ptr<DiskSet>    disk_set_;
    crowdb::rpc::SocketTransport *transport_;

    // Build a response control buffer for a diskio response msg_type.
    // ret_code is a proto::FBDiskIoRetCode value (int16_t to avoid
    // pulling the generated header into this .h).
    crowdb::rpc::Buffer *build_response_ctrl(crowdb::rpc::BufferPool *pool, uint64_t request_id, uint64_t rpc_create_nano,
                                           int16_t ret_code, uint16_t msg_type);

    // Send an error response synchronously (disk not found, parse error).
    void send_error_response(crowdb::rpc::Connection *conn, uint64_t request_id, uint64_t rpc_create_nano,
                             uint16_t msg_type, int16_t ret_code);
};

} // namespace crowdb::diskio
