// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "crow-rpc/buffer.h"
#include "crow-rpc/server/handler.h"
#include "crow-rpc/transport.h"
#include "crow-rpc/transport/socket_transport.h"

#include <atomic>
#include <memory>
#include <string>
#include <thread>

namespace crow::rpc
{

// RpcServer accepts connections, parses frames, and dispatches to
// registered handlers by msg_type. Common handlers (ping) are registered
// automatically. The server owns the transport and the acceptor thread.
class RpcServer
{
  public:
    RpcServer(BufferPool *pool = nullptr, uint32_t num_workers = 1);
    ~RpcServer();

    // Listen on the given address + port. Must be called before start().
    // If port is 0, the OS assigns an ephemeral port.
    bool listen(const std::string &addr, int port);

    int listen_port() const
    {
        return listen_port_;
    }

    // Register a handler for a msg_type.
    void register_handler(uint16_t msg_type, HandlerFn handler);

    // Start the server: spawns worker threads + acceptor thread.
    void start();

    // Stop the server: closes listener, signals workers, joins threads.
    void stop();

    // The transport (for sending responses from async handlers + client connect).
    SocketTransport *transport()
    {
        return transport_.get();
    }

    // The buffer pool (for handlers that allocate response buffers).
    BufferPool *pool()
    {
        return pool_;
    }

    // Set a dispatch callback (executor model). When set, the I/O worker
    // calls this callback instead of running the C++ handler inline.
    // The callback takes ownership of the frame's control/data buffers
    // (malloc'd) and must be non-blocking. The callback receives the raw
    // Connection* as conn_handle — pass it to submit_response later.
    using DispatchCallback = void (*)(void *user_data, void *conn_handle, uint16_t msg_type, uint8_t *control,
                                      uint32_t control_len, uint8_t *data, uint32_t data_len);

    void set_dispatch_callback(DispatchCallback cb, void *user_data)
    {
        dispatch_callback_  = cb;
        dispatch_user_data_ = user_data;
    }

  private:
    BufferPool                      *pool_;
    bool                             owns_pool_;
    std::unique_ptr<SocketTransport> transport_;
    HandlerRegistry                  handlers_;

    int               listen_fd_   = -1;
    int               listen_port_ = 0;
    std::atomic<bool> running_{false};
    std::thread       acceptor_thread_;

    // Dispatch callback (executor model). When non-null, dispatch()
    // calls this instead of the C++ handler.
    DispatchCallback dispatch_callback_  = nullptr;
    void            *dispatch_user_data_ = nullptr;

    void acceptor_loop();
    void dispatch(Frame *frame, Connection *conn);
};

} // namespace crow::rpc
