// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#include "crow-rpc/server.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cassert>
#include <cerrno>
#include <cstring>

namespace crow::rpc
{

RpcServer::RpcServer(BufferPool *pool) : pool_(pool), owns_pool_(pool == nullptr)
{
    if (pool_ == nullptr) {
        pool_ = new SystemBufferPool();
    }
    transport_ = std::make_unique<SocketTransport>(1, pool_);
}

RpcServer::~RpcServer()
{
    stop();
    if (owns_pool_) {
        delete pool_;
    }
}

bool RpcServer::listen(const std::string &addr, int port)
{
    listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_ < 0) {
        return false;
    }

    int opt = 1;
    ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in sa{};
    sa.sin_family = AF_INET;
    sa.sin_port   = htons(static_cast<uint16_t>(port));
    if (addr == "0.0.0.0" || addr.empty()) {
        sa.sin_addr.s_addr = htonl(INADDR_ANY);
    }
    else {
        ::inet_pton(AF_INET, addr.c_str(), &sa.sin_addr);
    }

    if (::bind(listen_fd_, reinterpret_cast<struct sockaddr *>(&sa), sizeof(sa)) < 0) {
        ::close(listen_fd_);
        listen_fd_ = -1;
        return false;
    }

    if (::listen(listen_fd_, 128) < 0) {
        ::close(listen_fd_);
        listen_fd_ = -1;
        return false;
    }

    // Get the assigned port (if port was 0).
    struct sockaddr_in bound{};
    socklen_t          bound_len = sizeof(bound);
    if (::getsockname(listen_fd_, reinterpret_cast<struct sockaddr *>(&bound), &bound_len) == 0) {
        listen_port_ = ntohs(bound.sin_port);
    }

    // Register the built-in ping handler.
    register_handler(2, handle_ping); // EConnectionPingRequest = 2

    return true;
}

int RpcServer::listen_port() const
{
    return listen_port_;
}

void RpcServer::register_handler(uint16_t msg_type, HandlerFn handler)
{
    std::lock_guard<std::mutex> lock(handlers_mu_);
    handlers_[msg_type] = std::move(handler);
}

void RpcServer::start()
{
    if (running_.exchange(true, std::memory_order_acq_rel)) {
        return; // already running
    }
    transport_->start();

    // Register the listen fd with the first worker's engine.
    if (listen_fd_ >= 0 && !transport_->get_worker()) {
        return;
    }

    // Spawn the acceptor thread.
    acceptor_thread_ = std::thread([this] { acceptor_loop(); });
}

void RpcServer::stop()
{
    if (!running_.exchange(false, std::memory_order_acq_rel)) {
        return;
    }
    if (listen_fd_ >= 0) {
        ::close(listen_fd_);
        listen_fd_ = -1;
    }
    if (acceptor_thread_.joinable()) {
        acceptor_thread_.join();
    }
    transport_->stop();
}

void RpcServer::acceptor_loop()
{
    while (running_.load(std::memory_order_relaxed)) {
        int fd = ::accept(listen_fd_, nullptr, nullptr);
        if (fd < 0) {
            if (errno == EINTR) {
                continue;
            }
            if (!running_.load(std::memory_order_relaxed)) {
                break; // shutdown
            }
            // Real error — log and continue.
            continue;
        }

        // Set non-blocking.
        int flags = fcntl(fd, F_GETFL, 0);
        fcntl(fd, F_SETFL, flags | O_NONBLOCK);

        // Create a connection and add it to a worker.
        auto conn = transport_->create_connection(fd, "client");

        // Set the on_frame callback to dispatch to handlers.
        conn->set_on_frame([this](Frame *frame, Connection *c) { dispatch(frame, c); });

        // The connection is now owned by the worker. We release our
        // shared_ptr here — the worker holds its own copy.
    }
}

void RpcServer::dispatch(Frame *frame, Connection *conn)
{
    uint16_t msg_type = frame->header.msg_type;

    HandlerFn handler;
    {
        std::lock_guard<std::mutex> lock(handlers_mu_);
        auto                        it = handlers_.find(msg_type);
        if (it != handlers_.end()) {
            handler = it->second;
        }
    }

    if (!handler) {
        // Unknown msg_type — send back an error response if this was a
        // request (not one-way). For simplicity, just discard the frame.
        delete frame;
        return;
    }

    // Invoke the handler. It may return a response Frame* or nullptr
    // (one-way / async). The handler owns the request frame (must delete
    // it or transfer ownership).
    Frame *response = handler(frame, conn);
    if (response != nullptr) {
        // Build an OutFrame and submit it.
        auto *out         = new OutFrame;
        out->request_id   = 0; // response — request_id is in the control msg
        out->header       = response->header;
        out->header.flags = 0;
        // The response frame's control/data are raw pointers (from the
        // handler). We need to wrap them in Buffers for the transport.
        // For simplicity in v1, the handler returns a Frame with control
        // and data as malloc'd bytes; we wrap them in Buffer here.
        // TODO: the handler should return Buffer* directly, not raw ptrs.
        // For now, just delete the response (ping handler returns nullptr).
        delete response;
        delete out;
    }
}

Frame *RpcServer::handle_ping(Frame *request, Connection * /*conn*/)
{
    // Ping is a no-op response for now — the real implementation will
    // parse the ConnectionPingRequest flatbuffer and build a
    // ConnectionPingResponse. For v1, just discard the request.
    delete request;
    return nullptr; // no response yet (will be wired with flatbuffer API)
}

} // namespace crow::rpc
