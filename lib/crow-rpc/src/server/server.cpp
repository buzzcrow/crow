// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#include "crow-rpc/server/server.h"

#include "crow-rpc/server/handler.h"
#include "crow-rpc/server/message.h"
#include "msg_type_generated.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

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

    struct sockaddr_in bound{};
    socklen_t          bound_len = sizeof(bound);
    if (::getsockname(listen_fd_, reinterpret_cast<struct sockaddr *>(&bound), &bound_len) == 0) {
        listen_port_ = ntohs(bound.sin_port);
    }

    // Register built-in handlers.
    handlers_.register_handler(static_cast<uint16_t>(proto::FBMsgType_EConnectionPingRequest), handle_ping);

    return true;
}

void RpcServer::register_handler(uint16_t msg_type, HandlerFn handler)
{
    handlers_.register_handler(msg_type, std::move(handler));
}

void RpcServer::start()
{
    if (running_.exchange(true, std::memory_order_acq_rel)) {
        return;
    }
    transport_->start();
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
                break;
            }
            continue;
        }

        int flags = fcntl(fd, F_GETFL, 0);
        fcntl(fd, F_SETFL, flags | O_NONBLOCK);

        auto conn = transport_->create_connection(fd, "client");
        conn->set_on_frame([this](Frame *frame, Connection *c) { dispatch(frame, c); });
    }
}

void RpcServer::dispatch(Frame *frame, Connection *conn)
{
    uint16_t msg_type   = frame->header.msg_type;
    bool     is_one_way = (frame->header.flags & FLAG_ONE_WAY) != 0;

    HandlerFn handler = handlers_.get_handler(msg_type);
    if (!handler) {
        // Unknown msg_type — send UnknownMessage response (if not one-way).
        if (!is_one_way) {
            handler = handle_unknown;
        }
        else {
            delete frame;
            return;
        }
    }

    OutFrame *response = handler(frame, conn);
    if (response != nullptr) {
        // Submit the response back through the transport.
        transport_->submit(conn, response);
    }
}

} // namespace crow::rpc
