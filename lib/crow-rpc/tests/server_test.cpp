// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#include "crow-rpc/buffer.h"
#include "crow-rpc/framing.h"
#include "crow-rpc/server/server.h"
#include "crow-rpc/transport/socket_transport.h"

#include <fcntl.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <thread>

using crow::rpc::Buffer;
using crow::rpc::Connection;
using crow::rpc::Frame;
using crow::rpc::Header;
using crow::rpc::OutFrame;
using crow::rpc::RpcServer;
using crow::rpc::SystemBufferPool;

// Full loopback: server listens, client connects and sends a frame, server
// dispatches to a registered handler.
TEST(RpcServerTest, FullLoopbackHandlerDispatch)
{
    RpcServer server;
    ASSERT_TRUE(server.listen("127.0.0.1", 0));
    int port = server.listen_port();
    ASSERT_GT(port, 0);

    // Register a custom handler for msg_type 100.
    std::atomic<bool> handler_called{false};
    uint16_t          recv_msg_type = 0;

    server.register_handler(100, [&](Frame *frame, Connection * /*conn*/) {
        recv_msg_type = frame->header.msg_type;
        handler_called.store(true, std::memory_order_release);
        delete frame;
        return static_cast<OutFrame *>(nullptr); // no response for now
    });

    server.start();

    // Connect a client.
    int client_fd = ::socket(AF_INET, SOCK_STREAM, 0);
    ASSERT_GE(client_fd, 0);
    struct sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port        = htons(static_cast<uint16_t>(port));
    ASSERT_EQ(::connect(client_fd, reinterpret_cast<struct sockaddr *>(&addr), sizeof(addr)), 0);

    // Send a frame with msg_type 100.
    Header h;
    h.msg_type  = 100;
    h.msg_size  = 16;
    h.data_size = 0;

    uint8_t buf[crow::rpc::HEADER_SIZE + 16];
    crow::rpc::serialize_header(buf, h);
    std::memset(buf + crow::rpc::HEADER_SIZE, 0xAB, 16);

    ssize_t written = ::write(client_fd, buf, sizeof(buf));
    ASSERT_EQ(written, static_cast<ssize_t>(sizeof(buf)));

    // Wait for the handler to fire.
    for (int i = 0; i < 200 && !handler_called.load(std::memory_order_acquire); i++) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    EXPECT_TRUE(handler_called.load(std::memory_order_acquire));
    EXPECT_EQ(recv_msg_type, 100u);

    ::close(client_fd);
    server.stop();
}

// Server should handle multiple connections.
TEST(RpcServerTest, MultipleConnections)
{
    RpcServer server;
    ASSERT_TRUE(server.listen("127.0.0.1", 0));
    int port = server.listen_port();
    ASSERT_GT(port, 0);

    std::atomic<int> handler_count{0};
    server.register_handler(200, [&](Frame *frame, Connection * /*conn*/) {
        handler_count.fetch_add(1, std::memory_order_relaxed);
        delete frame;
        return static_cast<OutFrame *>(nullptr);
    });

    server.start();

    // Connect 3 clients and send a frame from each.
    int fds[3];
    for (int i = 0; i < 3; i++) {
        fds[i] = ::socket(AF_INET, SOCK_STREAM, 0);
        ASSERT_GE(fds[i], 0);
        struct sockaddr_in addr{};
        addr.sin_family      = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port        = htons(static_cast<uint16_t>(port));
        ASSERT_EQ(::connect(fds[i], reinterpret_cast<struct sockaddr *>(&addr), sizeof(addr)), 0);

        Header h;
        h.msg_type  = 200;
        h.msg_size  = 8;
        h.data_size = 0;
        uint8_t buf[crow::rpc::HEADER_SIZE + 8];
        crow::rpc::serialize_header(buf, h);
        std::memset(buf + crow::rpc::HEADER_SIZE, 0xCD, 8);
        ::write(fds[i], buf, sizeof(buf));
    }

    // Wait for all 3 handlers to fire.
    for (int i = 0; i < 300 && handler_count.load(std::memory_order_acquire) < 3; i++) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    EXPECT_EQ(handler_count.load(std::memory_order_acquire), 3);

    for (int i = 0; i < 3; i++) {
        ::close(fds[i]);
    }
    server.stop();
}
