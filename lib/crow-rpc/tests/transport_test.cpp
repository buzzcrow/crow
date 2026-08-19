// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#include "crow-rpc/buffer.h"
#include "crow-rpc/framing.h"
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
using crow::rpc::SocketTransport;
using crow::rpc::SystemBufferPool;

// Loopback test: start a SocketTransport, create a TCP connection to a
// listening socket, send a frame, verify it arrives on the other side.
class TransportLoopbackTest : public ::testing::Test
{
  protected:
    void SetUp() override
    {
        // Create a listening socket.
        listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
        ASSERT_GE(listen_fd_, 0);

        int opt = 1;
        ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

        struct sockaddr_in addr{};
        addr.sin_family      = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port        = 0; // let the OS pick a port

        ASSERT_EQ(::bind(listen_fd_, reinterpret_cast<struct sockaddr *>(&addr), sizeof(addr)), 0);
        ASSERT_EQ(::listen(listen_fd_, 1), 0);

        // Get the assigned port.
        struct sockaddr_in bound{};
        socklen_t          bound_len = sizeof(bound);
        ::getsockname(listen_fd_, reinterpret_cast<struct sockaddr *>(&bound), &bound_len);
        port_ = ntohs(bound.sin_port);
    }

    void TearDown() override
    {
        if (listen_fd_ >= 0) {
            ::close(listen_fd_);
        }
    }

    int      listen_fd_ = -1;
    uint16_t port_      = 0;
};

TEST_F(TransportLoopbackTest, SendAndReceiveFrame)
{
    // Start the transport with 1 worker.
    SocketTransport transport(1, 1);
    transport.start();

    // Connect to the listening socket.
    int client_fd = ::socket(AF_INET, SOCK_STREAM, 0);
    ASSERT_GE(client_fd, 0);
    struct sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port        = htons(port_);
    ASSERT_EQ(::connect(client_fd, reinterpret_cast<struct sockaddr *>(&addr), sizeof(addr)), 0);

    // Accept on the server side.
    int server_fd = ::accept(listen_fd_, nullptr, nullptr);
    ASSERT_GE(server_fd, 0);

    // Set both sockets to non-blocking (the transport's event loop needs this).
    int flags = fcntl(client_fd, F_GETFL, 0);
    fcntl(client_fd, F_SETFL, flags | O_NONBLOCK);
    flags = fcntl(server_fd, F_GETFL, 0);
    fcntl(server_fd, F_SETFL, flags | O_NONBLOCK);

    // Create a connection for the server side (receiver).
    auto server_conn = transport.create_connection(server_fd, "server");

    // Atomic flag + received frame data.
    std::atomic<bool> got_frame{false};
    uint16_t          recv_msg_type    = 0;
    uint32_t          recv_control_len = 0;

    server_conn->set_on_frame([&](Frame *frame, Connection *) {
        recv_msg_type    = frame->header.msg_type;
        recv_control_len = frame->control_len;
        got_frame.store(true, std::memory_order_release);
        // Free the frame (parser-allocated buffers).
        delete frame;
    });

    // Build an OutFrame on the client side and send it via raw write
    // (bypassing the transport's send path — we're testing the receive
    // path here).
    Header h;
    h.msg_type  = 42;
    h.msg_size  = 16;
    h.data_size = 0;

    uint8_t buf[crow::rpc::HEADER_SIZE + 16];
    crow::rpc::serialize_header(buf, h);
    std::memset(buf + crow::rpc::HEADER_SIZE, 0xAB, 16);

    ssize_t written = ::write(client_fd, buf, sizeof(buf));
    ASSERT_EQ(written, static_cast<ssize_t>(sizeof(buf)));

    // Wait for the frame to arrive (up to 2 seconds).
    for (int i = 0; i < 200 && !got_frame.load(std::memory_order_acquire); i++) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    EXPECT_TRUE(got_frame.load(std::memory_order_acquire));
    EXPECT_EQ(recv_msg_type, 42u);
    EXPECT_EQ(recv_control_len, 16u);

    transport.stop();
    ::close(client_fd);
}

TEST_F(TransportLoopbackTest, ConnectionCloseCallback)
{
    SocketTransport transport(1, 1);
    transport.start();

    int client_fd = ::socket(AF_INET, SOCK_STREAM, 0);
    ASSERT_GE(client_fd, 0);
    struct sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port        = htons(port_);
    ASSERT_EQ(::connect(client_fd, reinterpret_cast<struct sockaddr *>(&addr), sizeof(addr)), 0);

    int server_fd = ::accept(listen_fd_, nullptr, nullptr);
    ASSERT_GE(server_fd, 0);

    int flags = fcntl(server_fd, F_GETFL, 0);
    fcntl(server_fd, F_SETFL, flags | O_NONBLOCK);

    auto server_conn = transport.create_connection(server_fd, "server");

    std::atomic<bool> closed{false};
    server_conn->set_on_close([&](Connection *) { closed.store(true, std::memory_order_release); });

    // Close the client side — server should detect EOF.
    ::close(client_fd);

    for (int i = 0; i < 200 && !closed.load(std::memory_order_acquire); i++) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    EXPECT_TRUE(closed.load(std::memory_order_acquire));

    transport.stop();
}
