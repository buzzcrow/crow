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

TEST_F(TransportLoopbackTest, StopWakesIdleWorker)
{
    SocketTransport transport(1, 1);
    transport.start();

    const auto started = std::chrono::steady_clock::now();
    transport.stop();
    const auto elapsed = std::chrono::steady_clock::now() - started;

    EXPECT_LT(elapsed, std::chrono::milliseconds(500));
}

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
    uint16_t          recv_msg_type = 0;
    uint32_t          recv_msg_size = 0;

    server_conn->set_on_frame([&](Frame *frame, Connection *) {
        recv_msg_type = frame->header.msg_type;
        recv_msg_size = frame->header.msg_size;
        got_frame.store(true, std::memory_order_release);
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
    EXPECT_EQ(recv_msg_size, 16u);

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

// Regression: when a client disconnects (EOF), the transport must close
// the server-side fd and remove it from epoll. Without this, the
// level-triggered worker spins at 100% CPU re-reading EOF forever.
TEST_F(TransportLoopbackTest, EofCloseStopsWorkerSpin)
{
    SocketTransport transport(1, 1); // level-triggered (no ONESHOT)
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

    // Wait for the close callback.
    for (int i = 0; i < 200 && !closed.load(std::memory_order_acquire); i++) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_TRUE(closed.load(std::memory_order_acquire));

    // Give the worker one more poll cycle to process the cleanup.
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // The transport must have closed the server-side fd. If it didn't,
    // the fd is still open and epoll keeps firing EOF — the spin.
    EXPECT_EQ(fcntl(server_fd, F_GETFL), -1);
    EXPECT_EQ(errno, EBADF);

    // The fd is closed (EBADF) — the worker can't spin on a closed fd.
    // The old read_calls-based spin check was removed with the raw
    // atomics cleanup; the fd closure check above is sufficient.

    transport.stop();
}

// Large data payload: header+control fit in recv_buf, but the 1MB data
// payload exceeds recv_buf (64KB). After process_recv_bytes parses the
// header+control and drains recv_buf, the parser enters ReadingData state.
// The direct-read path must read the remaining data bytes straight into
// data_buf_ — no extra copy through recv_buf. Verifies the frame arrives
// intact with correct data content.
TEST_F(TransportLoopbackTest, LargeDataPayloadDirectRead)
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

    int flags = fcntl(client_fd, F_GETFL, 0);
    fcntl(client_fd, F_SETFL, flags | O_NONBLOCK);
    flags = fcntl(server_fd, F_GETFL, 0);
    fcntl(server_fd, F_SETFL, flags | O_NONBLOCK);

    auto server_conn = transport.create_connection(server_fd, "server");

    // 1MB data payload — exceeds recv_buf (64KB), forces direct-read path.
    constexpr uint32_t DATA_SIZE = 1024 * 1024;
    constexpr uint32_t CTRL_SIZE = 32;

    std::atomic<bool>    got_frame{false};
    uint16_t             recv_msg_type  = 0;
    uint32_t             recv_data_size = 0;
    std::vector<uint8_t> recv_data;

    server_conn->set_on_frame([&](Frame *frame, Connection *) {
        recv_msg_type  = frame->header.msg_type;
        recv_data_size = frame->header.data_size;
        if (frame->data_buf != nullptr) {
            recv_data.assign(frame->data_buf->data, frame->data_buf->data + frame->data_buf->len);
        }
        got_frame.store(true, std::memory_order_release);
        delete frame;
    });

    // Build the frame: header + control + 1MB data.
    Header h;
    h.msg_type  = 99;
    h.msg_size  = CTRL_SIZE;
    h.data_size = DATA_SIZE;

    std::vector<uint8_t> ctrl(CTRL_SIZE, 0xAB);
    std::vector<uint8_t> data(DATA_SIZE);
    for (uint32_t i = 0; i < DATA_SIZE; i++) {
        data[i] = static_cast<uint8_t>(i % 256);
    }

    std::vector<uint8_t> buf(crow::rpc::HEADER_SIZE + CTRL_SIZE + DATA_SIZE);
    crow::rpc::serialize_header(buf.data(), h);
    std::memcpy(buf.data() + crow::rpc::HEADER_SIZE, ctrl.data(), CTRL_SIZE);
    std::memcpy(buf.data() + crow::rpc::HEADER_SIZE + CTRL_SIZE, data.data(), DATA_SIZE);

    // Write in chunks — the kernel send buffer may not accept 1MB at once.
    size_t total_written = 0;
    while (total_written < buf.size()) {
        ssize_t n = ::write(client_fd, buf.data() + total_written, buf.size() - total_written);
        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
                continue;
            }
            break;
        }
        total_written += static_cast<size_t>(n);
    }
    ASSERT_EQ(total_written, buf.size());

    // Wait for the frame to arrive (up to 5 seconds — 1MB over loopback).
    for (int i = 0; i < 500 && !got_frame.load(std::memory_order_acquire); i++) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    EXPECT_TRUE(got_frame.load(std::memory_order_acquire));
    EXPECT_EQ(recv_msg_type, 99u);
    EXPECT_EQ(recv_data_size, DATA_SIZE);
    ASSERT_EQ(recv_data.size(), DATA_SIZE);

    // Verify data content — catches corruption from the direct-read path.
    for (uint32_t i = 0; i < DATA_SIZE; i++) {
        EXPECT_EQ(recv_data[i], static_cast<uint8_t>(i % 256)) << "mismatch at byte " << i;
        if (recv_data[i] != static_cast<uint8_t>(i % 256)) {
            break; // don't spam 1M failures
        }
    }

    transport.stop();
    ::close(client_fd);
}
