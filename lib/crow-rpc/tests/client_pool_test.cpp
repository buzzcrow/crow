// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#include "crow-rpc/buffer.h"
#include "crow-rpc/client/client.h"
#include "crow-rpc/framing.h"
#include "crow-rpc/pool.h"
#include "crow-rpc/scheduled_executor.h"
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
using crow::rpc::CompletionCallback;
using crow::rpc::Connection;
using crow::rpc::Frame;
using crow::rpc::OutFrame;
using crow::rpc::RpcClient;
using crow::rpc::RpcError;
using crow::rpc::ScheduledExecutor;
using crow::rpc::SocketTransport;
using crow::rpc::SystemBufferPool;

// ── ScheduledExecutor tests ───────────────────────────────────────

TEST(ScheduledExecutorTest, FireDueTask)
{
    ScheduledExecutor exec;
    std::atomic<bool> fired{false};

    exec.schedule([&]() { fired.store(true); }, 10);

    // Wait for the task to be due.
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    int next_ms = exec.run_due_tasks();
    EXPECT_TRUE(fired.load());
    EXPECT_EQ(next_ms, -1); // no more pending tasks
}

TEST(ScheduledExecutorTest, CancelTask)
{
    ScheduledExecutor exec;
    std::atomic<bool> fired{false};

    auto id = exec.schedule([&]() { fired.store(true); }, 10);
    EXPECT_GT(id, 0u);

    EXPECT_TRUE(exec.cancel(id));
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    exec.run_due_tasks();
    EXPECT_FALSE(fired.load());
}

TEST(ScheduledExecutorTest, NextDeadline)
{
    ScheduledExecutor exec;
    std::atomic<bool> fired{false};

    exec.schedule([&]() { fired.store(true); }, 50);

    // Immediately — task not due, should return ~50ms.
    int next_ms = exec.run_due_tasks();
    EXPECT_FALSE(fired.load());
    EXPECT_GT(next_ms, 0);
    EXPECT_LE(next_ms, 50);

    // After 60ms — task due.
    std::this_thread::sleep_for(std::chrono::milliseconds(60));
    next_ms = exec.run_due_tasks();
    EXPECT_TRUE(fired.load());
    EXPECT_EQ(next_ms, -1);
}

// ── ConnectionPool tests ──────────────────────────────────────────

TEST(ConnectionPoolTest, RoundRobinSkipsUnhealthy)
{
    crow::rpc::ConnectionPool pool;
    SystemBufferPool          buf_pool;

    auto c1 = std::make_shared<Connection>(1, "node1", &buf_pool);
    auto c2 = std::make_shared<Connection>(2, "node2", &buf_pool);
    pool.add(c1);
    pool.add(c2);

    // Both healthy — should round-robin.
    Connection *first  = pool.get();
    Connection *second = pool.get();
    EXPECT_NE(first, nullptr);
    EXPECT_NE(second, nullptr);
    EXPECT_NE(first, second);

    // Close one — should always return the healthy one.
    c1->close();
    for (int i = 0; i < 5; i++) {
        Connection *c = pool.get();
        ASSERT_NE(c, nullptr);
        EXPECT_EQ(c, c2.get());
    }
}

TEST(ConnectionPoolTest, AllDownReturnsNull)
{
    crow::rpc::ConnectionPool pool;
    SystemBufferPool          buf_pool;

    auto c1 = std::make_shared<Connection>(1, "node1", &buf_pool);
    pool.add(c1);
    c1->close();

    EXPECT_EQ(pool.get(), nullptr);
    EXPECT_EQ(pool.healthy_count(), 0u);
}

TEST(ConnectionPoolTest, GetForEndpoint)
{
    crow::rpc::ConnectionPool pool;
    SystemBufferPool          buf_pool;

    auto c1 = std::make_shared<Connection>(1, "node1:8080", &buf_pool);
    auto c2 = std::make_shared<Connection>(2, "node2:8080", &buf_pool);
    pool.add(c1);
    pool.add(c2);

    Connection *c = pool.get_for("node1:8080");
    ASSERT_NE(c, nullptr);
    EXPECT_EQ(c, c1.get());

    EXPECT_EQ(pool.get_for("node3:8080"), nullptr);
}

// ── RpcClient tests (loopback) ─────────────────────────────────

class CallerLoopbackTest : public ::testing::Test
{
  protected:
    void SetUp() override
    {
        listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
        ASSERT_GE(listen_fd_, 0);
        int opt = 1;
        ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

        struct sockaddr_in addr{};
        addr.sin_family      = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port        = 0;
        ASSERT_EQ(::bind(listen_fd_, reinterpret_cast<struct sockaddr *>(&addr), sizeof(addr)), 0);
        ASSERT_EQ(::listen(listen_fd_, 1), 0);

        struct sockaddr_in bound{};
        socklen_t          len = sizeof(bound);
        ::getsockname(listen_fd_, reinterpret_cast<struct sockaddr *>(&bound), &len);
        port_ = ntohs(bound.sin_port);
    }

    void TearDown() override
    {
        if (listen_fd_ >= 0)
            ::close(listen_fd_);
    }

    int      listen_fd_ = -1;
    uint16_t port_      = 0;
};

TEST_F(CallerLoopbackTest, CallAndReceiveResponse)
{
    SocketTransport transport(1, 1);
    transport.start();

    // Connect client → server.
    int client_fd = ::socket(AF_INET, SOCK_STREAM, 0);
    ASSERT_GE(client_fd, 0);
    struct sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port        = htons(port_);
    ASSERT_EQ(::connect(client_fd, reinterpret_cast<struct sockaddr *>(&addr), sizeof(addr)), 0);

    int server_fd = ::accept(listen_fd_, nullptr, nullptr);
    ASSERT_GE(server_fd, 0);

    // Non-blocking.
    int flags = fcntl(client_fd, F_GETFL, 0);
    fcntl(client_fd, F_SETFL, flags | O_NONBLOCK);
    flags = fcntl(server_fd, F_GETFL, 0);
    fcntl(server_fd, F_SETFL, flags | O_NONBLOCK);

    // Server connection receives the request.
    auto server_conn = transport.create_connection(server_fd, "server");

    // Set up the server to echo back a response.
    std::atomic<bool> got_request{false};
    uint16_t          recv_msg_type = 0;

    server_conn->set_on_frame([&](Frame *frame, Connection * /*conn*/) {
        recv_msg_type = frame->header.msg_type;
        got_request.store(true, std::memory_order_release);
        delete frame;
    });

    // Client connection — we'll send via raw write (bypassing the transport
    // send path, since we're testing RpcClient's correlation logic, not
    // the send path).
    auto client_conn              = std::make_shared<Connection>(100, "client", nullptr);
    client_conn->transport_handle = static_cast<uint64_t>(client_fd);

    RpcClient         caller;
    std::atomic<bool> got_response{false};
    RpcError          recv_err = RpcError::Ok;

    // Build a control buffer with the request.
    SystemBufferPool buf_pool;
    Buffer          *ctrl = buf_pool.alloc(32);
    ASSERT_NE(ctrl, nullptr);
    std::memset(ctrl->data, 0x42, 32);
    ctrl->write(ctrl->data, 32);

    // Submit the call — the callback fires when on_response is called.
    uint64_t req_id = caller.next_request_id();
    uint64_t returned =
        caller.call(&transport, client_conn.get(), req_id, ctrl, nullptr, 42, [&](Frame * /*response*/, RpcError err) {
            recv_err = err;
            got_response.store(true, std::memory_order_release);
        });

    // The request didn't actually go through the transport (client_conn
    // isn't registered with a worker), so we manually simulate the response
    // by calling on_response.
    EXPECT_GT(returned, 0u);

    // Build a fake response frame.
    auto *resp_frame             = new Frame;
    resp_frame->header.msg_type  = 43;
    resp_frame->header.msg_size  = 0;
    resp_frame->header.data_size = 0;
    resp_frame->control          = nullptr;
    resp_frame->control_len      = 0;
    resp_frame->data             = nullptr;
    resp_frame->data_len         = 0;

    caller.on_response(req_id, resp_frame);

    EXPECT_TRUE(got_response.load(std::memory_order_acquire));
    EXPECT_EQ(recv_err, RpcError::Ok);

    // Late response (after the callback already fired) — should be discarded.
    auto *late_frame = new Frame;
    caller.on_response(req_id, late_frame);
    // No crash, no double-callback.

    transport.stop();
    ::close(client_fd);
}

TEST_F(CallerLoopbackTest, FailAllOnClose)
{
    RpcClient        caller;
    SystemBufferPool buf_pool;

    // Create a dummy connection (not connected, just for the pool).
    auto conn = std::make_shared<Connection>(1, "test", &buf_pool);

    // Test fail_all with 0 pending (edge case — should be a no-op).
    caller.fail_all(RpcError::ConnectionClosed);
    EXPECT_EQ(caller.pending_count(), 0u);
}
