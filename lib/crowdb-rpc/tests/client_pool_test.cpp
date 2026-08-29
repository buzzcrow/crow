// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-common/request_id.h"
#include "crowdb-rpc/buffer.h"
#include "crowdb-rpc/c_api.h"
#include "crowdb-rpc/client/client.h"
#include "crowdb-rpc/client/rpc_client_metrics.h"
#include "crowdb-rpc/framing.h"
#include "crowdb-rpc/pool.h"
#include "crowdb-rpc/scheduled_executor.h"
#include "crowdb-rpc/transport/socket_transport.h"

#include <fcntl.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <thread>

using crowdb::rpc::Buffer;
using crowdb::rpc::Connection;
using crowdb::rpc::Frame;
using crowdb::rpc::OutFrame;
using crowdb::rpc::reset_rpc_client_counters;
using crowdb::rpc::rpc_reaped;
using crowdb::rpc::rpc_resp_missed;
using crowdb::rpc::RpcClient;
using crowdb::rpc::RpcError;
using crowdb::rpc::ScheduledExecutor;
using crowdb::rpc::SocketTransport;
using crowdb::rpc::SystemBufferPool;

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
    crowdb::rpc::ConnectionPool pool;
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
    crowdb::rpc::ConnectionPool pool;
    SystemBufferPool          buf_pool;

    auto c1 = std::make_shared<Connection>(1, "node1", &buf_pool);
    pool.add(c1);
    c1->close();

    EXPECT_EQ(pool.get(), nullptr);
    EXPECT_EQ(pool.healthy_count(), 0u);
}

TEST(ConnectionPoolTest, GetForEndpoint)
{
    crowdb::rpc::ConnectionPool pool;
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

    int                        listen_fd_ = -1;
    uint16_t                   port_      = 0;
    crowdb::common::RequestIdGen id_gen_;
};

// Callback state + C ABI callback for CallAndReceiveResponse test.
struct CallState
{
    std::atomic<bool> got_response{false};
    std::atomic<int>  recv_status{CROWDB_RPC_OK};
};

extern "C" void call_recv_cb(uint64_t /*request_id*/, crowdb_rpc_buffer_t /*control*/, crowdb_rpc_buffer_t data,
                             crowdb_rpc_status status, void *user_data)
{
    auto *s = static_cast<CallState *>(user_data);
    s->recv_status.store(status, std::memory_order_relaxed);
    if (data != nullptr) {
        crowdb_rpc_buffer_release(data);
    }
    s->got_response.store(true, std::memory_order_release);
}

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

    CallState state;
    RpcClient caller;
    caller.set_completion_pool_size(16);

    // Build a control buffer with the request.
    SystemBufferPool buf_pool;
    Buffer          *ctrl = buf_pool.alloc(32);
    ASSERT_NE(ctrl, nullptr);
    std::memset(ctrl->data, 0x42, 32);
    ctrl->write(ctrl->data, 32);

    // Submit the call — the callback fires when on_response is called.
    uint64_t req_id = id_gen_.next();
    bool     ok     = caller.send(&transport, client_conn.get(), req_id, ctrl, nullptr, 42, call_recv_cb, &state);

    // The request didn't actually go through the transport (client_conn
    // isn't registered with a worker), so we manually simulate the response
    // by calling on_response.
    EXPECT_TRUE(ok);

    // Build a fake response frame.
    auto *resp_frame             = new Frame;
    resp_frame->header.msg_type  = 43;
    resp_frame->header.msg_size  = 0;
    resp_frame->header.data_size = 0;
    resp_frame->request_id       = req_id;
    resp_frame->rpc_create_nano  = 0;
    resp_frame->data_buf         = nullptr;

    caller.on_response(req_id, resp_frame);

    EXPECT_TRUE(state.got_response.load(std::memory_order_acquire));
    EXPECT_EQ(state.recv_status.load(), CROWDB_RPC_OK);

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
    caller.fail_all(nullptr, RpcError::ConnectionClosed);
    EXPECT_EQ(caller.pending_count(), 0u);
}

// ── Slab fallback + reaper tests ───────────────────────────────────

// C ABI callback that records the request_id + status into a struct.
struct SlabCallbackState
{
    std::atomic<int>      call_count{0};
    std::atomic<int>      last_status{CROWDB_RPC_OK};
    std::atomic<uint64_t> last_request_id{0};
};

extern "C" void slab_test_cb(uint64_t        request_id, crowdb_rpc_buffer_t /*control*/, crowdb_rpc_buffer_t /*data*/,
                             crowdb_rpc_status status, void *user_data)
{
    auto *s = static_cast<SlabCallbackState *>(user_data);
    s->call_count.fetch_add(1, std::memory_order_relaxed);
    s->last_status.store(status, std::memory_order_relaxed);
    s->last_request_id.store(request_id, std::memory_order_relaxed);
}

// Test: slab fallback to map when the slot is occupied by a slow request.
// Two requests with request_ids that map to the same slab slot (differ by
// pool_size). The first occupies the slot (PENDING, no response). The
// second should fall back to the map. Both callbacks should fire.
TEST_F(CallerLoopbackTest, SlabFallbackToMapWhenSlotOccupied)
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
    // Server: discard requests (we manually drive responses via on_response).
    server_conn->set_on_frame([&](Frame *frame, Connection * /*conn*/) { delete frame; });

    auto client_conn              = std::make_shared<Connection>(100, "client", nullptr);
    client_conn->transport_handle = static_cast<uint64_t>(client_fd);

    RpcClient caller;
    caller.set_completion_pool_size(4); // pool_size=4, mask=3
    caller.attach(client_conn.get());
    reset_rpc_client_counters();

    SlabCallbackState state1;
    SlabCallbackState state2;

    SystemBufferPool buf_pool;
    Buffer          *ctrl1 = buf_pool.alloc(32);
    ASSERT_NE(ctrl1, nullptr);
    std::memset(ctrl1->data, 0x42, 32);
    ctrl1->write(ctrl1->data, 32);

    Buffer *ctrl2 = buf_pool.alloc(32);
    ASSERT_NE(ctrl2, nullptr);
    std::memset(ctrl2->data, 0x43, 32);
    ctrl2->write(ctrl2->data, 32);

    // req_id=1 → slot 1. req_id=5 → slot 1 (5 & 3 = 1). Same slot.
    uint64_t req1 = 1;
    uint64_t req2 = 5;

    // First call — occupies slot 1 (PENDING). No response yet.
    bool ok1 = caller.send(&transport, client_conn.get(), req1, ctrl1, nullptr, 42, slab_test_cb, &state1);
    EXPECT_TRUE(ok1);

    // Second call — slot 1 is occupied, should fall back to map.
    bool ok2 = caller.send(&transport, client_conn.get(), req2, ctrl2, nullptr, 42, slab_test_cb, &state2);
    EXPECT_TRUE(ok2);

    // Deliver response for req1 — slab path.
    auto *resp1            = new Frame;
    resp1->request_id      = req1;
    resp1->header.msg_type = 42;
    resp1->data_buf        = nullptr;
    caller.on_response(req1, resp1);

    // Deliver response for req2 — map path.
    auto *resp2            = new Frame;
    resp2->request_id      = req2;
    resp2->header.msg_type = 42;
    resp2->data_buf        = nullptr;
    caller.on_response(req2, resp2);

    EXPECT_EQ(state1.call_count.load(std::memory_order_acquire), 1);
    EXPECT_EQ(state1.last_status.load(std::memory_order_relaxed), CROWDB_RPC_OK);
    EXPECT_EQ(state2.call_count.load(std::memory_order_acquire), 1);
    EXPECT_EQ(state2.last_status.load(std::memory_order_relaxed), CROWDB_RPC_OK);

    transport.stop();
    ::close(client_fd);
}

// Test: reaper times out a slab slot that never gets a response.
// The callback should be invoked with CROWDB_RPC_ERR_TIMEOUT.
TEST_F(CallerLoopbackTest, ReaperTimesOutSlabSlot)
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
    server_conn->set_on_frame([&](Frame *frame, Connection * /*conn*/) { delete frame; });

    auto client_conn              = std::make_shared<Connection>(100, "client", nullptr);
    client_conn->transport_handle = static_cast<uint64_t>(client_fd);

    RpcClient caller;
    caller.set_completion_pool_size(4);
    caller.attach(client_conn.get());
    reset_rpc_client_counters();

    // Start reaper: 50ms timeout, 10ms scan interval.
    caller.start_reaper(50 * 1000 * 1000, 10 * 1000 * 1000);

    SlabCallbackState state;
    SystemBufferPool  buf_pool;
    Buffer           *ctrl = buf_pool.alloc(32);
    ASSERT_NE(ctrl, nullptr);
    std::memset(ctrl->data, 0x42, 32);
    ctrl->write(ctrl->data, 32);

    // Submit a request — no response will arrive.
    bool ok = caller.send(&transport, client_conn.get(), 1, ctrl, nullptr, 42, slab_test_cb, &state);
    EXPECT_TRUE(ok);

    // Wait for the reaper to time it out (up to 200ms).
    for (int i = 0; i < 40; i++) {
        if (state.call_count.load(std::memory_order_acquire) > 0) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }

    EXPECT_EQ(state.call_count.load(std::memory_order_acquire), 1);
    EXPECT_EQ(state.last_status.load(std::memory_order_relaxed), CROWDB_RPC_ERR_TIMEOUT);
    EXPECT_EQ(rpc_reaped().window(), 1u);
    EXPECT_EQ(caller.pending_count(), 0u);

    // Late response after timeout — should be dropped, no double-invoke.
    auto *late_resp            = new Frame;
    late_resp->request_id      = 1;
    late_resp->header.msg_type = 42;
    late_resp->data_buf        = nullptr;
    caller.on_response(1, late_resp);

    EXPECT_EQ(state.call_count.load(std::memory_order_acquire), 1); // still 1
    EXPECT_EQ(rpc_resp_missed().window(), 1u);

    caller.stop_reaper();
    transport.stop();
    ::close(client_fd);
}

// Test: reaper times out a map-fallback entry (slab full → map).
TEST_F(CallerLoopbackTest, ReaperTimesOutMapFallback)
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
    server_conn->set_on_frame([&](Frame *frame, Connection * /*conn*/) { delete frame; });

    auto client_conn              = std::make_shared<Connection>(100, "client", nullptr);
    client_conn->transport_handle = static_cast<uint64_t>(client_fd);

    RpcClient caller;
    caller.set_completion_pool_size(4);
    caller.attach(client_conn.get());
    reset_rpc_client_counters();
    caller.start_reaper(50 * 1000 * 1000, 10 * 1000 * 1000);

    SlabCallbackState state1;
    SlabCallbackState state2;

    SystemBufferPool buf_pool;
    Buffer          *ctrl1 = buf_pool.alloc(32);
    ASSERT_NE(ctrl1, nullptr);
    std::memset(ctrl1->data, 0x42, 32);
    ctrl1->write(ctrl1->data, 32);
    Buffer *ctrl2 = buf_pool.alloc(32);
    ASSERT_NE(ctrl2, nullptr);
    std::memset(ctrl2->data, 0x43, 32);
    ctrl2->write(ctrl2->data, 32);

    // req_id=1 → slot 1 (occupied, no response).
    caller.send(&transport, client_conn.get(), 1, ctrl1, nullptr, 42, slab_test_cb, &state1);
    // req_id=5 → slot 1 occupied → map fallback (no response).
    caller.send(&transport, client_conn.get(), 5, ctrl2, nullptr, 42, slab_test_cb, &state2);

    // Wait for reaper to time out both (up to 200ms).
    for (int i = 0; i < 40; i++) {
        if (state1.call_count.load(std::memory_order_acquire) > 0 &&
            state2.call_count.load(std::memory_order_acquire) > 0) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }

    EXPECT_EQ(state1.call_count.load(std::memory_order_acquire), 1);
    EXPECT_EQ(state1.last_status.load(std::memory_order_relaxed), CROWDB_RPC_ERR_TIMEOUT);
    EXPECT_EQ(state2.call_count.load(std::memory_order_acquire), 1);
    EXPECT_EQ(state2.last_status.load(std::memory_order_relaxed), CROWDB_RPC_ERR_TIMEOUT);
    EXPECT_EQ(rpc_reaped().window(), 2u); // slab + map
    EXPECT_EQ(caller.pending_count(), 0u);

    caller.stop_reaper();
    transport.stop();
    ::close(client_fd);
}
