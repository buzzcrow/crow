// Copyright 2026-present Gian <crow.db@outlook.com>

#include "common_msg_generated.h"
#include "crowdb-rpc/buffer.h"
#include "crowdb-rpc/c_api.h"
#include "crowdb-rpc/client/client.h"
#include "crowdb-rpc/framing.h"
#include "crowdb-rpc/server/handler.h"
#include "crowdb-rpc/server/message.h"
#include "crowdb-rpc/server/server.h"
#include "crowdb-rpc/transport/socket_transport.h"
#include "msg_type_generated.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <thread>

using crowdb::rpc::Buffer;
using crowdb::rpc::BufferPool;
using crowdb::rpc::build_out_frame;
using crowdb::rpc::build_ping_request;
using crowdb::rpc::Connection;
using crowdb::rpc::Frame;
using crowdb::rpc::OutFrame;
using crowdb::rpc::RpcClient;
using crowdb::rpc::RpcServer;
using crowdb::rpc::SocketTransport;
using crowdb::rpc::SystemBufferPool;

// ── Callback state + C ABI callback for loopback tests ─────────────

struct LoopbackState
{
    std::atomic<bool>    got_response{false};
    uint64_t             recv_request_id{0};
    bool                 data_matches{false};
    std::vector<uint8_t> expected_payload;
};

extern "C" void loopback_on_complete(uint64_t request_id, crowdb_rpc_buffer_t /*control*/, crowdb_rpc_buffer_t data,
                                     crowdb_rpc_status status, void *user_data)
{
    auto *s = static_cast<LoopbackState *>(user_data);
    if (status == CROWDB_RPC_OK) {
        s->recv_request_id = request_id;
        if (data != nullptr && !s->expected_payload.empty()) {
            uint32_t    len = crowdb_rpc_buffer_len(data);
            const auto *ptr = crowdb_rpc_buffer_data(data);
            s->data_matches =
                (len == s->expected_payload.size() && std::memcmp(ptr, s->expected_payload.data(), len) == 0);
        }
    }
    if (data != nullptr) {
        crowdb_rpc_buffer_release(data);
    }
    s->got_response.store(true, std::memory_order_release);
}

// ── Simple ping loopback ──────────────────────────────────────────
// Server listens, client connects through transport, sends a ping
// request, receives a ping response. Verify request_id matches.
TEST(LoopbackTest, SimplePing)
{
    RpcServer server;
    ASSERT_TRUE(server.listen("127.0.0.1", 0));
    int port = server.listen_port();
    ASSERT_GT(port, 0);

    server.start();

    // Client side: connect through the transport.
    SocketTransport client_transport(1, 1);
    client_transport.start();

    auto conn = client_transport.connect("127.0.0.1", port);
    ASSERT_NE(conn, nullptr);

    RpcClient caller;
    caller.set_completion_pool_size(16);
    caller.attach(conn.get());

    LoopbackState state;

    // Build a ping request.
    BufferPool *pool   = client_transport.pool() != nullptr ? client_transport.pool() : server.pool();
    uint64_t    req_id = 42;
    Buffer     *ctrl   = build_ping_request(pool, req_id, 0);

    ASSERT_TRUE(caller.send(&client_transport, conn.get(), req_id, ctrl, nullptr,
                            static_cast<uint16_t>(crowdb::rpc::proto::FBMsgType_EConnectionPingRequest),
                            loopback_on_complete, &state));

    // Wait for the response.
    for (int i = 0; i < 300 && !state.got_response.load(std::memory_order_acquire); i++) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    EXPECT_TRUE(state.got_response.load(std::memory_order_acquire));
    EXPECT_EQ(state.recv_request_id, 42u);

    client_transport.stop();
    server.stop();
}

// ── Echo handler with 512-byte data ───────────────────────────────
// Register a custom echo handler that returns the request data as the
// response data. Client sends 512-byte data, verifies the response data
// matches.
TEST(LoopbackTest, EchoHandler512B)
{
    RpcServer server;
    ASSERT_TRUE(server.listen("127.0.0.1", 0));
    int port = server.listen_port();
    ASSERT_GT(port, 0);

    // Echo handler: msg_type=100, returns the request data as response data.
    constexpr uint16_t ECHO_MSG_TYPE = 100;
    server.register_handler(ECHO_MSG_TYPE, [](Frame *request, Connection *conn) -> OutFrame * {
        uint64_t req_id = request->request_id;

        // Allocate a response control buffer (echo back request_id).
        BufferPool *pool      = conn->pool();
        Buffer     *resp_ctrl = build_ping_response(pool, req_id, 0, 0);

        // Echo the request data back.
        Buffer *resp_data = nullptr;
        if (request->data_buf != nullptr && request->data_buf->len > 0) {
            resp_data = pool->alloc(request->data_buf->len);
            if (resp_data != nullptr) {
                std::memcpy(resp_data->data, request->data_buf->data, request->data_buf->len);
                resp_data->write(resp_data->data, request->data_buf->len);
            }
        }

        delete request;
        return build_out_frame(req_id, ECHO_MSG_TYPE, resp_ctrl, resp_data);
    });

    server.start();

    // Client side.
    SocketTransport client_transport(1, 1);
    client_transport.start();

    auto conn = client_transport.connect("127.0.0.1", port);
    ASSERT_NE(conn, nullptr);

    RpcClient caller;
    caller.set_completion_pool_size(16);
    caller.attach(conn.get());

    // Build a request with 512-byte data.
    BufferPool *pool   = client_transport.pool() != nullptr ? client_transport.pool() : server.pool();
    uint64_t    req_id = 100;
    Buffer     *ctrl   = build_ping_request(pool, req_id, 0);

    // 512-byte data payload.
    constexpr uint32_t DATA_SIZE = 512;
    Buffer            *data      = pool->alloc(DATA_SIZE);
    ASSERT_NE(data, nullptr);
    std::vector<uint8_t> payload(DATA_SIZE);
    for (uint32_t i = 0; i < DATA_SIZE; i++) {
        payload[i] = static_cast<uint8_t>(i % 256);
    }
    data->write(payload.data(), DATA_SIZE);

    LoopbackState state;
    state.expected_payload = payload;

    ASSERT_TRUE(
        caller.send(&client_transport, conn.get(), req_id, ctrl, data, ECHO_MSG_TYPE, loopback_on_complete, &state));

    // Wait for the response.
    for (int i = 0; i < 300 && !state.got_response.load(std::memory_order_acquire); i++) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    EXPECT_TRUE(state.got_response.load(std::memory_order_acquire));
    EXPECT_TRUE(state.data_matches) << "Response data does not match request data";

    client_transport.stop();
    server.stop();
}
