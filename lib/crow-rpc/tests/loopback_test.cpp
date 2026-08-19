// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#include "common_msg_generated.h"
#include "crow-rpc/buffer.h"
#include "crow-rpc/client/client.h"
#include "crow-rpc/framing.h"
#include "crow-rpc/server/handler.h"
#include "crow-rpc/server/message.h"
#include "crow-rpc/server/server.h"
#include "crow-rpc/transport/socket_transport.h"
#include "msg_type_generated.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <thread>

using crow::rpc::Buffer;
using crow::rpc::BufferPool;
using crow::rpc::build_out_frame;
using crow::rpc::build_ping_request;
using crow::rpc::Connection;
using crow::rpc::extract_request_id;
using crow::rpc::Frame;
using crow::rpc::OutFrame;
using crow::rpc::RpcClient;
using crow::rpc::RpcServer;
using crow::rpc::SocketTransport;
using crow::rpc::SystemBufferPool;

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

    RpcClient         caller;
    std::atomic<bool> got_response{false};
    uint64_t          recv_request_id = 0;

    caller.attach(conn.get());

    // Build a ping request.
    BufferPool *pool   = client_transport.pool() != nullptr ? client_transport.pool() : server.pool();
    uint64_t    req_id = 42;
    Buffer     *ctrl   = build_ping_request(pool, req_id, 0);

    caller.call(&client_transport, conn.get(), req_id, ctrl, nullptr,
                static_cast<uint16_t>(crow::rpc::proto::FBMsgType_EConnectionPingRequest),
                [&](Frame *response, crow::rpc::RpcError err) {
                    if (err == crow::rpc::RpcError::Ok && response != nullptr) {
                        auto *resp = flatbuffers::GetRoot<crow::rpc::proto::ConnectionPingResponse>(response->control);
                        if (resp != nullptr) {
                            recv_request_id = resp->id();
                        }
                    }
                    got_response.store(true, std::memory_order_release);
                    delete response;
                });

    // Wait for the response.
    for (int i = 0; i < 300 && !got_response.load(std::memory_order_acquire); i++) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    EXPECT_TRUE(got_response.load(std::memory_order_acquire));
    EXPECT_EQ(recv_request_id, 42u);

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
        uint64_t req_id = extract_request_id(request->control, request->control_len);

        // Allocate a response control buffer (echo back request_id).
        BufferPool *pool      = conn->pool();
        Buffer     *resp_ctrl = build_ping_response(pool, req_id, 0);

        // Echo the request data back.
        Buffer *resp_data = nullptr;
        if (request->data != nullptr && request->data_len > 0) {
            resp_data = pool->alloc(request->data_len);
            if (resp_data != nullptr) {
                std::memcpy(resp_data->data, request->data, request->data_len);
                resp_data->write(resp_data->data, request->data_len);
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

    RpcClient         caller;
    std::atomic<bool> got_response{false};
    bool              data_matches = false;

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

    caller.call(&client_transport, conn.get(), req_id, ctrl, data, ECHO_MSG_TYPE,
                [&](Frame *response, crow::rpc::RpcError err) {
                    if (err == crow::rpc::RpcError::Ok && response != nullptr) {
                        if (response->data != nullptr && response->data_len == DATA_SIZE) {
                            data_matches = (std::memcmp(response->data, payload.data(), DATA_SIZE) == 0);
                        }
                    }
                    got_response.store(true, std::memory_order_release);
                    delete response;
                });

    // Wait for the response.
    for (int i = 0; i < 300 && !got_response.load(std::memory_order_acquire); i++) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    EXPECT_TRUE(got_response.load(std::memory_order_acquire));
    EXPECT_TRUE(data_matches) << "Response data does not match request data";

    client_transport.stop();
    server.stop();
}
