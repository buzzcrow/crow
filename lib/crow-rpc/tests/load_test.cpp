// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#include "common_msg_generated.h"
#include "crow-rpc/buffer.h"
#include "crow-rpc/client/caller.h"
#include "crow-rpc/framing.h"
#include "crow-rpc/server/handler.h"
#include "crow-rpc/server/message.h"
#include "crow-rpc/server/server.h"
#include "crow-rpc/transport/socket_transport.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <thread>
#include <vector>

using crow::rpc::Buffer;
using crow::rpc::BufferPool;
using crow::rpc::build_out_frame;
using crow::rpc::build_ping_request;
using crow::rpc::Connection;
using crow::rpc::extract_request_id;
using crow::rpc::Frame;
using crow::rpc::OutFrame;
using crow::rpc::RemoteCaller;
using crow::rpc::RpcServer;
using crow::rpc::SocketTransport;
using crow::rpc::SystemBufferPool;

// ── Multi-threaded load test ──────────────────────────────────────
// T client threads, each creates C connections and sends R requests
// with 512-byte data per request. Server echo handler responds.
// Verify all requests get responses with matching data.
//
// Config: T=4, C=2, R=100, 512B data → 800 total requests.
TEST(LoadTest, MultiThreadEcho)
{
    constexpr int      T             = 4;   // client threads
    constexpr int      C             = 2;   // connections per thread
    constexpr int      R             = 100; // requests per connection
    constexpr uint32_t DATA_SIZE     = 512;
    constexpr uint16_t ECHO_MSG_TYPE = 100;

    RpcServer server;
    ASSERT_TRUE(server.listen("127.0.0.1", 0));
    int port = server.listen_port();
    ASSERT_GT(port, 0);

    // Echo handler: returns the request data as response data.
    server.register_handler(ECHO_MSG_TYPE, [](Frame *request, Connection *conn) -> OutFrame * {
        uint64_t req_id = extract_request_id(request->control, request->control_len);

        BufferPool *pool      = conn->pool();
        Buffer     *resp_ctrl = build_ping_response(pool, req_id, 0);

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

    std::atomic<int> success_count{0};
    std::atomic<int> failure_count{0};

    auto worker_fn = [&]() {
        SocketTransport transport(1);
        transport.start();

        std::vector<std::shared_ptr<Connection>>   conns;
        std::vector<std::unique_ptr<RemoteCaller>> callers;
        for (int c = 0; c < C; c++) {
            auto conn = transport.connect("127.0.0.1", port);
            if (conn == nullptr) {
                failure_count.fetch_add(R, std::memory_order_relaxed);
                continue;
            }
            auto caller = std::make_unique<RemoteCaller>();
            caller->attach(conn.get());
            conns.push_back(conn);
            callers.push_back(std::move(caller));
        }

        for (int c = 0; c < static_cast<int>(conns.size()); c++) {
            for (int r = 0; r < R; r++) {
                int   cidx   = c;
                auto &conn   = conns[cidx];
                auto &caller = callers[cidx];

                std::atomic<bool> got_response{false};
                bool              data_matches = false;

                BufferPool *pool   = transport.pool();
                uint64_t    req_id = caller->next_request_id();
                Buffer     *ctrl   = build_ping_request(pool, req_id, 0);
                Buffer     *data   = pool->alloc(DATA_SIZE);
                if (data == nullptr) {
                    failure_count.fetch_add(1, std::memory_order_relaxed);
                    continue;
                }

                std::vector<uint8_t> payload(DATA_SIZE);
                for (uint32_t i = 0; i < DATA_SIZE; i++) {
                    payload[i] = static_cast<uint8_t>((i + r) % 256);
                }
                data->write(payload.data(), DATA_SIZE);

                caller->call(&transport, conn.get(), req_id, ctrl, data, ECHO_MSG_TYPE,
                             [&](Frame *response, crow::rpc::RpcError err) {
                                 if (err == crow::rpc::RpcError::Ok && response != nullptr) {
                                     if (response->data != nullptr && response->data_len == DATA_SIZE) {
                                         data_matches = (std::memcmp(response->data, payload.data(), DATA_SIZE) == 0);
                                     }
                                 }
                                 got_response.store(true, std::memory_order_release);
                                 delete response;
                             });

                // Wait for the response (up to 10 seconds).
                for (int i = 0; i < 1000 && !got_response.load(std::memory_order_acquire); i++) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(10));
                }

                if (got_response.load(std::memory_order_acquire) && data_matches) {
                    success_count.fetch_add(1, std::memory_order_relaxed);
                }
                else {
                    failure_count.fetch_add(1, std::memory_order_relaxed);
                }
            }
        }

        transport.stop();
    };

    std::vector<std::thread> threads;
    for (int t = 0; t < T; t++) {
        threads.emplace_back(worker_fn);
    }
    for (auto &t : threads) {
        t.join();
    }

    server.stop();

    int total = T * C * R;
    EXPECT_EQ(success_count.load(), total);
    EXPECT_EQ(failure_count.load(), 0) << "failures: " << failure_count.load() << " / " << total;
}
