// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#include "common_msg_generated.h"
#include "crow-rpc/buffer.h"
#include "crow-rpc/c_api.h"
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
#include <memory>
#include <thread>
#include <vector>

using crow::rpc::Buffer;
using crow::rpc::BufferPool;
using crow::rpc::build_out_frame;
using crow::rpc::build_ping_request;
using crow::rpc::Connection;
using crow::rpc::Frame;
using crow::rpc::OutFrame;
using crow::rpc::RpcClient;
using crow::rpc::RpcServer;
using crow::rpc::SocketTransport;

// ── Shared callback state + C ABI callback for load tests ──────────

struct PendingReq
{
    std::atomic<bool>    got_response{false};
    bool                 data_matches = false;
    std::vector<uint8_t> payload;
};

extern "C" void load_on_complete(uint64_t /*request_id*/, crow_rpc_buffer_t /*control*/, crow_rpc_buffer_t data,
                                 crow_rpc_status status, void *user_data)
{
    auto *pr = static_cast<PendingReq *>(user_data);
    if (status == CROW_RPC_OK && data != nullptr) {
        uint32_t    len = crow_rpc_buffer_len(data);
        const auto *ptr = crow_rpc_buffer_data(data);
        if (len == pr->payload.size()) {
            pr->data_matches = (std::memcmp(ptr, pr->payload.data(), len) == 0);
        }
    }
    if (data != nullptr) {
        crow_rpc_buffer_release(data);
    }
    pr->got_response.store(true, std::memory_order_release);
}

// ── Multi-threaded pipelined echo load test ───────────────────────
// T client threads share C connections (created once, not per thread).
// Each thread pipelines R requests — fires all R without waiting for
// responses, then collects all R responses. Multiple threads submit
// to the same connection concurrently, giving the transport a chance
// to aggregate frames into batched writev calls.
//
// Config: T=4, C=2, R=100, 512B data → 400 total requests.
TEST(LoadTest, MultiThreadEcho)
{
    constexpr int      T             = 4;   // client threads (loaders)
    constexpr int      C             = 2;   // shared connections
    constexpr int      R             = 100; // requests per thread (pipelined)
    constexpr uint32_t DATA_SIZE     = 512;
    constexpr uint16_t ECHO_MSG_TYPE = 100;

    RpcServer server;
    ASSERT_TRUE(server.listen("127.0.0.1", 0));
    int port = server.listen_port();
    ASSERT_GT(port, 0);

    // Echo handler: returns the request data as response data.
    server.register_handler(ECHO_MSG_TYPE, [](Frame *request, Connection *conn) -> OutFrame * {
        uint64_t req_id = request->request_id;

        BufferPool *pool      = conn->pool();
        Buffer     *resp_ctrl = build_ping_response(pool, req_id, 0);

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

    // Shared transport + C connections, each with one RpcClient.
    // All T threads submit to these shared connections.
    SocketTransport transport(1, 1);
    transport.start();

    std::vector<std::shared_ptr<Connection>> conns;
    std::vector<std::unique_ptr<RpcClient>>  callers;
    for (int c = 0; c < C; c++) {
        auto conn = transport.connect("127.0.0.1", port);
        ASSERT_NE(conn, nullptr);
        auto caller = std::make_unique<RpcClient>();
        caller->set_completion_pool_size(T * R);
        caller->attach(conn.get());
        conns.push_back(conn);
        callers.push_back(std::move(caller));
    }

    std::atomic<int> success_count{0};
    std::atomic<int> failure_count{0};

    auto worker_fn = [&](int tid) {
        // Phase 1: fire all R requests (pipelined, no wait between sends).
        // Round-robin across shared connections so multiple threads hit
        // the same connection concurrently.
        std::vector<std::shared_ptr<PendingReq>> reqs;
        reqs.reserve(R);

        for (int r = 0; r < R; r++) {
            int   cidx   = (tid + r) % C;
            auto &conn   = conns[cidx];
            auto &caller = callers[cidx];

            auto pr = std::make_shared<PendingReq>();
            pr->payload.resize(DATA_SIZE);
            for (uint32_t i = 0; i < DATA_SIZE; i++) {
                pr->payload[i] = static_cast<uint8_t>((i + r * 7 + tid * 13) % 256);
            }

            uint64_t    req_id = caller->next_request_id();
            BufferPool *pool   = transport.pool();
            Buffer     *ctrl   = build_ping_request(pool, req_id, 0);
            Buffer     *data   = pool->alloc(DATA_SIZE);

            if (ctrl == nullptr || data == nullptr) {
                failure_count.fetch_add(1, std::memory_order_relaxed);
                continue;
            }

            data->write(pr->payload.data(), DATA_SIZE);

            caller->send(&transport, conn.get(), req_id, ctrl, data, ECHO_MSG_TYPE, load_on_complete, pr.get());

            reqs.push_back(std::move(pr));
        }

        // Phase 2: poll all responses until all done or 30s deadline.
        auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
        while (std::chrono::steady_clock::now() < deadline) {
            bool all_done = true;
            for (auto &pr : reqs) {
                if (!pr->got_response.load(std::memory_order_acquire)) {
                    all_done = false;
                    break;
                }
            }
            if (all_done) {
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }

        // Tally results.
        for (auto &pr : reqs) {
            if (pr->got_response.load(std::memory_order_acquire) && pr->data_matches) {
                success_count.fetch_add(1, std::memory_order_relaxed);
            }
            else {
                failure_count.fetch_add(1, std::memory_order_relaxed);
            }
        }
    };

    std::vector<std::thread> threads;
    threads.reserve(T);
    for (int t = 0; t < T; t++) {
        threads.emplace_back(worker_fn, t);
    }
    for (auto &t : threads) {
        t.join();
    }

    transport.stop();
    server.stop();

    int total = T * R;
    EXPECT_EQ(success_count.load(), total);
    EXPECT_EQ(failure_count.load(), 0) << "failures: " << failure_count.load() << " / " << total;
}

// ── Multi-worker oneshot echo load test ───────────────────────────
// Same as MultiThreadEcho but with 1 engine × 2 workers (EPOLLONESHOT).
// Exercises the oneshot re-arm path and concurrent worker access to
// shared connections on the same engine.
TEST(LoadTest, MultiWorkerOneshotEcho)
{
    constexpr int      T             = 4;
    constexpr int      C             = 2;
    constexpr int      R             = 100;
    constexpr uint32_t DATA_SIZE     = 512;
    constexpr uint16_t ECHO_MSG_TYPE = 100;

    // Server with 1 engine × 2 workers → EPOLLONESHOT mode.
    RpcServer server(nullptr, 1, 2);
    ASSERT_TRUE(server.listen("127.0.0.1", 0));
    int port = server.listen_port();
    ASSERT_GT(port, 0);

    server.register_handler(ECHO_MSG_TYPE, [](Frame *request, Connection *conn) -> OutFrame * {
        uint64_t req_id = request->request_id;

        BufferPool *pool      = conn->pool();
        Buffer     *resp_ctrl = build_ping_response(pool, req_id, 0);

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

    // Client transport: 1 engine × 1 worker (no oneshot needed on client).
    SocketTransport transport(1, 1);
    transport.start();

    std::vector<std::shared_ptr<Connection>> conns;
    std::vector<std::unique_ptr<RpcClient>>  callers;
    for (int c = 0; c < C; c++) {
        auto conn = transport.connect("127.0.0.1", port);
        ASSERT_NE(conn, nullptr);
        auto caller = std::make_unique<RpcClient>();
        caller->set_completion_pool_size(T * R);
        caller->attach(conn.get());
        conns.push_back(conn);
        callers.push_back(std::move(caller));
    }

    std::atomic<int> success_count{0};
    std::atomic<int> failure_count{0};

    auto worker_fn = [&](int tid) {
        std::vector<std::shared_ptr<PendingReq>> reqs;
        reqs.reserve(R);

        for (int r = 0; r < R; r++) {
            int   cidx   = (tid + r) % C;
            auto &conn   = conns[cidx];
            auto &caller = callers[cidx];

            auto pr = std::make_shared<PendingReq>();
            pr->payload.resize(DATA_SIZE);
            for (uint32_t i = 0; i < DATA_SIZE; i++) {
                pr->payload[i] = static_cast<uint8_t>((i + r * 7 + tid * 13) % 256);
            }

            uint64_t    req_id = caller->next_request_id();
            BufferPool *pool   = transport.pool();
            Buffer     *ctrl   = build_ping_request(pool, req_id, 0);
            Buffer     *data   = pool->alloc(DATA_SIZE);

            if (ctrl == nullptr || data == nullptr) {
                failure_count.fetch_add(1, std::memory_order_relaxed);
                continue;
            }

            data->write(pr->payload.data(), DATA_SIZE);

            caller->send(&transport, conn.get(), req_id, ctrl, data, ECHO_MSG_TYPE, load_on_complete, pr.get());

            reqs.push_back(std::move(pr));
        }

        auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
        while (std::chrono::steady_clock::now() < deadline) {
            bool all_done = true;
            for (auto &pr : reqs) {
                if (!pr->got_response.load(std::memory_order_acquire)) {
                    all_done = false;
                    break;
                }
            }
            if (all_done) {
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }

        for (auto &pr : reqs) {
            if (pr->got_response.load(std::memory_order_acquire) && pr->data_matches) {
                success_count.fetch_add(1, std::memory_order_relaxed);
            }
            else {
                failure_count.fetch_add(1, std::memory_order_relaxed);
            }
        }
    };

    std::vector<std::thread> threads;
    threads.reserve(T);
    for (int t = 0; t < T; t++) {
        threads.emplace_back(worker_fn, t);
    }
    for (auto &t : threads) {
        t.join();
    }

    transport.stop();
    server.stop();

    int total = T * R;
    EXPECT_EQ(success_count.load(), total);
    EXPECT_EQ(failure_count.load(), 0) << "failures: " << failure_count.load() << " / " << total;
}

// ── Shared-transport oneshot echo load test ───────────────────────
// Mimics the bench setup: server and client share the SAME transport
// (1 engine × 2 workers, EPOLLONESHOT). Client connections are created
// via transport.connect() on the server's own transport.
TEST(LoadTest, SharedTransportOneshotEcho)
{
    constexpr int      T             = 4;
    constexpr int      C             = 2;
    constexpr int      R             = 100;
    constexpr uint32_t DATA_SIZE     = 512;
    constexpr uint16_t ECHO_MSG_TYPE = 100;

    RpcServer server(nullptr, 1, 2);
    ASSERT_TRUE(server.listen("127.0.0.1", 0));
    int port = server.listen_port();
    ASSERT_GT(port, 0);

    server.register_handler(ECHO_MSG_TYPE, [](Frame *request, Connection *conn) -> OutFrame * {
        uint64_t    req_id    = request->request_id;
        BufferPool *pool      = conn->pool();
        Buffer     *resp_ctrl = build_ping_response(pool, req_id, 0);
        Buffer     *resp_data = nullptr;
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

    // Client connections on the SERVER's transport (shared).
    // This is the key difference from MultiWorkerOneshotEcho.
    auto &shared_transport = *server.transport();
    auto  pool             = shared_transport.pool();

    std::vector<std::shared_ptr<Connection>> conns;
    std::vector<std::unique_ptr<RpcClient>>  callers;
    for (int c = 0; c < C; c++) {
        auto conn = shared_transport.connect("127.0.0.1", port);
        ASSERT_NE(conn, nullptr);
        auto caller = std::make_unique<RpcClient>();
        caller->set_completion_pool_size(T * R);
        caller->attach(conn.get());
        conns.push_back(conn);
        callers.push_back(std::move(caller));
    }

    std::atomic<int> success_count{0};
    std::atomic<int> failure_count{0};

    auto worker_fn = [&](int tid) {
        std::vector<std::shared_ptr<PendingReq>> reqs;
        reqs.reserve(R);
        for (int r = 0; r < R; r++) {
            int   cidx   = (tid + r) % C;
            auto &conn   = conns[cidx];
            auto &caller = callers[cidx];
            auto  pr     = std::make_shared<PendingReq>();
            pr->payload.resize(DATA_SIZE);
            for (uint32_t i = 0; i < DATA_SIZE; i++) {
                pr->payload[i] = static_cast<uint8_t>((i + r * 7 + tid * 13) % 256);
            }
            uint64_t req_id = caller->next_request_id();
            Buffer  *ctrl   = build_ping_request(pool, req_id, 0);
            Buffer  *data   = pool->alloc(DATA_SIZE);
            if (ctrl == nullptr || data == nullptr) {
                failure_count.fetch_add(1, std::memory_order_relaxed);
                continue;
            }
            data->write(pr->payload.data(), DATA_SIZE);
            caller->send(&shared_transport, conn.get(), req_id, ctrl, data, ECHO_MSG_TYPE, load_on_complete, pr.get());
            reqs.push_back(std::move(pr));
        }
        auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
        while (std::chrono::steady_clock::now() < deadline) {
            bool all_done = true;
            for (auto &pr : reqs) {
                if (!pr->got_response.load(std::memory_order_acquire)) {
                    all_done = false;
                    break;
                }
            }
            if (all_done)
                break;
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        for (auto &pr : reqs) {
            if (pr->got_response.load(std::memory_order_acquire) && pr->data_matches) {
                success_count.fetch_add(1, std::memory_order_relaxed);
            }
            else {
                failure_count.fetch_add(1, std::memory_order_relaxed);
            }
        }
    };

    std::vector<std::thread> threads;
    threads.reserve(T);
    for (int t = 0; t < T; t++) {
        threads.emplace_back(worker_fn, t);
    }
    for (auto &t : threads) {
        t.join();
    }

    server.stop();
    int total = T * R;
    EXPECT_EQ(success_count.load(), total);
    EXPECT_EQ(failure_count.load(), 0) << "failures: " << failure_count.load() << " / " << total;
}
