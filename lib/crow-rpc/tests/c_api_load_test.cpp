// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// Reproduces the bench's FFI access pattern via the C API directly:
// shared transport, 1 engine x 2 workers (EPOLLONESHOT), many client
// threads calling crow_rpc_client_send concurrently on shared
// connections. This test runs under ASAN to catch heap corruption.

#include "crow-rpc/c_api.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <thread>
#include <vector>

// Simple on-complete callback: signals a promise (atomic flag).
struct PendingReq
{
    std::atomic<bool> got_response{false};
    bool              ok = false;
};

extern "C" void on_complete_cb(uint64_t /*request_id*/, crow_rpc_buffer_t /*control*/, crow_rpc_buffer_t /*data*/,
                               int status, void *user_data)
{
    auto *pr = static_cast<PendingReq *>(user_data);
    pr->ok   = (status == CROW_RPC_OK);
    pr->got_response.store(true, std::memory_order_release);
    // Reclaim the PendingReq via the user_data pointer — the caller
    // allocates it with new and deletes it after checking the flag.
}

TEST(CApiLoadTest, MultiWorkerOneshotSharedTransport)
{
    constexpr int      T             = 4;
    constexpr int      C             = 2;
    constexpr int      R             = 100;
    constexpr uint32_t DATA_SIZE     = 64;
    constexpr uint16_t ECHO_MSG_TYPE = 100;

    crow_rpc_pool_t   pool   = crow_rpc_pool_create(T * R * 4);
    crow_rpc_server_t server = crow_rpc_server_create_with_engines(pool, 1, 2);
    ASSERT_NE(server, nullptr);
    ASSERT_EQ(crow_rpc_server_listen(server, "127.0.0.1", 0), CROW_RPC_OK);
    int port = crow_rpc_server_port(server);
    ASSERT_GT(port, 0);

    crow_rpc_server_register_echo_handler(server, ECHO_MSG_TYPE);
    crow_rpc_server_start(server);

    crow_rpc_client_t client = crow_rpc_client_create();
    ASSERT_NE(client, nullptr);
    // Size the slab completion pool for send() (must be >= max
    // in-flight = T * R per thread, but all threads share one client).
    crow_rpc_client_set_completion_pool_size(client, T * R * 4);

    std::vector<crow_rpc_conn_t> conns;
    for (int c = 0; c < C; c++) {
        crow_rpc_conn_t conn = crow_rpc_connect(server, "127.0.0.1", port);
        ASSERT_NE(conn, nullptr);
        crow_rpc_client_attach(client, conn);
        conns.push_back(conn);
    }

    std::atomic<int>      success_count{0};
    std::atomic<int>      failure_count{0};
    std::atomic<int>      timeout_count{0};
    std::atomic<int>      error_count{0};
    std::atomic<uint64_t> req_id_counter{1};

    auto worker_fn = [&](int /*tid*/) {
        std::vector<PendingReq *> reqs;
        reqs.reserve(R);
        for (int r = 0; r < R; r++) {
            int   cidx = r % C;
            auto *pr   = new PendingReq;

            // Build a minimal control buffer (flatbuffer ConnectionPingRequest).
            // For the echo handler, we just need a valid buffer with the
            // request_id at VT_ID=4. Use a simple 8-byte placeholder.
            // The echo handler extracts request_id via extract_request_id.
            // We'll use build_ping_request format: a flatbuffer with id field.
            // For simplicity, allocate a small buffer and write a minimal
            // flatbuffer. Actually, the C API doesn't expose build_ping_request,
            // so we'll just allocate a buffer and write raw bytes. The echo
            // handler will extract_request_id from it (may return 0, which
            // is fine — the response will still come back).
            // Build a valid ConnectionPingRequest flatbuffer control message.
            // The flatbuffer is 24 bytes: a fixed vtable + the id field at
            // offset 16 (little-endian u64). We copy the template and patch
            // the id. This mirrors what the bench does (Rust builds the
            // flatbuffer, allocs a pool buffer, writes to it).
            static const uint8_t PING_TEMPLATE[24] = {0x0c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x06, 0x00,
                                                      0x0c, 0x00, 0x04, 0x00, 0x06, 0x00, 0x00, 0x00,
                                                      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
            uint64_t             req_id            = req_id_counter.fetch_add(1, std::memory_order_relaxed);

            crow_rpc_buffer_t ctrl = crow_rpc_buffer_alloc(pool, 24);
            if (ctrl == nullptr) {
                failure_count.fetch_add(1, std::memory_order_relaxed);
                delete pr;
                continue;
            }
            uint8_t ctrl_buf[24];
            std::memcpy(ctrl_buf, PING_TEMPLATE, 24);
            std::memcpy(ctrl_buf + 16, &req_id, 8); // patch id field
            crow_rpc_buffer_write(ctrl, ctrl_buf, 24);

            crow_rpc_buffer_t data = nullptr;
            if (DATA_SIZE > 0) {
                data = crow_rpc_buffer_alloc(pool, DATA_SIZE);
                if (data == nullptr) {
                    crow_rpc_buffer_release(ctrl);
                    failure_count.fetch_add(1, std::memory_order_relaxed);
                    delete pr;
                    continue;
                }
                uint8_t payload[DATA_SIZE];
                for (uint32_t i = 0; i < DATA_SIZE; i++) {
                    payload[i] = static_cast<uint8_t>((i + r * 7) % 256);
                }
                crow_rpc_buffer_write(data, payload, DATA_SIZE);
            }

            crow_rpc_status status = crow_rpc_client_send(client, server, conns[cidx], req_id, ctrl, data,
                                                          ECHO_MSG_TYPE, on_complete_cb, pr);

            if (status != CROW_RPC_OK) {
                failure_count.fetch_add(1, std::memory_order_relaxed);
                delete pr;
                continue;
            }
            reqs.push_back(pr);
        }

        // Wait for all responses (30s deadline).
        auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
        while (std::chrono::steady_clock::now() < deadline) {
            bool all_done = true;
            for (auto *pr : reqs) {
                if (!pr->got_response.load(std::memory_order_acquire)) {
                    all_done = false;
                    break;
                }
            }
            if (all_done)
                break;
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }

        for (auto *pr : reqs) {
            if (pr->got_response.load(std::memory_order_acquire) && pr->ok) {
                success_count.fetch_add(1, std::memory_order_relaxed);
            }
            else if (!pr->got_response.load(std::memory_order_acquire)) {
                timeout_count.fetch_add(1, std::memory_order_relaxed);
            }
            else {
                error_count.fetch_add(1, std::memory_order_relaxed);
            }
            delete pr;
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

    crow_rpc_client_destroy(client);
    // Connections are owned by the transport; the crow_rpc_conn_s
    // wrappers are intentionally leaked (no crow_rpc_conn_destroy API).
    crow_rpc_server_stop(server);
    crow_rpc_server_destroy(server);
    crow_rpc_pool_destroy(pool);

    int total = T * R;
    EXPECT_EQ(success_count.load(), total);
    EXPECT_EQ(failure_count.load(), 0) << "failures: " << failure_count.load() << " / " << total
                                       << " (timeouts: " << timeout_count.load() << ", errors: " << error_count.load()
                                       << ")";
}
