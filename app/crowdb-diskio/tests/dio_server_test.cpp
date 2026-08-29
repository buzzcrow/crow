// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// DiskioServer loopback tests: start an RPC server with DiskioServer
// handlers, connect a client, send write/read/fsync requests, verify
// responses. Uses BlockingEngine + BlockDisk for real I/O.
#include "crowdb-rpc/buffer.h"
#include "crowdb-rpc/c_api.h"
#include "crowdb-rpc/client/client.h"
#include "crowdb-rpc/framing.h"
#include "crowdb-rpc/server/message.h"
#include "crowdb-rpc/server/server.h"
#include "crowdb-rpc/transport/socket_transport.h"
#include "disk/block_disk.h"
#include "disk/types.h"
#include "engine/blocking/blocking_engine.h"
#include "rpc/dio_server.h"

#include <diskio_generated.h>
#include <fcntl.h>
#include <flatbuffers/flatbuffers.h>
#include <gtest/gtest.h>
#include <msg_type_generated.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <thread>

using crowdb::rpc::Buffer;
using crowdb::rpc::BufferPool;
using crowdb::rpc::Connection;
using crowdb::rpc::Frame;
using crowdb::rpc::OutFrame;
using crowdb::rpc::RpcClient;
using crowdb::rpc::RpcServer;
using crowdb::rpc::SocketTransport;
using crowdb::rpc::SystemBufferPool;

namespace dproto = crowdb::diskio::proto;
namespace rproto = crowdb::rpc::proto;

namespace
{

// Temp file helper.
std::string temp_path()
{
    const char *root = getenv("TMPDIR");
    if (root == nullptr) {
        root = "/tmp";
    }
    char tmpl[128];
    std::snprintf(tmpl, sizeof(tmpl), "%s/dx_XXXXXX", root);
    std::vector<char> buf(tmpl, tmpl + std::strlen(tmpl) + 1);
    int               fd = mkstemp(buf.data());
    if (fd >= 0) {
        close(fd);
    }
    return std::string(buf.data());
}

// Build a diskio write request control buffer.
Buffer *build_write_request(BufferPool *pool, uint64_t req_id, crowdb::diskio::DiskId disk_id, uint32_t zone_index,
                            uint64_t zone_offset, uint32_t size)
{
    flatbuffers::FlatBufferBuilder fbb(128);
    crowdb::rpc::proto::FBInt128     fb_disk_id(disk_id.high, disk_id.low);
    auto off = dproto::CreateFBDiskWriteRequest(fbb, req_id, 0, &fb_disk_id, zone_index, zone_offset, size);
    fbb.Finish(off);
    uint32_t sz  = fbb.GetSize();
    auto    *buf = pool->alloc(sz);
    if (buf == nullptr) {
        return nullptr;
    }
    std::memcpy(buf->data, fbb.GetBufferPointer(), sz);
    buf->write(buf->data, sz);
    return buf;
}

Buffer *build_read_request(BufferPool *pool, uint64_t req_id, crowdb::diskio::DiskId disk_id, uint32_t zone_index,
                           uint64_t zone_offset, uint32_t size)
{
    flatbuffers::FlatBufferBuilder fbb(128);
    crowdb::rpc::proto::FBInt128     fb_disk_id(disk_id.high, disk_id.low);
    auto off = dproto::CreateFBDiskReadRequest(fbb, req_id, 0, &fb_disk_id, zone_index, zone_offset, size, 0);
    fbb.Finish(off);
    uint32_t sz  = fbb.GetSize();
    auto    *buf = pool->alloc(sz);
    if (buf == nullptr) {
        return nullptr;
    }
    std::memcpy(buf->data, fbb.GetBufferPointer(), sz);
    buf->write(buf->data, sz);
    return buf;
}

Buffer *build_fsync_request(BufferPool *pool, uint64_t req_id, crowdb::diskio::DiskId disk_id)
{
    flatbuffers::FlatBufferBuilder fbb(128);
    crowdb::rpc::proto::FBInt128     fb_disk_id(disk_id.high, disk_id.low);
    auto                           off = dproto::CreateFBDiskFsyncRequest(fbb, req_id, 0, &fb_disk_id);
    fbb.Finish(off);
    uint32_t sz  = fbb.GetSize();
    auto    *buf = pool->alloc(sz);
    if (buf == nullptr) {
        return nullptr;
    }
    std::memcpy(buf->data, fbb.GetBufferPointer(), sz);
    buf->write(buf->data, sz);
    return buf;
}

// Response state for diskio tests.
struct DioState
{
    std::atomic<bool>    got_response{false};
    std::atomic<int16_t> ret_code{-1};
    uint64_t             recv_request_id{0};
    std::vector<uint8_t> recv_data;
};

extern "C" void dio_on_complete(uint64_t request_id, crowdb_rpc_buffer_t control, crowdb_rpc_buffer_t data,
                                crowdb_rpc_status status, void *user_data)
{
    auto *s = static_cast<DioState *>(user_data);
    if (status == CROWDB_RPC_OK) {
        s->recv_request_id = request_id;
        // Parse the response control to get ret_code.
        if (control != nullptr) {
            uint32_t    len  = crowdb_rpc_buffer_len(control);
            const auto *ptr  = crowdb_rpc_buffer_data(control);
            auto       *resp = ::flatbuffers::GetRoot<dproto::FBDiskWriteResponse>(ptr);
            if (resp != nullptr && len >= 4) {
                s->ret_code.store(static_cast<int16_t>(resp->ret_code()), std::memory_order_relaxed);
            }
        }
        // Copy response data.
        if (data != nullptr) {
            uint32_t    len = crowdb_rpc_buffer_len(data);
            const auto *ptr = crowdb_rpc_buffer_data(data);
            s->recv_data.assign(ptr, ptr + len);
            crowdb_rpc_buffer_release(data);
        }
    }
    s->got_response.store(true, std::memory_order_release);
}

bool wait_for(DioState &s, int timeout_ms = 300)
{
    for (int i = 0; i < timeout_ms / 10; i++) {
        if (s.got_response.load(std::memory_order_acquire)) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return s.got_response.load(std::memory_order_acquire);
}

} // namespace

// ── Write + Read round-trip via RPC ───────────────────────────────
TEST(DiskioServerTest, WriteAndReadRoundTrip)
{
    std::string path = temp_path();
    ASSERT_EQ(::truncate(path.c_str(), 1 << 16), 0);

    // Set up DiskSet + BlockingEngine + DiskioServer.
    auto                            engine = std::make_shared<crowdb::diskio::BlockingEngine>(2);
    std::vector<crowdb::diskio::Zone> zones;
    zones.push_back({0, 0, 1 << 24});
    auto disk =
        std::make_shared<crowdb::diskio::BlockDisk>(crowdb::diskio::DiskId{1, 1}, path, engine, std::move(zones), false);

    auto disk_set = std::make_shared<crowdb::diskio::DiskSet>();
    disk_set->add(disk);

    // Start the RPC server.
    RpcServer server;
    ASSERT_TRUE(server.listen("127.0.0.1", 0));
    int port = server.listen_port();
    ASSERT_GT(port, 0);

    auto *transport  = server.transport();
    auto  dio_server = std::make_unique<crowdb::diskio::DiskioServer>(disk_set, transport);
    dio_server->register_handlers(server);

    server.start();

    // Client side.
    SocketTransport client_transport(1, 1);
    client_transport.start();

    auto conn = client_transport.connect("127.0.0.1", port);
    ASSERT_NE(conn, nullptr);

    RpcClient caller;
    caller.set_completion_pool_size(16);
    caller.attach(conn.get());

    BufferPool *pool = client_transport.pool() != nullptr ? client_transport.pool() : server.pool();

    // Write 4096 bytes.
    constexpr uint32_t   DATA_SIZE = 4096;
    std::vector<uint8_t> payload(DATA_SIZE);
    for (uint32_t i = 0; i < DATA_SIZE; i++) {
        payload[i] = static_cast<uint8_t>(i % 256);
    }

    uint64_t write_req_id = 10;
    Buffer  *write_ctrl   = build_write_request(pool, write_req_id, {1, 1}, 0, 0, DATA_SIZE);
    Buffer  *write_data   = pool->alloc(DATA_SIZE);
    write_data->write(payload.data(), DATA_SIZE);

    DioState write_state;
    ASSERT_TRUE(caller.send(&client_transport, conn.get(), write_req_id, write_ctrl, write_data,
                            static_cast<uint16_t>(rproto::FBMsgType_EDiskWriteRequest), dio_on_complete, &write_state));

    ASSERT_TRUE(wait_for(write_state));
    EXPECT_EQ(write_state.recv_request_id, write_req_id);
    EXPECT_EQ(write_state.ret_code.load(), static_cast<int16_t>(dproto::FBDiskIoRetCode_Success));

    // Read 4096 bytes back.
    uint64_t read_req_id = 20;
    Buffer  *read_ctrl   = build_read_request(pool, read_req_id, {1, 1}, 0, 0, DATA_SIZE);

    DioState read_state;
    ASSERT_TRUE(caller.send(&client_transport, conn.get(), read_req_id, read_ctrl, nullptr,
                            static_cast<uint16_t>(rproto::FBMsgType_EDiskReadRequest), dio_on_complete, &read_state));

    ASSERT_TRUE(wait_for(read_state));
    EXPECT_EQ(read_state.recv_request_id, read_req_id);
    EXPECT_EQ(read_state.ret_code.load(), static_cast<int16_t>(dproto::FBDiskIoRetCode_Success));
    ASSERT_EQ(read_state.recv_data.size(), DATA_SIZE);
    EXPECT_EQ(std::memcmp(read_state.recv_data.data(), payload.data(), DATA_SIZE), 0);

    client_transport.stop();
    server.stop();
}

// ── Fsync via RPC ─────────────────────────────────────────────────
TEST(DiskioServerTest, FsyncRoundTrip)
{
    std::string path = temp_path();
    ASSERT_EQ(::truncate(path.c_str(), 4096), 0);

    auto                            engine = std::make_shared<crowdb::diskio::BlockingEngine>(1);
    std::vector<crowdb::diskio::Zone> zones;
    zones.push_back({0, 0, 1 << 24});
    auto disk =
        std::make_shared<crowdb::diskio::BlockDisk>(crowdb::diskio::DiskId{2, 2}, path, engine, std::move(zones), false);

    auto disk_set = std::make_shared<crowdb::diskio::DiskSet>();
    disk_set->add(disk);

    RpcServer server;
    ASSERT_TRUE(server.listen("127.0.0.1", 0));
    int port = server.listen_port();
    ASSERT_GT(port, 0);

    auto *transport  = server.transport();
    auto  dio_server = std::make_unique<crowdb::diskio::DiskioServer>(disk_set, transport);
    dio_server->register_handlers(server);

    server.start();

    SocketTransport client_transport(1, 1);
    client_transport.start();

    auto conn = client_transport.connect("127.0.0.1", port);
    ASSERT_NE(conn, nullptr);

    RpcClient caller;
    caller.set_completion_pool_size(16);
    caller.attach(conn.get());

    BufferPool *pool = client_transport.pool() != nullptr ? client_transport.pool() : server.pool();

    uint64_t req_id = 30;
    Buffer  *ctrl   = build_fsync_request(pool, req_id, {2, 2});

    DioState state;
    ASSERT_TRUE(caller.send(&client_transport, conn.get(), req_id, ctrl, nullptr,
                            static_cast<uint16_t>(rproto::FBMsgType_EDiskFsyncRequest), dio_on_complete, &state));

    ASSERT_TRUE(wait_for(state));
    EXPECT_EQ(state.recv_request_id, req_id);
    EXPECT_EQ(state.ret_code.load(), static_cast<int16_t>(dproto::FBDiskIoRetCode_Success));

    client_transport.stop();
    server.stop();
}

// ── Disk not found error ──────────────────────────────────────────
TEST(DiskioServerTest, DiskNotExist)
{
    auto disk_set = std::make_shared<crowdb::diskio::DiskSet>();

    RpcServer server;
    ASSERT_TRUE(server.listen("127.0.0.1", 0));
    int port = server.listen_port();
    ASSERT_GT(port, 0);

    auto *transport  = server.transport();
    auto  dio_server = std::make_unique<crowdb::diskio::DiskioServer>(disk_set, transport);
    dio_server->register_handlers(server);

    server.start();

    SocketTransport client_transport(1, 1);
    client_transport.start();

    auto conn = client_transport.connect("127.0.0.1", port);
    ASSERT_NE(conn, nullptr);

    RpcClient caller;
    caller.set_completion_pool_size(16);
    caller.attach(conn.get());

    BufferPool *pool = client_transport.pool() != nullptr ? client_transport.pool() : server.pool();

    // Write to a non-existent disk.
    uint64_t req_id = 40;
    Buffer  *ctrl   = build_write_request(pool, req_id, {99, 99}, 0, 0, 4096);
    Buffer  *data   = pool->alloc(4096);
    std::memset(data->data, 0xAB, 4096);
    data->write(data->data, 4096);

    DioState state;
    ASSERT_TRUE(caller.send(&client_transport, conn.get(), req_id, ctrl, data,
                            static_cast<uint16_t>(rproto::FBMsgType_EDiskWriteRequest), dio_on_complete, &state));

    ASSERT_TRUE(wait_for(state));
    EXPECT_EQ(state.ret_code.load(), static_cast<int16_t>(dproto::FBDiskIoRetCode_DiskNotExist));

    client_transport.stop();
    server.stop();
}
