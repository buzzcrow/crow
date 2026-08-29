// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// Startup integration test: launch the crowdb-diskio binary as a
// subprocess, connect a client, write + read + fsync, then shut down.
#include "crowdb-rpc/buffer.h"
#include "crowdb-rpc/c_api.h"
#include "crowdb-rpc/client/client.h"
#include "crowdb-rpc/framing.h"
#include "crowdb-rpc/transport/socket_transport.h"

#include <diskio_generated.h>
#include <fcntl.h>
#include <flatbuffers/flatbuffers.h>
#include <gtest/gtest.h>
#include <msg_type_generated.h>
#include <signal.h>
#include <sys/wait.h>
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
using crowdb::rpc::RpcClient;
using crowdb::rpc::SocketTransport;

namespace dproto = crowdb::diskio::proto;
namespace rproto = crowdb::rpc::proto;

namespace
{

// Find the crowdb-diskio binary relative to the test executable.
std::string find_binary()
{
    // The test executable is at app/crowdb-diskio/build/crow_diskio_tests.
    // The binary is at app/crowdb-diskio/build/crowdb-diskio.
    char    buf[4096];
    ssize_t n = readlink("/proc/self/exe", buf, sizeof(buf) - 1);
    if (n <= 0) {
        return "";
    }
    buf[n] = '\0';
    std::string path(buf);
    // Replace "crow_diskio_tests" with "crowdb-diskio".
    size_t pos = path.rfind("crow_diskio_tests");
    if (pos == std::string::npos) {
        return "";
    }
    path.replace(pos, std::string("crow_diskio_tests").length(), "crowdb-diskio");
    return path;
}

// Create a temp file of the given size.
std::string temp_file(int64_t size)
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
        if (size > 0) {
            ftruncate(fd, size);
        }
        close(fd);
    }
    return std::string(buf.data());
}

// Response state.
struct StartupState
{
    std::atomic<bool>    got_response{false};
    std::atomic<int16_t> ret_code{-1};
    uint64_t             recv_request_id{0};
    std::vector<uint8_t> recv_data;
};

extern "C" void startup_on_complete(uint64_t request_id, crowdb_rpc_buffer_t control, crowdb_rpc_buffer_t data,
                                    crowdb_rpc_status status, void *user_data)
{
    auto *s = static_cast<StartupState *>(user_data);
    if (status == CROWDB_RPC_OK) {
        s->recv_request_id = request_id;
        if (control != nullptr) {
            uint32_t    len  = crowdb_rpc_buffer_len(control);
            const auto *ptr  = crowdb_rpc_buffer_data(control);
            auto       *resp = ::flatbuffers::GetRoot<dproto::FBDiskWriteResponse>(ptr);
            if (resp != nullptr && len >= 4) {
                s->ret_code.store(static_cast<int16_t>(resp->ret_code()), std::memory_order_relaxed);
            }
            crowdb_rpc_buffer_release(control);
        }
        if (data != nullptr) {
            uint32_t    len = crowdb_rpc_buffer_len(data);
            const auto *ptr = crowdb_rpc_buffer_data(data);
            s->recv_data.assign(ptr, ptr + len);
            crowdb_rpc_buffer_release(data);
        }
    }
    s->got_response.store(true, std::memory_order_release);
}

bool wait_for(StartupState &s, int timeout_ms = 500)
{
    for (int i = 0; i < timeout_ms / 10; i++) {
        if (s.got_response.load(std::memory_order_acquire)) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return s.got_response.load(std::memory_order_acquire);
}

// Read a line from a pipe (up to '\n' or EOF).
std::string read_line(int fd, int timeout_ms = 5000)
{
    std::string result;
    char        ch;
    auto        start = std::chrono::steady_clock::now();
    while (true) {
        ssize_t n = read(fd, &ch, 1);
        if (n > 0) {
            result += ch;
            if (ch == '\n') {
                return result;
            }
        }
        else if (n == 0) {
            return result; // EOF
        }
        else {
            if (errno != EAGAIN && errno != EWOULDBLOCK) {
                return result;
            }
        }
        if (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count() >
            timeout_ms) {
            return result;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

} // namespace

// ── Start the binary, write + read, shut down ─────────────────────
TEST(DiskioStartupTest, WriteReadRoundTrip)
{
    std::string binary = find_binary();
    ASSERT_FALSE(binary.empty()) << "could not find crowdb-diskio binary";

    std::string disk_path = temp_file(1 << 16);
    ASSERT_FALSE(disk_path.empty());

    // Create a pipe to read the binary's stdout.
    int pipefd[2];
    ASSERT_EQ(pipe(pipefd), 0);

    pid_t pid = fork();
    ASSERT_GE(pid, 0);
    if (pid == 0) {
        // Child: redirect stdout to pipe, exec the binary.
        close(pipefd[0]);
        dup2(pipefd[1], STDOUT_FILENO);
        close(pipefd[1]);
        std::string port_arg = "0";
        // --disk format: <hex_id>:<path>[:<capacity>]
        // hex_id is "high:low" or just "low". Use "1" (low=1, high=0).
        std::string disk_arg = "1:" + disk_path + ":" + std::to_string(1 << 24);
        execl(binary.c_str(), "crowdb-diskio", "--port", "0", "--disk", disk_arg.c_str(), nullptr);
        // If exec fails:
        std::fprintf(stderr, "exec failed: %s\n", std::strerror(errno));
        _exit(127);
    }
    close(pipefd[1]);

    // Set the read end of the pipe to non-blocking.
    int flags = fcntl(pipefd[0], F_GETFL, 0);
    fcntl(pipefd[0], F_SETFL, flags | O_NONBLOCK);

    // Read the "listening" line to get the port.
    std::string line = read_line(pipefd[0]);
    ASSERT_FALSE(line.empty()) << "no output from crowdb-diskio binary";

    // Parse "crowdb-diskio listening on 127.0.0.1:PORT (N disks)"
    int port = 0;
    {
        // Find the last ':' before the port and parse the integer after it.
        size_t colon_pos = line.rfind(':');
        if (colon_pos != std::string::npos) {
            port = std::atoi(line.c_str() + colon_pos + 1);
        }
    }
    if (port == 0) {
        // Try to read another line in case the first was something else.
        line             = read_line(pipefd[0]);
        size_t colon_pos = line.rfind(':');
        if (colon_pos != std::string::npos) {
            port = std::atoi(line.c_str() + colon_pos + 1);
        }
    }
    ASSERT_GT(port, 0) << "could not parse listen port from: " << line;

    // Client side.
    SocketTransport client_transport(1, 1);
    client_transport.start();

    auto conn = client_transport.connect("127.0.0.1", port);
    ASSERT_NE(conn, nullptr);

    RpcClient caller;
    caller.set_completion_pool_size(16);
    caller.attach(conn.get());

    BufferPool *pool = client_transport.pool();

    // Write 4096 bytes.
    constexpr uint32_t   DATA_SIZE = 4096;
    std::vector<uint8_t> payload(DATA_SIZE);
    for (uint32_t i = 0; i < DATA_SIZE; i++) {
        payload[i] = static_cast<uint8_t>(i % 256);
    }

    uint64_t write_req_id = 10;
    {
        flatbuffers::FlatBufferBuilder fbb(128);
        rproto::FBInt128               fb_disk_id(0, 1);
        auto off = dproto::CreateFBDiskWriteRequest(fbb, write_req_id, 0, &fb_disk_id, 0, 0, DATA_SIZE);
        fbb.Finish(off);
        Buffer *ctrl = pool->alloc(fbb.GetSize());
        std::memcpy(ctrl->data, fbb.GetBufferPointer(), fbb.GetSize());
        ctrl->write(ctrl->data, fbb.GetSize());

        Buffer *data = pool->alloc(DATA_SIZE);
        data->write(payload.data(), DATA_SIZE);

        StartupState state;
        ASSERT_TRUE(caller.send(&client_transport, conn.get(), write_req_id, ctrl, data,
                                static_cast<uint16_t>(rproto::FBMsgType_EDiskWriteRequest), startup_on_complete,
                                &state));
        ASSERT_TRUE(wait_for(state));
        EXPECT_EQ(state.recv_request_id, write_req_id);
        EXPECT_EQ(state.ret_code.load(), static_cast<int16_t>(dproto::FBDiskIoRetCode_Success));
    }

    // Read 4096 bytes back.
    uint64_t read_req_id = 20;
    {
        flatbuffers::FlatBufferBuilder fbb(128);
        rproto::FBInt128               fb_disk_id(0, 1);
        auto off = dproto::CreateFBDiskReadRequest(fbb, read_req_id, 0, &fb_disk_id, 0, 0, DATA_SIZE, 0);
        fbb.Finish(off);
        Buffer *ctrl = pool->alloc(fbb.GetSize());
        std::memcpy(ctrl->data, fbb.GetBufferPointer(), fbb.GetSize());
        ctrl->write(ctrl->data, fbb.GetSize());

        StartupState state;
        ASSERT_TRUE(caller.send(&client_transport, conn.get(), read_req_id, ctrl, nullptr,
                                static_cast<uint16_t>(rproto::FBMsgType_EDiskReadRequest), startup_on_complete,
                                &state));
        ASSERT_TRUE(wait_for(state));
        EXPECT_EQ(state.recv_request_id, read_req_id);
        EXPECT_EQ(state.ret_code.load(), static_cast<int16_t>(dproto::FBDiskIoRetCode_Success));
        ASSERT_EQ(state.recv_data.size(), DATA_SIZE);
        EXPECT_EQ(std::memcmp(state.recv_data.data(), payload.data(), DATA_SIZE), 0);
    }

    // Fsync.
    uint64_t fsync_req_id = 30;
    {
        flatbuffers::FlatBufferBuilder fbb(128);
        rproto::FBInt128               fb_disk_id(0, 1);
        auto                           off = dproto::CreateFBDiskFsyncRequest(fbb, fsync_req_id, 0, &fb_disk_id);
        fbb.Finish(off);
        Buffer *ctrl = pool->alloc(fbb.GetSize());
        std::memcpy(ctrl->data, fbb.GetBufferPointer(), fbb.GetSize());
        ctrl->write(ctrl->data, fbb.GetSize());

        StartupState state;
        ASSERT_TRUE(caller.send(&client_transport, conn.get(), fsync_req_id, ctrl, nullptr,
                                static_cast<uint16_t>(rproto::FBMsgType_EDiskFsyncRequest), startup_on_complete,
                                &state));
        ASSERT_TRUE(wait_for(state));
        EXPECT_EQ(state.ret_code.load(), static_cast<int16_t>(dproto::FBDiskIoRetCode_Success));
    }

    // Shut down.
    client_transport.stop();
    kill(pid, SIGTERM);
    int status = 0;
    waitpid(pid, &status, 0);
    close(pipefd[0]);

    // Clean up temp file.
    unlink(disk_path.c_str());
}
