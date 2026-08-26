// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// Example: standalone echo server using the crow-rpc C API.
//
// Listens on a port, registers the built-in echo handler, and runs
// until SIGTERM/SIGINT. On shutdown, prints transport stats to stdout
// as key=value lines (parsed by the CLI bench runner).
//
// Usage: crow-rpc-echo-server --port <port> [--io-engines N] [--io-workers M]

#include "crow-rpc/c_api.h"

#include <atomic>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>

static std::atomic<bool> g_running{true};

static void on_signal(int)
{
    g_running.store(false);
}

// Parse a non-negative integer from argv, or exit on error.
static uint32_t parse_u32(const char *s, const char *name)
{
    char *end = nullptr;
    long  v   = std::strtol(s, &end, 10);
    if (end == s || *end != '\0' || v < 0 || v > 0xFFFFFFFF) {
        std::fprintf(stderr, "error: --%s expects a non-negative integer, got %s\n", name, s);
        std::exit(1);
    }
    return static_cast<uint32_t>(v);
}

int main(int argc, char *argv[])
{
    int      port         = 0;
    uint32_t io_engines   = 1;
    uint32_t io_workers   = 1;
    int      direct_write = 0;
    int      tcp_nodelay  = 1;

    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "--port" && i + 1 < argc) {
            port = static_cast<int>(parse_u32(argv[++i], "port"));
        }
        else if (arg == "--io-engines" && i + 1 < argc) {
            io_engines = parse_u32(argv[++i], "io-engines");
        }
        else if (arg == "--io-workers" && i + 1 < argc) {
            io_workers = parse_u32(argv[++i], "io-workers");
        }
        else if (arg == "--direct-write") {
            direct_write = 1;
        }
        else if (arg == "--enable-nagle") {
            tcp_nodelay = 0;
        }
        else if (arg == "--help" || arg == "-h") {
            std::printf("usage: crow-rpc-echo-server --port <port> "
                        "[--io-engines N] [--io-workers M] [--direct-write] [--enable-nagle]\n");
            return 0;
        }
        else {
            std::fprintf(stderr, "error: unknown argument %s\n", argv[i]);
            return 1;
        }
    }

    std::signal(SIGTERM, on_signal);
    std::signal(SIGINT, on_signal);

    crow_rpc_server_t server = crow_rpc_server_create_with_engines(nullptr, io_engines, io_workers);
    if (server == nullptr) {
        std::fprintf(stderr, "error: failed to create server\n");
        return 1;
    }

    crow_rpc_server_set_direct_write(server, direct_write);
    crow_rpc_server_set_tcp_nodelay(server, tcp_nodelay);

    if (crow_rpc_server_listen(server, "127.0.0.1", port) != CROW_RPC_OK) {
        std::fprintf(stderr, "error: failed to listen on port %d\n", port);
        crow_rpc_server_destroy(server);
        return 1;
    }

    int actual_port = crow_rpc_server_port(server);
    // Print the port so the parent process can read it.
    std::printf("listening port=%d\n", actual_port);
    std::fflush(stdout);

    // Register the built-in echo handler for msg_type 100.
    const uint16_t ECHO_MSG_TYPE = 100;
    crow_rpc_server_register_echo_handler(server, ECHO_MSG_TYPE);

    crow_rpc_server_start(server);

    // Run until signaled.
    while (g_running.load()) {
        // Busy-wait with a short sleep to avoid spinning.
        struct timespec ts;
        ts.tv_sec  = 0;
        ts.tv_nsec = 10'000'000; // 10ms
        nanosleep(&ts, nullptr);
    }

    // Print transport stats before stopping.
    crow_rpc_transport_stats_t stats;
    std::memset(&stats, 0, sizeof(stats));
    crow_rpc_server_transport_stats(server, &stats);
    std::printf("stats read_calls=%llu writev_calls=%llu "
                "frames_sent=%llu frames_parsed=%llu "
                "read_bytes=%llu writev_bytes=%llu "
                "submit_to_writev_count=%llu submit_to_writev_sum_ns=%llu\n",
                static_cast<unsigned long long>(stats.read_calls), static_cast<unsigned long long>(stats.writev_calls),
                static_cast<unsigned long long>(stats.frames_sent),
                static_cast<unsigned long long>(stats.frames_parsed), static_cast<unsigned long long>(stats.read_bytes),
                static_cast<unsigned long long>(stats.writev_bytes),
                static_cast<unsigned long long>(stats.submit_to_writev.count),
                static_cast<unsigned long long>(stats.submit_to_writev.sum_ns));
    std::fflush(stdout);

    crow_rpc_server_stop(server);
    crow_rpc_server_destroy(server);
    return 0;
}
