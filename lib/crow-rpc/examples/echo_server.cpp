// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// Example: standalone echo server using the crow-rpc C API.
//
// Listens on a port, registers the built-in echo handler, and runs
// until SIGTERM/SIGINT. On shutdown, prints transport stats to stdout
// as key=value lines (parsed by the CLI bench runner).
//
// Usage: crow-rpc-echo-server [--port=18080] [--io-engines=1] [--io-workers=1]
//        [--enable-nagle] [--log-dir=./log] [--metrics-interval=5]
// Defaults --log-dir to ./log (relative to CWD) when not specified.

#include "crow-rpc/c_api.h"

#include <gflags/gflags.h>

#include <atomic>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <string>

static std::atomic<bool> g_running{true};

static void on_signal(int /*signo*/)
{
    g_running.store(false);
}

// gflags definitions. gflags auto-generates --help and validates types.
DEFINE_int32(port, 18080, "Listen port");
DEFINE_uint32(io_engines, 1,
              "Number of independent epoll instances (each owns its own fd + connections, round-robin). "
              "1 = single-engine. >1 parallelizes event processing across independent kernel queues.");
DEFINE_uint32(io_workers, 1,
              "Total C++ I/O worker threads (across all engines). Per-engine = M / N. "
              "1 = single-worker (fast path). >1 per engine enables EPOLLONESHOT for multi-worker safety. "
              "Must be divisible by --io_engines.");
DEFINE_bool(enable_nagle, false,
            "Enable Nagle's algorithm (disable TCP_NODELAY). Default false (Nagle off, low latency).");
DEFINE_string(logdir, "", "Log directory for server + metrics logs. Default: ./log (relative to CWD)");
DEFINE_uint32(metrics_interval, 5, "Metrics flush interval in seconds (counters + latency histograms)");

int main(int argc, char *argv[])
{
    // Pre-scan for --help/-h: gflags --help dumps ALL transitive flags
    // (glog, folly). Show only our flags via --helpon instead.
    for (int i = 1; i < argc; i++) {
        std::string a = argv[i];
        if (a == "--help" || a == "-h" || a == "-help") {
            // Replace with --helpon=echo_server so gflags shows only our flags.
            argv[i] = const_cast<char *>("--helpon=echo_server");
            break;
        }
    }

    gflags::SetUsageMessage("crow-rpc-echo-server — standalone RPC echo server for bench rpc.\n"
                            "The server listens on --port and echoes back any request with a data payload.\n"
                            "Use with `crow-cli bench rpc --server_port <PORT>`.");
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    // Init logging — default to ./log (relative to CWD) so a manually-
    // started server always writes files alongside the bench run.
    std::string log_dir_arg = FLAGS_logdir;
    if (log_dir_arg.empty()) {
        log_dir_arg = "log";
    }
    std::error_code ec;
    std::filesystem::create_directories(log_dir_arg, ec);
    crow_rpc_init_logging(log_dir_arg.c_str(), "info", 30, 5, "echo-server");

    std::signal(SIGTERM, on_signal);
    std::signal(SIGINT, on_signal);

    crow_rpc_server_t server = crow_rpc_server_create_with_engines(nullptr, FLAGS_io_engines, FLAGS_io_workers);
    if (server == nullptr) {
        std::fprintf(stderr, "error: failed to create server\n");
        return 1;
    }

    crow_rpc_server_set_tcp_nodelay(server, FLAGS_enable_nagle ? 0 : 1);

    if (crow_rpc_server_listen(server, "127.0.0.1", FLAGS_port) != CROW_RPC_OK) {
        std::fprintf(stderr, "error: failed to listen on port %d\n", FLAGS_port);
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

    // Start metrics flush — the RPC lib registers all histograms,
    // bandwidths, and counters with MetricsRegistry::global() via
    // function-local statics. The periodic flush writes them to
    // metrics.log + stdout.
    std::string metrics_log_path = log_dir_arg + "/metrics.log";
    crow_rpc_metrics_start(metrics_log_path.c_str(), static_cast<double>(FLAGS_metrics_interval), 30, 5, 1);

    // Run until signaled.
    while (g_running.load()) {
        struct timespec ts;
        ts.tv_sec  = 0;
        ts.tv_nsec = 10'000'000; // 10ms
        nanosleep(&ts, nullptr);
    }

    crow_rpc_metrics_stop();

    // Print final transport stats for the CLI bench runner.
    crow_rpc_transport_stats_t stats;
    std::memset(&stats, 0, sizeof(stats));
    crow_rpc_server_transport_stats(server, &stats);
    std::printf("stats submit_to_writev_count=%llu submit_to_writev_sum_ns=%llu\n",
                static_cast<unsigned long long>(stats.submit_to_writev.count),
                static_cast<unsigned long long>(stats.submit_to_writev.sum_ns));
    std::fflush(stdout);

    crow_rpc_server_stop(server);
    crow_rpc_server_destroy(server);

    crow_rpc_shutdown_logging();
    return 0;
}
