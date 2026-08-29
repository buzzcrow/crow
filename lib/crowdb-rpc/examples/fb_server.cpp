// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// Example: standalone fb server using the crowdb-rpc C API.
//
// Listens on a port, registers the built-in echo handler, and runs
// until SIGTERM/SIGINT. On shutdown, prints transport stats to stdout
// as key=value lines (parsed by the CLI bench runner).
//
// Usage: crowdb-rpc-fb-server [--port=18080] [--io-engines=1] [--io-workers=1]
//        [--enable-nagle] [--logdir=./log] [--metrics-interval=5]
// Short aliases: -p -e -w -n -l -m (e.g. -p=18080 -e=2 -w=4).
// Defaults --logdir to ./log (relative to CWD) when not specified.

#include "crowdb-rpc/c_api.h"

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
// gflags has no native alias support, so short forms (-p, -e, etc.) are
// separate flags forwarded to the long ones after parse (see main).
DEFINE_int32(port, 18080, "Listen port");
DEFINE_int32(p, 18080, "Alias for --port");
DEFINE_uint32(io_engines, 1, "Independent epoll instances (round-robin). >1 parallelizes event queues.");
DEFINE_uint32(e, 1, "Alias for --io_engines");
DEFINE_uint32(io_workers, 1, "Total I/O worker threads. Must be divisible by --io_engines.");
DEFINE_uint32(w, 1, "Alias for --io_workers");
DEFINE_bool(enable_nagle, false, "Enable Nagle's algorithm (disable TCP_NODELAY).");
DEFINE_bool(n, false, "Alias for --enable_nagle");
DEFINE_bool(quickack, false, "Enable TCP_QUICKACK on connections (Linux only). Breaks Nagle + delayed-ACK deadlock.");
DEFINE_bool(event_write, false, "Event-write mode: submit() enqueues to I/O worker for coalesced writev.");
DEFINE_uint32(send_queue_capacity, 4096, "Per-connection send queue capacity (backpressure bound).");
DEFINE_string(logdir, "", "Log directory for server + metrics logs. Default: ./log.");
DEFINE_string(l, "", "Alias for --logdir");
DEFINE_uint32(metrics_interval, 5, "Metrics flush interval in seconds.");
DEFINE_uint32(m, 5, "Alias for --metrics_interval");

int main(int argc, char *argv[])
{
    // Pre-scan for --help/-h: gflags --help dumps ALL transitive flags
    // (glog, folly). Show only our flags via --helpon instead.
    for (int i = 1; i < argc; i++) {
        std::string a = argv[i];
        if (a == "--help" || a == "-h" || a == "-help") {
            // Replace with --helpon=fb_server so gflags shows only our flags.
            argv[i] = const_cast<char *>("--helpon=fb_server");
            break;
        }
    }

    gflags::SetUsageMessage("crowdb-rpc-fb-server — standalone RPC fb server for bench rpc.\n"
                            "The server listens on --port and echoes back any request with a data payload.\n"
                            "Use with `crowdb-cli bench rpc --server_port <PORT>`.");
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    // Forward short aliases to long flags. A short flag "is set" when
    // is_default is false (explicitly passed on the CLI). Short wins
    // over long when both are given.
    gflags::CommandLineFlagInfo info;
    if (gflags::GetCommandLineFlagInfo("p", &info) && !info.is_default)
        FLAGS_port = FLAGS_p;
    if (gflags::GetCommandLineFlagInfo("e", &info) && !info.is_default)
        FLAGS_io_engines = FLAGS_e;
    if (gflags::GetCommandLineFlagInfo("w", &info) && !info.is_default)
        FLAGS_io_workers = FLAGS_w;
    if (gflags::GetCommandLineFlagInfo("n", &info) && !info.is_default)
        FLAGS_enable_nagle = FLAGS_n;
    if (gflags::GetCommandLineFlagInfo("l", &info) && !info.is_default)
        FLAGS_logdir = FLAGS_l;
    if (gflags::GetCommandLineFlagInfo("m", &info) && !info.is_default)
        FLAGS_metrics_interval = FLAGS_m;

    // Init logging — default to ./log (relative to CWD) so a manually-
    // started server always writes files alongside the bench run.
    std::string log_dir_arg = FLAGS_logdir;
    if (log_dir_arg.empty()) {
        log_dir_arg = "log";
    }
    std::error_code ec;
    std::filesystem::create_directories(log_dir_arg, ec);
    crowdb_rpc_init_logging(log_dir_arg.c_str(), "info", 30, 5, "fb-server");

    std::signal(SIGTERM, on_signal);
    std::signal(SIGINT, on_signal);

    crowdb_rpc_server_t server = crowdb_rpc_server_create_with_engines(nullptr, FLAGS_io_engines, FLAGS_io_workers);
    if (server == nullptr) {
        std::fprintf(stderr, "error: failed to create server\n");
        return 1;
    }

    crowdb_rpc_server_set_tcp_nodelay(server, FLAGS_enable_nagle ? 0 : 1);
    crowdb_rpc_server_set_quickack(server, FLAGS_quickack ? 1 : 0);
    crowdb_rpc_server_set_event_write(server, FLAGS_event_write ? 1 : 0);
    crowdb_rpc_server_set_send_queue_capacity(server, FLAGS_send_queue_capacity);

    if (crowdb_rpc_server_listen(server, "127.0.0.1", FLAGS_port) != CROWDB_RPC_OK) {
        std::fprintf(stderr, "error: failed to listen on port %d\n", FLAGS_port);
        crowdb_rpc_server_destroy(server);
        return 1;
    }

    int actual_port = crowdb_rpc_server_port(server);
    // Print the port so the parent process can read it.
    std::printf("listening port=%d\n", actual_port);
    std::fflush(stdout);

    // Register the built-in echo handler for msg_type 100.
    const uint16_t ECHO_MSG_TYPE = 100;
    crowdb_rpc_server_register_echo_handler(server, ECHO_MSG_TYPE);

    crowdb_rpc_server_start(server);

    // Start metrics flush — the RPC lib registers all histograms,
    // bandwidths, and counters with MetricsRegistry::global() via
    // function-local statics. The periodic flush writes them to
    // metrics.log + stdout.
    std::string metrics_log_path = log_dir_arg + "/metrics.log";
    crowdb_rpc_metrics_start(metrics_log_path.c_str(), static_cast<double>(FLAGS_metrics_interval), 30, 5, 1);

    // Run until signaled.
    while (g_running.load()) {
        struct timespec ts;
        ts.tv_sec  = 0;
        ts.tv_nsec = 10'000'000; // 10ms
        nanosleep(&ts, nullptr);
    }

    crowdb_rpc_metrics_stop();

    // Print final transport stats for the CLI bench runner.
    crowdb_rpc_transport_stats_t stats;
    std::memset(&stats, 0, sizeof(stats));
    crowdb_rpc_server_transport_stats(server, &stats);
    std::printf("stats submit_to_writev_count=%llu submit_to_writev_sum_ns=%llu send_queue_rejects=%llu\n",
                static_cast<unsigned long long>(stats.submit_to_writev.count),
                static_cast<unsigned long long>(stats.submit_to_writev.sum_ns),
                static_cast<unsigned long long>(stats.send_queue_rejects));
    std::fflush(stdout);

    crowdb_rpc_server_stop(server);
    crowdb_rpc_server_destroy(server);

    crowdb_rpc_shutdown_logging();
    return 0;
}
