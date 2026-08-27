// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// Example: standalone echo server using the crow-rpc C API.
//
// Listens on a port, registers the built-in echo handler, and runs
// until SIGTERM/SIGINT. On shutdown, prints transport stats to stdout
// as key=value lines (parsed by the CLI bench runner).
//
// Usage: crow-rpc-echo-server --port <port> [--io-engines N] [--io-workers M]
//        [--enable-nagle] [--log-dir <dir>]

#include "crow-rpc/c_api.h"

#include "crow-common/metrics/counter.h"
#include "crow-common/metrics/metrics.h"

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

// Transport stats counters registered with MetricsRegistry for periodic flush.
struct TransportMetricsBridge
{
    crow::common::metrics::Counter *read_calls;
    crow::common::metrics::Counter *writev_calls;
    crow::common::metrics::Counter *frames_sent;
    crow::common::metrics::Counter *frames_parsed;

    // Previous cumulative values for delta computation.
    uint64_t prev_read_calls{0};
    uint64_t prev_writev_calls{0};
    uint64_t prev_frames_sent{0};
    uint64_t prev_frames_parsed{0};

    TransportMetricsBridge()
    {
        auto &reg = crow::common::metrics::MetricsRegistry::global();
        read_calls    = reg.register_counter("rpc.transport.read_calls");
        writev_calls  = reg.register_counter("rpc.transport.writev_calls");
        frames_sent   = reg.register_counter("rpc.transport.frames_sent");
        frames_parsed = reg.register_counter("rpc.transport.frames_parsed");
    }

    void sample(crow_rpc_server_t server)
    {
        crow_rpc_transport_stats_t stats;
        std::memset(&stats, 0, sizeof(stats));
        crow_rpc_server_transport_stats(server, &stats);

        uint64_t cur_read_calls    = stats.read_calls;
        uint64_t cur_writev_calls  = stats.writev_calls;
        uint64_t cur_frames_sent   = stats.frames_sent;
        uint64_t cur_frames_parsed = stats.frames_parsed;

        read_calls->inc_by(cur_read_calls - prev_read_calls);
        writev_calls->inc_by(cur_writev_calls - prev_writev_calls);
        frames_sent->inc_by(cur_frames_sent - prev_frames_sent);
        frames_parsed->inc_by(cur_frames_parsed - prev_frames_parsed);

        prev_read_calls    = cur_read_calls;
        prev_writev_calls  = cur_writev_calls;
        prev_frames_sent   = cur_frames_sent;
        prev_frames_parsed = cur_frames_parsed;
    }
};

int main(int argc, char *argv[])
{
    int      port              = 0;
    uint32_t io_engines        = 1;
    uint32_t io_workers        = 1;
    int      tcp_nodelay       = 1;
    const char *log_dir        = nullptr;
    double   metrics_interval  = 5.0;

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
        else if (arg == "--enable-nagle") {
            tcp_nodelay = 0;
        }
        else if (arg == "--log-dir" && i + 1 < argc) {
            log_dir = argv[++i];
        }
        else if (arg == "--metrics-interval" && i + 1 < argc) {
            metrics_interval = static_cast<double>(parse_u32(argv[++i], "metrics-interval"));
        }
        else if (arg == "--help" || arg == "-h") {
            std::printf("usage: crow-rpc-echo-server --port <port> "
                        "[--io-engines N] [--io-workers M] [--enable-nagle] "
                        "[--log-dir <dir>] [--metrics-interval N]\n");
            return 0;
        }
        else {
            std::fprintf(stderr, "error: unknown argument %s\n", argv[i]);
            return 1;
        }
    }

    // Init logging to log_dir if specified.
    if (log_dir != nullptr) {
        crow_rpc_init_logging(log_dir, "info", 30, 5, "echo-server");
    }

    std::signal(SIGTERM, on_signal);
    std::signal(SIGINT, on_signal);

    crow_rpc_server_t server = crow_rpc_server_create_with_engines(nullptr, io_engines, io_workers);
    if (server == nullptr) {
        std::fprintf(stderr, "error: failed to create server\n");
        return 1;
    }

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

    // Start metrics flush: bridge transport stats to crow-common metrics
    // and flush periodically to file + console.
    TransportMetricsBridge bridge;
    std::string metrics_log_path =
        log_dir != nullptr ? std::string(log_dir) + "/metrics.log" : std::string("/tmp/echo-server-metrics.log");
    crow_rpc_metrics_start(metrics_log_path.c_str(), metrics_interval, 30, 5, 1);

    // Run until signaled. Sample transport stats every ~1s to feed
    // deltas into the crow-common metrics counters.
    int sample_tick = 0;
    while (g_running.load()) {
        struct timespec ts;
        ts.tv_sec  = 0;
        ts.tv_nsec = 10'000'000; // 10ms
        nanosleep(&ts, nullptr);
        if (++sample_tick >= 100) { // ~1s
            bridge.sample(server);
            sample_tick = 0;
        }
    }

    // Final bridge sample + stop metrics (does one last flush).
    bridge.sample(server);
    crow_rpc_metrics_stop();

    // Print transport stats before stopping.
    crow_rpc_transport_stats_t stats;
    std::memset(&stats, 0, sizeof(stats));
    crow_rpc_server_transport_stats(server, &stats);
    std::printf("stats read_calls=%llu writev_calls=%llu "
                "frames_sent=%llu frames_parsed=%llu "
                "read_bytes=%llu writev_bytes=%llu "
                "submit_to_writev_count=%llu submit_to_writev_sum_ns=%llu "
                "loop_count=%llu event_count_sum=%llu "
                "wait_ns_sum=%llu read_ns_sum=%llu flush_ns_sum=%llu\n",
                static_cast<unsigned long long>(stats.read_calls), static_cast<unsigned long long>(stats.writev_calls),
                static_cast<unsigned long long>(stats.frames_sent),
                static_cast<unsigned long long>(stats.frames_parsed), static_cast<unsigned long long>(stats.read_bytes),
                static_cast<unsigned long long>(stats.writev_bytes),
                static_cast<unsigned long long>(stats.submit_to_writev.count),
                static_cast<unsigned long long>(stats.submit_to_writev.sum_ns),
                static_cast<unsigned long long>(stats.loop_count),
                static_cast<unsigned long long>(stats.event_count_sum),
                static_cast<unsigned long long>(stats.wait_ns_sum),
                static_cast<unsigned long long>(stats.read_ns_sum),
                static_cast<unsigned long long>(stats.flush_ns_sum));
    std::fflush(stdout);

    crow_rpc_server_stop(server);
    crow_rpc_server_destroy(server);

    if (log_dir != nullptr) {
        crow_rpc_shutdown_logging();
    }
    return 0;
}
