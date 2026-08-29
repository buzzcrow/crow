// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// Metrics module: lightweight atomic counters, gauges, bandwidth,
// latency histograms, and latency summaries with periodic flush to a
// dedicated metrics log file. Mirrors the Rust metrics design.
//
// Each metric type lives in its own header (counter.h, gauge.h,
// bandwidth.h, latency_histogram.h, latency_summary.h); this header
// aggregates them and defines the MetricsRegistry that owns all
// metric instances and drives the periodic flush.
//
// Two registry scopes:
//   - global() — process-level singleton for unprefixed metrics
//     (e.g. rpc.client.*). Use from function-local statics:
//       static Counter *c = MetricsRegistry::global().register_counter("name");
//   - per-instance — constructed by engines that need dynamic prefixes
//     (e.g. s.{store_id}.g.{group_id}.buf.hits.c). Owned by the engine.
#pragma once

#include "crowdb-common/metrics/bandwidth.h"
#include "crowdb-common/metrics/callback_gauge.h"
#include "crowdb-common/metrics/counter.h"
#include "crowdb-common/metrics/gauge.h"
#include "crowdb-common/metrics/latency_histogram.h"
#include "crowdb-common/metrics/latency_summary.h"

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace crowdb::common::metrics
{

// ── Registry ────────────────────────────────────────────────────

class MetricsRegistry
{
  public:
    MetricsRegistry() = default;
    ~MetricsRegistry();

    MetricsRegistry(const MetricsRegistry &)            = delete;
    MetricsRegistry &operator=(const MetricsRegistry &) = delete;

    // Process-level singleton. Use for unprefixed metrics that are
    // shared across all instances (e.g. rpc.client.*). Thread-safe
    // (Meyers singleton, C++11+).
    static MetricsRegistry &global();

    Counter          *register_counter(const std::string &name);
    Gauge            *register_gauge(const std::string &name);
    CallbackGauge    *register_callback_gauge(const std::string &name, CallbackGauge::Callback cb);
    Bandwidth        *register_bandwidth(const std::string &name);
    LatencyHistogram *register_histogram(const std::string &name);
    LatencySummary   *register_summary(const std::string &name);

    // Flush all metrics to the given file stream.
    // section_label: "metrics" or "cpp-metrics" (header prefix).
    // width: 0 = use internal max name length; >0 = use this width for
    //        column alignment across Rust and C++ sections.
    // col_w: negotiated count/tps column widths (0 = use C++ defaults).
    void flush_to(FILE *fp, double window_secs, const char *timestamp, const char *section_label = "metrics",
                  size_t width = 0, size_t count_w = 0, size_t tps_w = 0);

    // Return the current max metric name length across all sections.
    size_t max_name_len() const;

    // Start periodic flush thread. interval_secs in seconds.
    // max_file_mb and max_files control size-based rotation with gzip
    // compression of rotated files. When console is true, each flush
    // is also written to stdout.
    void start(const std::string &log_path, double interval_secs, size_t max_file_mb = 30, size_t max_files = 5,
               bool console = false);
    void stop();

  private:
    std::vector<std::unique_ptr<Counter>>          counters_;
    std::vector<std::unique_ptr<Gauge>>            gauges_;
    std::vector<std::unique_ptr<CallbackGauge>>    callback_gauges_;
    std::vector<std::unique_ptr<Bandwidth>>        bandwidths_;
    std::vector<std::unique_ptr<LatencyHistogram>> histograms_;
    std::vector<std::unique_ptr<LatencySummary>>   summaries_;

    std::mutex              flush_mutex_;
    std::condition_variable stop_cv_;
    std::thread             flush_thread_;
    std::atomic<bool>       running_{false};
    std::string             log_path_;
    double                  interval_secs_  = 0.0;
    size_t                  max_file_bytes_ = 30ULL * 1024ULL * 1024ULL;
    size_t                  max_files_      = 5;
    bool                    console_        = false;

    void flush_to_file();
    void check_rotate();
    void prune_rotated();
};

} // namespace crowdb::common::metrics
