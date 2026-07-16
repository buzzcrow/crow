// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// Metrics module: lightweight atomic counters, gauges, bandwidth,
// latency histograms, and latency summaries with periodic flush to a
// dedicated metrics log file. Mirrors the Rust metrics design.
#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace crowtree
{

// ── Metric types ────────────────────────────────────────────────

// Monotonic counter with window delta and cumulative total.
// `window` is reset to 0 on each flush; `total` accumulates forever.
class Counter
{
  public:
    explicit Counter(std::string name) : name_(std::move(name)), window_(0), total_(0)
    {
    }

    void inc()
    {
        window_.fetch_add(1, std::memory_order_relaxed);
    }

    void inc_by(uint64_t n)
    {
        window_.fetch_add(n, std::memory_order_relaxed);
    }

    // Flush: return {window_delta, total} and reset window.
    struct Snapshot
    {
        uint64_t count;
        uint64_t total;
    };

    Snapshot flush()
    {
        uint64_t w = window_.exchange(0, std::memory_order_relaxed);
        uint64_t t = total_.fetch_add(w, std::memory_order_relaxed) + w;
        return {w, t};
    }

    const std::string &name() const
    {
        return name_;
    }

  private:
    std::string           name_;
    std::atomic<uint64_t> window_;
    std::atomic<uint64_t> total_;
};

// Gauge: current state, can go up or down.
class Gauge
{
  public:
    explicit Gauge(std::string name) : name_(std::move(name)), value_(0)
    {
    }

    void set(uint64_t v)
    {
        value_.store(v, std::memory_order_relaxed);
    }

    uint64_t get() const
    {
        return value_.load(std::memory_order_relaxed);
    }

    const std::string &name() const
    {
        return name_;
    }

  private:
    std::string           name_;
    std::atomic<uint64_t> value_;
};

// Bandwidth: tracks count, byte sum (window), and total bytes.
class Bandwidth
{
  public:
    explicit Bandwidth(std::string name) : name_(std::move(name)), count_(0), sum_(0), total_bytes_(0)
    {
    }

    void observe(uint64_t bytes)
    {
        count_.fetch_add(1, std::memory_order_relaxed);
        sum_.fetch_add(bytes, std::memory_order_relaxed);
        total_bytes_.fetch_add(bytes, std::memory_order_relaxed);
    }

    struct Snapshot
    {
        uint64_t count;
        uint64_t sum;
        uint64_t total_bytes;
    };

    Snapshot flush()
    {
        uint64_t c = count_.exchange(0, std::memory_order_relaxed);
        uint64_t s = sum_.exchange(0, std::memory_order_relaxed);
        uint64_t t = total_bytes_.load(std::memory_order_relaxed);
        return {c, s, t};
    }

    const std::string &name() const
    {
        return name_;
    }

  private:
    std::string           name_;
    std::atomic<uint64_t> count_;
    std::atomic<uint64_t> sum_;
    std::atomic<uint64_t> total_bytes_;
};

// Latency histogram with fixed buckets and percentile reporting.
class LatencyHistogram
{
  public:
    explicit LatencyHistogram(std::string name);

    void observe(uint64_t ns);

    struct Snapshot
    {
        uint64_t              count;
        uint64_t              sum;
        uint64_t              total_count;
        std::vector<uint64_t> bucket_counts;
    };

    Snapshot flush();

    static uint64_t percentile(const Snapshot &snap, double p);

    const std::string &name() const
    {
        return name_;
    }

  private:
    std::string                                         name_;
    std::vector<std::unique_ptr<std::atomic<uint64_t>>> buckets_;
    std::atomic<uint64_t>                               count_;
    std::atomic<uint64_t>                               sum_;
    std::atomic<uint64_t>                               total_count_;
};

// Lightweight latency summary: count, sum, max, total_count.
class LatencySummary
{
  public:
    explicit LatencySummary(std::string name) : name_(std::move(name)), count_(0), sum_(0), max_(0), total_count_(0)
    {
    }

    void observe(uint64_t ns)
    {
        count_.fetch_add(1, std::memory_order_relaxed);
        sum_.fetch_add(ns, std::memory_order_relaxed);
        total_count_.fetch_add(1, std::memory_order_relaxed);
        uint64_t old_max = max_.load(std::memory_order_relaxed);
        while (ns > old_max && !max_.compare_exchange_weak(old_max, ns, std::memory_order_relaxed)) {
        }
    }

    struct Snapshot
    {
        uint64_t count;
        uint64_t sum;
        uint64_t max;
        uint64_t total_count;
    };

    Snapshot flush()
    {
        uint64_t c = count_.exchange(0, std::memory_order_relaxed);
        uint64_t s = sum_.exchange(0, std::memory_order_relaxed);
        uint64_t m = max_.exchange(0, std::memory_order_relaxed);
        uint64_t t = total_count_.load(std::memory_order_relaxed);
        return {c, s, m, t};
    }

    const std::string &name() const
    {
        return name_;
    }

  private:
    std::string           name_;
    std::atomic<uint64_t> count_;
    std::atomic<uint64_t> sum_;
    std::atomic<uint64_t> max_;
    std::atomic<uint64_t> total_count_;
};

// ── Registry ────────────────────────────────────────────────────

class MetricsRegistry
{
  public:
    MetricsRegistry() = default;
    ~MetricsRegistry();

    MetricsRegistry(const MetricsRegistry &)            = delete;
    MetricsRegistry &operator=(const MetricsRegistry &) = delete;

    Counter          *register_counter(const std::string &name);
    Gauge            *register_gauge(const std::string &name);
    Bandwidth        *register_bandwidth(const std::string &name);
    LatencyHistogram *register_histogram(const std::string &name);
    LatencySummary   *register_summary(const std::string &name);

    // Flush all metrics to the given file stream.
    void flush_to(FILE *fp, double window_secs, const char *timestamp);

    // Start periodic flush thread. interval_secs in seconds.
    void start(const std::string &log_path, double interval_secs);
    void stop();

  private:
    std::vector<std::unique_ptr<Counter>>          counters_;
    std::vector<std::unique_ptr<Gauge>>            gauges_;
    std::vector<std::unique_ptr<Bandwidth>>        bandwidths_;
    std::vector<std::unique_ptr<LatencyHistogram>> histograms_;
    std::vector<std::unique_ptr<LatencySummary>>   summaries_;

    std::mutex        flush_mutex_;
    std::thread       flush_thread_;
    std::atomic<bool> running_{false};
    std::string       log_path_;
    double            interval_secs_ = 0.0;
};

} // namespace crowtree
