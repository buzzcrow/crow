// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#include "crowtree/metrics.h"

#include "crowtree/gzip.h"

#include <sys/stat.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdio>
#include <cstring>

namespace crowtree
{

// ── LatencyHistogram ────────────────────────────────────────────

static constexpr size_t                        kNumBuckets     = 12;
static const std::array<uint64_t, kNumBuckets> kBucketBoundsNs = {
    1'000,         // 1us
    10'000,        // 10us
    100'000,       // 100us
    500'000,       // 500us
    1'000'000,     // 1ms
    5'000'000,     // 5ms
    10'000'000,    // 10ms
    50'000'000,    // 50ms
    100'000'000,   // 100ms
    500'000'000,   // 500ms
    1'000'000'000, // 1s
    UINT64_MAX     // infinity
};

LatencyHistogram::LatencyHistogram(std::string name) : name_(std::move(name)), count_(0), sum_(0), total_count_(0)
{
    buckets_.reserve(kNumBuckets);
    for (size_t i = 0; i < kNumBuckets; ++i) {
        buckets_.push_back(std::make_unique<std::atomic<uint64_t>>(0));
    }
}

void LatencyHistogram::observe(uint64_t ns)
{
    size_t lo = 0;
    size_t hi = kNumBuckets;
    while (lo < hi) {
        size_t mid = (lo + hi) / 2;
        if (kBucketBoundsNs[mid] < ns) {
            lo = mid + 1;
        }
        else {
            hi = mid;
        }
    }
    if (lo >= kNumBuckets) {
        lo = kNumBuckets - 1;
    }
    buckets_[lo]->fetch_add(1, std::memory_order_relaxed);
    count_.fetch_add(1, std::memory_order_relaxed);
    sum_.fetch_add(ns, std::memory_order_relaxed);
    total_count_.fetch_add(1, std::memory_order_relaxed);
}

LatencyHistogram::Snapshot LatencyHistogram::flush()
{
    Snapshot snap;
    snap.count       = count_.exchange(0, std::memory_order_relaxed);
    snap.sum         = sum_.exchange(0, std::memory_order_relaxed);
    snap.total_count = total_count_.load(std::memory_order_relaxed);
    snap.bucket_counts.resize(kNumBuckets);
    for (size_t i = 0; i < kNumBuckets; ++i) {
        snap.bucket_counts[i] = buckets_[i]->exchange(0, std::memory_order_relaxed);
    }
    return snap;
}

uint64_t LatencyHistogram::percentile(const Snapshot &snap, double p)
{
    if (snap.count == 0) {
        return 0;
    }
    double target_d = static_cast<double>(snap.count) * p / 100.0;
    auto   target   = static_cast<uint64_t>(target_d);
    if (target == 0) {
        target = 1;
    }
    uint64_t cumulative = 0;
    for (size_t i = 0; i < kNumBuckets; ++i) {
        cumulative += snap.bucket_counts[i];
        if (cumulative >= target) {
            return kBucketBoundsNs[i];
        }
    }
    return kBucketBoundsNs[kNumBuckets - 1];
}

// ── MetricsRegistry ─────────────────────────────────────────────

MetricsRegistry::~MetricsRegistry() // NOLINT(bugprone-exception-escape)
{
    stop();
}

Counter *MetricsRegistry::register_counter(const std::string &name)
{
    auto     h   = std::make_unique<Counter>(name);
    Counter *raw = h.get();
    counters_.push_back(std::move(h));
    return raw;
}

Gauge *MetricsRegistry::register_gauge(const std::string &name)
{
    auto   h   = std::make_unique<Gauge>(name);
    Gauge *raw = h.get();
    gauges_.push_back(std::move(h));
    return raw;
}

Bandwidth *MetricsRegistry::register_bandwidth(const std::string &name)
{
    auto       h   = std::make_unique<Bandwidth>(name);
    Bandwidth *raw = h.get();
    bandwidths_.push_back(std::move(h));
    return raw;
}

LatencyHistogram *MetricsRegistry::register_histogram(const std::string &name)
{
    auto              h   = std::make_unique<LatencyHistogram>(name);
    LatencyHistogram *raw = h.get();
    histograms_.push_back(std::move(h));
    return raw;
}

LatencySummary *MetricsRegistry::register_summary(const std::string &name)
{
    auto            h   = std::make_unique<LatencySummary>(name);
    LatencySummary *raw = h.get();
    summaries_.push_back(std::move(h));
    return raw;
}

static std::string iso8601_now()
{
    auto                 now = std::chrono::system_clock::now();
    auto                 t   = std::chrono::system_clock::to_time_t(now);
    auto                 ms  = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()) % 1000;
    std::array<char, 40> buf{}; // NOLINT(modernize-avoid-c-arrays)
    std::strftime(buf.data(), buf.size(), "%Y-%m-%dT%H:%M:%S", std::gmtime(&t));
    std::array<char, 48> out{}; // NOLINT(modernize-avoid-c-arrays)
    std::snprintf(out.data(), out.size(), "%s.%03lldZ", buf.data(), static_cast<long long>(ms.count()));
    return out.data();
}

// Helper: sort indices by metric name for deterministic output.
template <typename T> static std::vector<size_t> sorted_indices(const std::vector<std::unique_ptr<T>> &vec)
{
    std::vector<size_t> idx(vec.size());
    for (size_t i = 0; i < vec.size(); ++i) {
        idx[i] = i;
    }
    std::sort(idx.begin(), idx.end(), [&](size_t a, size_t b) { return vec[a]->name() < vec[b]->name(); });
    return idx;
}

void MetricsRegistry::flush_to(FILE *fp, double window_secs, const char *timestamp)
{
    std::lock_guard<std::mutex> lock(flush_mutex_);

    std::fprintf(fp, "[metrics %s window=%.0fs]\n", timestamp, window_secs);

    // Counters
    if (!counters_.empty()) {
        size_t max_name = 0;
        for (const auto &e : counters_) {
            max_name = std::max(max_name, e->name().size());
        }
        std::fprintf(fp, "%-*s  count  tps(/s)  total\n", static_cast<int>(max_name), "name");
        auto idx = sorted_indices(counters_);
        for (size_t i : idx) {
            auto snap = counters_[i]->flush();
            if (snap.count == 0) {
                continue;
            }
            double tps_d = static_cast<double>(snap.count) / window_secs;
            auto   tps   = static_cast<uint64_t>(tps_d);
            std::fprintf(fp, "%-*s  %5llu  %7llu  %6llu\n", static_cast<int>(max_name), counters_[i]->name().c_str(),
                         static_cast<unsigned long long>(snap.count), static_cast<unsigned long long>(tps),
                         static_cast<unsigned long long>(snap.total));
        }
    }

    // Histograms
    if (!histograms_.empty()) {
        size_t max_name = 0;
        for (const auto &e : histograms_) {
            max_name = std::max(max_name, e->name().size());
        }
        std::fprintf(fp, "%-*s  count  p50  p99  avg(ns)  total\n", static_cast<int>(max_name), "name");
        auto idx = sorted_indices(histograms_);
        for (size_t i : idx) {
            auto snap = histograms_[i]->flush();
            if (snap.count == 0) {
                continue;
            }
            uint64_t p50 = LatencyHistogram::percentile(snap, 50.0);
            uint64_t p99 = LatencyHistogram::percentile(snap, 99.0);
            uint64_t avg = snap.count > 0 ? snap.sum / snap.count : 0;
            std::fprintf(fp, "%-*s  %5llu  %4llu  %4llu  %7llu  %5llu\n", static_cast<int>(max_name),
                         histograms_[i]->name().c_str(), static_cast<unsigned long long>(snap.count),
                         static_cast<unsigned long long>(p50), static_cast<unsigned long long>(p99),
                         static_cast<unsigned long long>(avg), static_cast<unsigned long long>(snap.total_count));
        }
    }

    // Summaries
    if (!summaries_.empty()) {
        size_t max_name = 0;
        for (const auto &e : summaries_) {
            max_name = std::max(max_name, e->name().size());
        }
        std::fprintf(fp, "%-*s  count  avg(ns)  max(ns)  total\n", static_cast<int>(max_name), "name");
        auto idx = sorted_indices(summaries_);
        for (size_t i : idx) {
            auto snap = summaries_[i]->flush();
            if (snap.count == 0) {
                continue;
            }
            uint64_t avg = snap.count > 0 ? snap.sum / snap.count : 0;
            std::fprintf(fp, "%-*s  %5llu  %7llu  %7llu  %5llu\n", static_cast<int>(max_name),
                         summaries_[i]->name().c_str(), static_cast<unsigned long long>(snap.count),
                         static_cast<unsigned long long>(avg), static_cast<unsigned long long>(snap.max),
                         static_cast<unsigned long long>(snap.total_count));
        }
    }

    // Bandwidths
    if (!bandwidths_.empty()) {
        size_t max_name = 0;
        for (const auto &e : bandwidths_) {
            max_name = std::max(max_name, e->name().size());
        }
        std::fprintf(fp, "%-*s  count  avg_size  rate(B/s)  total(B)\n", static_cast<int>(max_name), "name");
        auto idx = sorted_indices(bandwidths_);
        for (size_t i : idx) {
            auto snap = bandwidths_[i]->flush();
            if (snap.count == 0) {
                continue;
            }
            uint64_t avg_size = snap.count > 0 ? snap.sum / snap.count : 0;
            double   rate_d   = static_cast<double>(snap.sum) / window_secs;
            auto     rate     = static_cast<uint64_t>(rate_d);
            std::fprintf(fp, "%-*s  %5llu  %8llu  %9llu  %8llu\n", static_cast<int>(max_name),
                         bandwidths_[i]->name().c_str(), static_cast<unsigned long long>(snap.count),
                         static_cast<unsigned long long>(avg_size), static_cast<unsigned long long>(rate),
                         static_cast<unsigned long long>(snap.total_bytes));
        }
    }

    // Gauges (always printed, even if 0)
    if (!gauges_.empty()) {
        size_t max_name = 0;
        for (const auto &e : gauges_) {
            max_name = std::max(max_name, e->name().size());
        }
        std::fprintf(fp, "%-*s  value\n", static_cast<int>(max_name), "name");
        auto idx = sorted_indices(gauges_);
        for (size_t i : idx) {
            std::fprintf(fp, "%-*s  %5llu\n", static_cast<int>(max_name), gauges_[i]->name().c_str(),
                         static_cast<unsigned long long>(gauges_[i]->get()));
        }
    }

    std::fprintf(fp, "\n");
}

void MetricsRegistry::start(const std::string &log_path, double interval_secs, size_t max_file_mb, size_t max_files)
{
    log_path_       = log_path;
    interval_secs_  = interval_secs;
    max_file_bytes_ = max_file_mb * 1024 * 1024;
    max_files_      = max_files;
    running_.store(true, std::memory_order_relaxed);
    flush_thread_ = std::thread([this]() {
        while (running_.load(std::memory_order_relaxed)) {
            std::this_thread::sleep_for(std::chrono::milliseconds(static_cast<int>(interval_secs_ * 1000)));
            if (!running_.load(std::memory_order_relaxed)) {
                break;
            }
            flush_to_file();
        }
    });
}

void MetricsRegistry::stop()
{
    if (!running_.exchange(false, std::memory_order_relaxed)) {
        return;
    }
    if (flush_thread_.joinable()) {
        flush_thread_.join();
    }
    flush_to_file();
}

void MetricsRegistry::flush_to_file()
{
    // Check if rotation is needed before writing.
    check_rotate();

    FILE *fp = std::fopen(log_path_.c_str(), "a");
    if (fp == nullptr) {
        return;
    }
    std::string ts = iso8601_now();
    flush_to(fp, interval_secs_, ts.c_str());
    std::fflush(fp);
    std::fclose(fp);
}

void MetricsRegistry::check_rotate()
{
    FILE *fp = std::fopen(log_path_.c_str(), "rb");
    if (fp == nullptr) {
        return;
    }
    std::fseek(fp, 0, SEEK_END);
    long size = std::ftell(fp);
    std::fclose(fp);
    if (size < 0 || static_cast<size_t>(size) < max_file_bytes_) {
        return;
    }

    // Shift compressed rotated files: N → N+1 (delete the oldest).
    for (size_t i = max_files_; i > 0; --i) {
        std::string src = rotated_path(i - 1);
        if (i == max_files_) {
            std::string gz = src + ".gz";
            std::remove(gz.c_str());
            std::remove(src.c_str());
            continue;
        }
        std::string gz        = src + ".gz";
        std::string target_gz = rotated_path(i) + ".gz";
        struct stat st;
        if (::stat(gz.c_str(), &st) == 0) {
            std::remove(target_gz.c_str());
            std::rename(gz.c_str(), target_gz.c_str());
        }
    }

    // Rename current → rotated.1, then gzip-compress.
    std::string rotated = rotated_path(1);
    std::remove(rotated.c_str());
    std::rename(log_path_.c_str(), rotated.c_str());
    gzip_compress_file(rotated);
}

std::string MetricsRegistry::rotated_path(size_t index) const
{
    return log_path_ + "." + std::to_string(index) + ".log";
}

} // namespace crowtree
