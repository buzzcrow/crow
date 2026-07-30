// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#include "crow-common/metrics.h"

#include "crow-common/gzip.h"
#include "crow-common/log.h"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <filesystem>
#include <utility>
#include <vector>

namespace crow::common
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

void MetricsRegistry::flush_to(FILE *fp, double window_secs, const char *timestamp, const char *section_label,
                               size_t width, size_t count_w, size_t tps_w)
{
    std::lock_guard<std::mutex> lock(flush_mutex_);

    // Global max name length across all sections (or override from caller).
    size_t name_w = width;
    if (name_w == 0) {
        name_w = max_name_len();
    }
    // Negotiated column widths (0 = use C++ defaults).
    size_t cw = count_w > 0 ? count_w : 5;
    size_t tw = tps_w > 0 ? tps_w : 7;

    std::fprintf(fp, "[%s %s window=%.3fs]\n", section_label, timestamp, window_secs);

    // Counters
    if (!counters_.empty()) {
        auto                                              idx = sorted_indices(counters_);
        std::vector<std::pair<size_t, Counter::Snapshot>> active;
        for (size_t i : idx) {
            auto snap = counters_[i]->flush();
            if (snap.count > 0) {
                active.emplace_back(i, snap);
            }
        }
        if (!active.empty()) {
            std::fprintf(fp, "%-*s  count  tps(/s)  total\n", static_cast<int>(name_w), "");
            for (const auto &[i, snap] : active) {
                double tps_d = static_cast<double>(snap.count) / window_secs;
                auto   tps   = static_cast<uint64_t>(tps_d);
                std::fprintf(fp, "%-*s  %*llu  %*llu  %6llu\n", static_cast<int>(name_w), counters_[i]->name().c_str(),
                             static_cast<int>(cw), static_cast<unsigned long long>(snap.count), static_cast<int>(tw),
                             static_cast<unsigned long long>(tps), static_cast<unsigned long long>(snap.total));
            }
        }
    }

    // Histograms
    if (!histograms_.empty()) {
        auto                                                       idx = sorted_indices(histograms_);
        std::vector<std::pair<size_t, LatencyHistogram::Snapshot>> active;
        for (size_t i : idx) {
            auto snap = histograms_[i]->flush();
            if (snap.count > 0) {
                active.emplace_back(i, snap);
            }
        }
        if (!active.empty()) {
            std::fprintf(fp, "%-*s  count  tps(/s)  avg(us)  p50  p99  max  total\n", static_cast<int>(name_w), "");
            for (const auto &[i, snap] : active) {
                uint64_t p50   = LatencyHistogram::percentile(snap, 50.0);
                uint64_t p99   = LatencyHistogram::percentile(snap, 99.0);
                uint64_t avg   = snap.count > 0 ? snap.sum / snap.count : 0;
                double   tps_d = static_cast<double>(snap.count) / window_secs;
                auto     tps   = static_cast<uint64_t>(tps_d);
                std::fprintf(fp, "%-*s  %*llu  %*llu  %7llu  %4llu  %4llu  %7llu  %5llu\n", static_cast<int>(name_w),
                             histograms_[i]->name().c_str(), static_cast<int>(cw),
                             static_cast<unsigned long long>(snap.count), static_cast<int>(tw),
                             static_cast<unsigned long long>(tps), static_cast<unsigned long long>(avg / 1000),
                             static_cast<unsigned long long>(p50 / 1000), static_cast<unsigned long long>(p99 / 1000),
                             static_cast<unsigned long long>(snap.sum / snap.count),
                             static_cast<unsigned long long>(snap.total_count));
            }
        }
    }

    // Summaries
    if (!summaries_.empty()) {
        auto                                                     idx = sorted_indices(summaries_);
        std::vector<std::pair<size_t, LatencySummary::Snapshot>> active;
        for (size_t i : idx) {
            auto snap = summaries_[i]->flush();
            if (snap.count > 0) {
                active.emplace_back(i, snap);
            }
        }
        if (!active.empty()) {
            std::fprintf(fp, "%-*s  count  tps(/s)  avg(us)  max(us)  total\n", static_cast<int>(name_w), "");
            for (const auto &[i, snap] : active) {
                uint64_t avg   = snap.count > 0 ? snap.sum / snap.count : 0;
                double   tps_d = static_cast<double>(snap.count) / window_secs;
                auto     tps   = static_cast<uint64_t>(tps_d);
                std::fprintf(fp, "%-*s  %*llu  %*llu  %7llu  %7llu  %5llu\n", static_cast<int>(name_w),
                             summaries_[i]->name().c_str(), static_cast<int>(cw),
                             static_cast<unsigned long long>(snap.count), static_cast<int>(tw),
                             static_cast<unsigned long long>(tps), static_cast<unsigned long long>(avg / 1000),
                             static_cast<unsigned long long>(snap.max / 1000),
                             static_cast<unsigned long long>(snap.total_count));
            }
        }
    }

    // Bandwidths
    if (!bandwidths_.empty()) {
        auto                                                idx = sorted_indices(bandwidths_);
        std::vector<std::pair<size_t, Bandwidth::Snapshot>> active;
        for (size_t i : idx) {
            auto snap = bandwidths_[i]->flush();
            if (snap.count > 0) {
                active.emplace_back(i, snap);
            }
        }
        if (!active.empty()) {
            std::fprintf(fp, "%-*s  count  tps(/s)  avg_size(KB)  rate(KB/s)  total(KB)\n", static_cast<int>(name_w),
                         "");
            for (const auto &[i, snap] : active) {
                uint64_t avg_size = snap.count > 0 ? snap.sum / snap.count : 0;
                double   rate_d   = static_cast<double>(snap.sum) / window_secs;
                auto     rate     = static_cast<uint64_t>(rate_d);
                double   tps_d    = static_cast<double>(snap.count) / window_secs;
                auto     tps      = static_cast<uint64_t>(tps_d);
                std::fprintf(fp, "%-*s  %*llu  %*llu  %12llu  %10llu  %9llu\n", static_cast<int>(name_w),
                             bandwidths_[i]->name().c_str(), static_cast<int>(cw),
                             static_cast<unsigned long long>(snap.count), static_cast<int>(tw),
                             static_cast<unsigned long long>(tps), static_cast<unsigned long long>(avg_size / 1024),
                             static_cast<unsigned long long>(rate / 1024),
                             static_cast<unsigned long long>(snap.total_bytes / 1024));
            }
        }
    }

    // Gauges (always printed, even if 0)
    if (!gauges_.empty()) {
        std::fprintf(fp, "%-*s  value\n", static_cast<int>(name_w), "");
        auto idx = sorted_indices(gauges_);
        for (size_t i : idx) {
            std::fprintf(fp, "%-*s  %5llu\n", static_cast<int>(name_w), gauges_[i]->name().c_str(),
                         static_cast<unsigned long long>(gauges_[i]->get()));
        }
    }

    std::fprintf(fp, "\n");
}

size_t MetricsRegistry::max_name_len() const
{
    size_t max_len = 0;
    for (const auto &e : counters_) {
        max_len = std::max(max_len, e->name().size());
    }
    for (const auto &e : histograms_) {
        max_len = std::max(max_len, e->name().size());
    }
    for (const auto &e : summaries_) {
        max_len = std::max(max_len, e->name().size());
    }
    for (const auto &e : bandwidths_) {
        max_len = std::max(max_len, e->name().size());
    }
    for (const auto &e : gauges_) {
        max_len = std::max(max_len, e->name().size());
    }
    return max_len;
}

void MetricsRegistry::start(const std::string &log_path, double interval_secs, size_t max_file_mb, size_t max_files)
{
    log_path_       = log_path;
    interval_secs_  = interval_secs;
    max_file_bytes_ = max_file_mb * 1024 * 1024;
    max_files_      = max_files;
    running_.store(true, std::memory_order_relaxed);
    flush_thread_ = std::thread([this]() {
        set_current_thread_name("ct-metrics");
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
    flush_to(fp, interval_secs_, ts.c_str(), "metrics", 0);
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

    // Rename current → <base>.YYYYMMDD-HHMMSS.log, then gzip-compress.
    const auto now = std::time(nullptr);
    std::tm    tm_buf{};
    gmtime_r(&now, &tm_buf);
    std::array<char, 128> ts{};
    std::snprintf(ts.data(), ts.size(), "%04d%02d%02d-%02d%02d%02d", tm_buf.tm_year + 1900, tm_buf.tm_mon + 1,
                  tm_buf.tm_mday, tm_buf.tm_hour, tm_buf.tm_min, tm_buf.tm_sec);
    std::string rotated = log_path_ + "." + ts.data() + ".log";
    std::remove(rotated.c_str());
    std::rename(log_path_.c_str(), rotated.c_str());
    gzip_compress_file(rotated);

    // Delete oldest rotated files beyond max_files_.
    prune_rotated();
}

void MetricsRegistry::prune_rotated()
{
    namespace fs = std::filesystem;
    fs::path    base(log_path_);
    fs::path    dir    = base.parent_path().empty() ? fs::current_path() : base.parent_path();
    std::string prefix = base.filename().string() + ".";

    std::vector<fs::path> rotated_files;
    std::error_code       ec;
    for (const auto &entry : fs::directory_iterator(dir, ec)) {
        if (!entry.is_regular_file()) {
            continue;
        }
        std::string name = entry.path().filename().string();
        if (name.size() <= prefix.size() || !name.starts_with(prefix)) {
            continue;
        }
        if (name.size() < 7 || !name.ends_with(".log.gz")) {
            continue;
        }
        rotated_files.push_back(entry.path());
    }
    std::ranges::sort(rotated_files, std::greater<>());
    for (size_t i = max_files_; i < rotated_files.size(); ++i) {
        std::error_code rm_ec;
        fs::remove(rotated_files[i], rm_ec);
    }
}

} // namespace crow::common
