// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-common/metrics/metrics.h"

#include "crowdb-common/gzip.h"
#include "crowdb-common/log.h"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <filesystem>
#include <utility>
#include <vector>

namespace crowdb::common::metrics
{

// ── MetricsRegistry ─────────────────────────────────────────────

MetricsRegistry &MetricsRegistry::global()
{
    static MetricsRegistry instance;
    return instance;
}

MetricsRegistry::~MetricsRegistry() // NOLINT(bugprone-exception-escape)
{
    stop();
}

Counter *MetricsRegistry::register_counter(const std::string &name)
{
    std::scoped_lock lock(flush_mutex_);
    auto             it = std::find_if(counters_.begin(), counters_.end(),
                                       [&](const std::unique_ptr<Counter> &e) { return e->name() == name; });
    if (it != counters_.end()) {
        *it = std::make_unique<Counter>(name);
        return it->get();
    }
    auto     h   = std::make_unique<Counter>(name);
    Counter *raw = h.get();
    counters_.push_back(std::move(h));
    return raw;
}

Gauge *MetricsRegistry::register_gauge(const std::string &name)
{
    std::scoped_lock lock(flush_mutex_);
    auto             it = std::find_if(gauges_.begin(), gauges_.end(),
                                       [&](const std::unique_ptr<Gauge> &e) { return e->name() == name; });
    if (it != gauges_.end()) {
        *it = std::make_unique<Gauge>(name);
        return it->get();
    }
    auto   h   = std::make_unique<Gauge>(name);
    Gauge *raw = h.get();
    gauges_.push_back(std::move(h));
    return raw;
}

CallbackGauge *MetricsRegistry::register_callback_gauge(const std::string &name, CallbackGauge::Callback cb)
{
    std::scoped_lock lock(flush_mutex_);
    auto             it = std::find_if(callback_gauges_.begin(), callback_gauges_.end(),
                                       [&](const std::unique_ptr<CallbackGauge> &e) { return e->name() == name; });
    if (it != callback_gauges_.end()) {
        *it = std::make_unique<CallbackGauge>(name, std::move(cb));
        return it->get();
    }
    auto           h   = std::make_unique<CallbackGauge>(name, std::move(cb));
    CallbackGauge *raw = h.get();
    callback_gauges_.push_back(std::move(h));
    return raw;
}

Bandwidth *MetricsRegistry::register_bandwidth(const std::string &name)
{
    std::scoped_lock lock(flush_mutex_);
    auto             it = std::find_if(bandwidths_.begin(), bandwidths_.end(),
                                       [&](const std::unique_ptr<Bandwidth> &e) { return e->name() == name; });
    if (it != bandwidths_.end()) {
        *it = std::make_unique<Bandwidth>(name);
        return it->get();
    }
    auto       h   = std::make_unique<Bandwidth>(name);
    Bandwidth *raw = h.get();
    bandwidths_.push_back(std::move(h));
    return raw;
}

LatencyHistogram *MetricsRegistry::register_histogram(const std::string &name)
{
    std::scoped_lock lock(flush_mutex_);
    auto             it = std::find_if(histograms_.begin(), histograms_.end(),
                                       [&](const std::unique_ptr<LatencyHistogram> &e) { return e->name() == name; });
    if (it != histograms_.end()) {
        *it = std::make_unique<LatencyHistogram>(name);
        return it->get();
    }
    auto              h   = std::make_unique<LatencyHistogram>(name);
    LatencyHistogram *raw = h.get();
    histograms_.push_back(std::move(h));
    return raw;
}

LatencySummary *MetricsRegistry::register_summary(const std::string &name)
{
    std::scoped_lock lock(flush_mutex_);
    auto             it = std::find_if(summaries_.begin(), summaries_.end(),
                                       [&](const std::unique_ptr<LatencySummary> &e) { return e->name() == name; });
    if (it != summaries_.end()) {
        *it = std::make_unique<LatencySummary>(name);
        return it->get();
    }
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
    std::scoped_lock lock(flush_mutex_);

    // Global max name length across all sections (or override from caller).
    size_t name_w = width;
    if (name_w == 0) {
        name_w = max_name_len();
    }
    // Negotiated column widths (0 = use C++ defaults).
    size_t cw = count_w > 0 ? count_w : 5;
    size_t tw = tps_w > 0 ? tps_w : 7;

    std::fprintf(fp, "[%s %s window=%.3fs]\n", section_label, timestamp, window_secs);

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
            std::fprintf(fp, "%-*s  count  tps(/s)  avg_size(MB)  max(MB)  rate(MB/s)  total(MB)\n",
                         static_cast<int>(name_w), "");
            for (const auto &[i, snap] : active) {
                uint64_t avg_size = snap.count > 0 ? snap.sum / snap.count : 0;
                double   rate_d   = static_cast<double>(snap.sum) / window_secs;
                auto     rate     = static_cast<uint64_t>(rate_d);
                double   tps_d    = static_cast<double>(snap.count) / window_secs;
                auto     tps      = static_cast<uint64_t>(tps_d);
                std::fprintf(fp, "%-*s  %*llu  %*llu  %12.2f  %7.2f  %10.2f  %9.2f\n", static_cast<int>(name_w),
                             bandwidths_[i]->name().c_str(), static_cast<int>(cw),
                             static_cast<unsigned long long>(snap.count), static_cast<int>(tw),
                             static_cast<unsigned long long>(tps),
                             static_cast<double>(avg_size) / (1024.0 * 1024.0),
                             static_cast<double>(snap.max_bytes) / (1024.0 * 1024.0),
                             static_cast<double>(rate) / (1024.0 * 1024.0),
                             static_cast<double>(snap.total_bytes) / (1024.0 * 1024.0));
            }
        }
    }

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

    // Gauges (always printed, even if 0). Regular gauges and callback
    // gauges are merged into one sorted list for a unified output section.
    if (!gauges_.empty() || !callback_gauges_.empty()) {
        // Collect (name, value) pairs from both gauge types, then sort.
        std::vector<std::pair<std::string, uint64_t>> all_gauges;
        all_gauges.reserve(gauges_.size() + callback_gauges_.size());
        for (const auto &g : gauges_) {
            all_gauges.emplace_back(g->name(), g->get());
        }
        for (const auto &g : callback_gauges_) {
            all_gauges.emplace_back(g->name(), g->get());
        }
        std::sort(all_gauges.begin(), all_gauges.end(), [](const auto &a, const auto &b) { return a.first < b.first; });
        std::fprintf(fp, "%-*s  value\n", static_cast<int>(name_w), "");
        for (const auto &[name, value] : all_gauges) {
            std::fprintf(fp, "%-*s  %5llu\n", static_cast<int>(name_w), name.c_str(),
                         static_cast<unsigned long long>(value));
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
    for (const auto &e : callback_gauges_) {
        max_len = std::max(max_len, e->name().size());
    }
    return max_len;
}

void MetricsRegistry::start(const std::string &log_path, double interval_secs, size_t max_file_mb, size_t max_files,
                            bool console)
{
    log_path_       = log_path;
    interval_secs_  = interval_secs;
    max_file_bytes_ = max_file_mb * 1024 * 1024;
    max_files_      = max_files;
    console_        = console;
    running_.store(true, std::memory_order_relaxed);
    flush_thread_ = std::thread([this]() {
        set_current_thread_name("ct-metrics");
        std::unique_lock<std::mutex> lk(flush_mutex_);
        while (running_.load(std::memory_order_relaxed)) {
            stop_cv_.wait_for(lk, std::chrono::milliseconds(static_cast<int>(interval_secs_ * 1000)),
                              [this] { return !running_.load(std::memory_order_relaxed); });
            if (!running_.load(std::memory_order_relaxed)) {
                break;
            }
            lk.unlock();
            flush_to_file();
            lk.lock();
        }
    });
}

void MetricsRegistry::stop()
{
    if (!running_.exchange(false, std::memory_order_relaxed)) {
        return;
    }
    {
        std::scoped_lock lk(flush_mutex_);
    }
    stop_cv_.notify_all();
    if (flush_thread_.joinable()) {
        flush_thread_.join();
    }
    flush_to_file();
}

void MetricsRegistry::flush_to_file()
{
    // Check if rotation is needed before writing.
    check_rotate();

    std::string ts = iso8601_now();

    // Flush once to a memory buffer, then write to both file and
    // stdout. This avoids double-flushing (which would reset the
    // metric windows on the first flush, leaving nothing for the
    // second).
    char  *buf = nullptr;
    size_t len = 0;
    FILE  *mem = open_memstream(&buf, &len);
    if (mem == nullptr) {
        return;
    }
    flush_to(mem, interval_secs_, ts.c_str(), "metrics", 0);
    std::fflush(mem);
    std::fclose(mem);

    if (len == 0) {
        free(buf);
        return;
    }

    // Write to file.
    FILE *fp = std::fopen(log_path_.c_str(), "a");
    if (fp != nullptr) {
        std::fwrite(buf, 1, len, fp);
        std::fflush(fp);
        std::fclose(fp);
    }

    // Also write to stdout when console mode is enabled.
    if (console_) {
        std::fwrite(buf, 1, len, stdout);
        std::fflush(stdout);
    }

    free(buf);
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

} // namespace crowdb::common::metrics
