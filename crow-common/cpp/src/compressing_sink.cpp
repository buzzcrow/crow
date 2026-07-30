// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#include "crow-common/compressing_sink.h"

#ifdef CROW_HAVE_SPDLOG

#    include "crow-common/gzip.h"

#    include <spdlog/details/os.h>

#    include <algorithm>
#    include <array>
#    include <cstdio>
#    include <cstring>
#    include <ctime>
#    include <filesystem>
#    include <utility>
#    include <vector>

namespace crow::common
{

// ── compressing_file_sink ───────────────────────────────────────

// Generate a UTC timestamp string in YYYYMMDD-HHMMSS format.
static std::string utc_timestamp_secs()
{
    const auto now = std::time(nullptr);
    std::tm    tm_buf{};
    gmtime_r(&now, &tm_buf);
    std::array<char, 128> buf{};
    std::snprintf(buf.data(), buf.size(), "%04d%02d%02d-%02d%02d%02d", tm_buf.tm_year + 1900, tm_buf.tm_mon + 1,
                  tm_buf.tm_mday, tm_buf.tm_hour, tm_buf.tm_min, tm_buf.tm_sec);
    return {buf.data()};
}

template <typename Mutex>
compressing_file_sink<Mutex>::compressing_file_sink(std::string base_filename, std::size_t max_size,
                                                    std::size_t max_files)
    : base_filename_(std::move(base_filename)),
      max_size_(max_size),
      max_files_(max_files)
{
    if (max_size == 0) {
        spdlog::throw_spdlog_ex("compressing_file_sink: max_size cannot be zero");
    }
    file_helper_.open(calc_filename(base_filename_, 0));
    current_size_ = file_helper_.size();
}

template <typename Mutex>
std::string compressing_file_sink<Mutex>::calc_filename(const std::string &base_filename, std::size_t index)
{
    if (index == 0) {
        return base_filename;
    }
    return base_filename + "." + utc_timestamp_secs() + ".log";
}

template <typename Mutex> void compressing_file_sink<Mutex>::sink_it_(const spdlog::details::log_msg &msg)
{
    spdlog::memory_buf_t formatted;
    this->formatter_->format(msg, formatted);
    auto new_size = current_size_ + formatted.size();
    if (new_size > max_size_) {
        file_helper_.flush();
        if (file_helper_.size() > 0) {
            rotate_();
            new_size = formatted.size();
        }
    }
    file_helper_.write(formatted);
    current_size_ = new_size;
}

template <typename Mutex> void compressing_file_sink<Mutex>::flush_()
{
    file_helper_.flush();
}

template <typename Mutex> void compressing_file_sink<Mutex>::rotate_() // NOLINT(readability-identifier-naming)
{
    file_helper_.close();

    // Rename current → <base>.YYYYMMDD-HHMMSS.log, then gzip-compress.
    std::string current = calc_filename(base_filename_, 0);
    std::string rotated = calc_filename(base_filename_, 1);
    if (spdlog::details::os::path_exists(current)) {
        std::remove(rotated.c_str());
        spdlog::details::os::rename(current, rotated);
        gzip_compress_file(rotated);
    }

    // Delete oldest rotated files beyond max_files_.
    prune_rotated();

    file_helper_.reopen(true);
    current_size_ = 0;
}

template <typename Mutex> void compressing_file_sink<Mutex>::prune_rotated()
{
    namespace fs = std::filesystem;
    fs::path    base(base_filename_);
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
        // Only keep .log.gz files (compressed rotated files).
        if (name.size() < 7 || !name.ends_with(".log.gz")) {
            continue;
        }
        rotated_files.push_back(entry.path());
    }
    // Sort descending (newest first) — timestamps sort chronologically.
    std::ranges::sort(rotated_files, std::greater<>());
    for (size_t i = max_files_; i < rotated_files.size(); ++i) {
        std::error_code rm_ec;
        fs::remove(rotated_files[i], rm_ec);
    }
}

// Explicit instantiations.
template class compressing_file_sink<std::mutex>;
template class compressing_file_sink<spdlog::details::null_mutex>;

} // namespace crow::common

#endif // CROW_HAVE_SPDLOG
