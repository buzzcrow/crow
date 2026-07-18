// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#include "crowtree/compressing_sink.h"

#ifdef CROWTREE_HAVE_SPDLOG

#    include "crowtree/gzip.h"

#    include <spdlog/details/os.h>

#    include <cstdio>
#    include <cstring>
#    include <utility>

namespace crowtree
{

// ── compressing_file_sink ───────────────────────────────────────

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
    return base_filename + "." + std::to_string(index) + ".log";
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

    // Shift compressed rotated files: N → N+1 (delete the oldest).
    for (auto i = max_files_; i > 0; --i) {
        std::string src = calc_filename(base_filename_, i - 1);
        if (i == max_files_) {
            // The oldest slot: delete both .log and .log.gz if they exist.
            std::string gz = src + ".gz";
            std::remove(gz.c_str());
            std::remove(src.c_str());
            continue;
        }
        std::string gz        = src + ".gz";
        std::string target_gz = calc_filename(base_filename_, i) + ".gz";
        if (spdlog::details::os::path_exists(gz)) {
            std::remove(target_gz.c_str());
            spdlog::details::os::rename(gz, target_gz);
        }
    }

    // Compress the just-closed current file (index 0) → index 1.
    std::string current = calc_filename(base_filename_, 0);
    std::string rotated = calc_filename(base_filename_, 1);
    if (spdlog::details::os::path_exists(current)) {
        // Rename current → rotated, then compress rotated → rotated.gz.
        std::remove(rotated.c_str());
        spdlog::details::os::rename(current, rotated);
        gzip_compress_file(rotated);
    }

    file_helper_.reopen(true);
    current_size_ = 0;
}

// Explicit instantiations.
template class compressing_file_sink<std::mutex>;
template class compressing_file_sink<spdlog::details::null_mutex>;

} // namespace crowtree

#endif // CROWTREE_HAVE_SPDLOG
