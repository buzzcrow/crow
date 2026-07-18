// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// Custom spdlog sink: size-based rotation with gzip compression of
// rotated files. Used by both the crowtree service log and the crowtree
// metrics log.
//
// When the current log file exceeds `max_size` bytes, it is closed,
// renamed to `<base>.YYYYMMDD-HHMMSS.log`, gzip-compressed to
// `<base>.YYYYMMDD-HHMMSS.log.gz`, and a new current file is opened.
// At most `max_files` compressed rotated files are kept; older ones
// are deleted.
#pragma once

#ifdef CROWTREE_HAVE_SPDLOG

#    include <spdlog/details/file_helper.h>
#    include <spdlog/details/null_mutex.h>
#    include <spdlog/sinks/base_sink.h>

#    include <cstddef>
#    include <mutex>
#    include <string>

namespace crowtree
{

// spdlog sink: rotating file with gzip compression of rotated files.
// Thread-safe (inherits base_sink's mutex).
template <typename Mutex> class compressing_file_sink final : public spdlog::sinks::base_sink<Mutex>
{
  public:
    compressing_file_sink(std::string base_filename, std::size_t max_size, std::size_t max_files);

    static std::string calc_filename(const std::string &base_filename, std::size_t index);

  protected:
    void sink_it_(const spdlog::details::log_msg &msg) override;
    void flush_() override;

  private:
    void rotate_(); // NOLINT(readability-identifier-naming) — matches spdlog convention
    void prune_rotated();

    std::string                  base_filename_;
    std::size_t                  max_size_;
    std::size_t                  max_files_;
    std::size_t                  current_size_{0};
    spdlog::details::file_helper file_helper_;
};

using compressing_file_sink_mt = compressing_file_sink<std::mutex>;
using compressing_file_sink_st = compressing_file_sink<spdlog::details::null_mutex>;

} // namespace crowtree

#endif // CROWTREE_HAVE_SPDLOG
