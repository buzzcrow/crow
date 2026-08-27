// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// Crowtree logging facade (plan-tree #10).
//
// The engine logs through the CR_LOG_* macros below. When the library is built
// with spdlog (the CMake build defines CROW_HAVE_SPDLOG), these expand to an
// async, rotating-file logger. In builds without spdlog — notably the Rust FFI
// `cc` build, where the Rust side already has its own `tracing` — every macro is
// a zero-cost no-op and the init/shutdown entry points do nothing.
//
// Lifetime: logging is process-global (a single spdlog default logger).
// The application should call init_logging() once at process startup (before
// any Crowtree::open()) and shutdown_logging() once at process exit (after all
// Crowtree instances are destroyed). flush_logging() may be called at any time
// to push buffered messages to disk without stopping the logger.
//
// It is intentionally NOT called from ~Crowtree or Crowtree::open(), so
// multiple Crowtree instances in one process share one logger without
// tearing each other's down; init_logging() is idempotent-safe and simply
// resets any previous logger.
#pragma once

#include <cstddef>
#include <string>

namespace crow::common
{

// Initialize an async, size-rotating file logger writing to
// `<log_dir>/<file_prefix>-<YYYYMMDD-HHMMSS.mmm>-<pid>.log`. Rotated files are
// gzip-compressed. `level` is one of trace/debug/info/warn/error/off
// (spdlog names). No-op if `log_dir` is empty or the library was built without
// spdlog. Any failure to open the file leaves logging disabled (never throws).
void init_logging(const std::string &log_dir, const std::string &level = "info", size_t max_file_mb = 30,
                  size_t max_files = 5, const std::string &file_prefix = "crow-tree");

// Add an additional file sink to the existing logger created by
// init_logging. Messages go to both the original file and this new file.
// No-op (never throws) if logging was never initialized. Used by
// crow-rpc to get its own log file alongside the crow-tree log file.
void add_log_file(const std::string &log_dir, size_t max_file_mb, size_t max_files, const std::string &file_prefix);

// Flush buffered messages to the sink without stopping the logger.
// Safe to call when uninitialized or already shut down (no-op).
void flush_logging();

// Flush and stop the logger (joins the async thread). Safe to call when
// uninitialized and safe to call more than once.
void shutdown_logging();

// True once init_logging() has succeeded and shutdown_logging() has not run.
// Cheap (a single relaxed atomic load); used to gate the CR_LOG_* macros so
// nothing is emitted before logging is configured.
[[nodiscard]] bool logging_enabled();

// True only after init_logging() has successfully created a logger.
// Distinct from logging_enabled() (which defaults to true even before
// init_logging is called, so spdlog's default stderr logger is used).
[[nodiscard]] bool logger_initialized();

// Set the current thread's name for CT_LOG output (stored thread_local and
// also passed to pthread_setname_np for debugger/htop visibility). Should be
// called at the start of each engine thread's body.
void set_current_thread_name(const char *name);

} // namespace crow::common

#ifdef CROW_HAVE_SPDLOG
#    include <spdlog/spdlog.h>

// Each macro first checks the runtime enabled flag (no output before
// init_logging), then defers to spdlog's own compile-time level filtering
// (SPDLOG_ACTIVE_LEVEL) and runtime level. Args use fmt formatting, e.g.
// CR_LOG_INFO("open iu={} frame={}", iu, frame_bytes).
#    define CR_LOG_ERROR(...)                        \
        do {                                         \
            if (::crow::common::logging_enabled()) { \
                SPDLOG_ERROR(__VA_ARGS__);           \
            }                                        \
        } while (0)
#    define CR_LOG_WARN(...)                         \
        do {                                         \
            if (::crow::common::logging_enabled()) { \
                SPDLOG_WARN(__VA_ARGS__);            \
            }                                        \
        } while (0)
#    define CR_LOG_INFO(...)                         \
        do {                                         \
            if (::crow::common::logging_enabled()) { \
                SPDLOG_INFO(__VA_ARGS__);            \
            }                                        \
        } while (0)
#    define CR_LOG_DEBUG(...)                        \
        do {                                         \
            if (::crow::common::logging_enabled()) { \
                SPDLOG_DEBUG(__VA_ARGS__);           \
            }                                        \
        } while (0)
#    define CR_LOG_TRACE(...)                        \
        do {                                         \
            if (::crow::common::logging_enabled()) { \
                SPDLOG_TRACE(__VA_ARGS__);           \
            }                                        \
        } while (0)

#else // !CROW_HAVE_SPDLOG — zero-cost no-ops

#    define CR_LOG_ERROR(...) \
        do {                  \
        } while (0)
#    define CR_LOG_WARN(...) \
        do {                 \
        } while (0)
#    define CR_LOG_INFO(...) \
        do {                 \
        } while (0)
#    define CR_LOG_DEBUG(...) \
        do {                  \
        } while (0)
#    define CR_LOG_TRACE(...) \
        do {                  \
        } while (0)

#endif // CROW_HAVE_SPDLOG
