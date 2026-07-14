// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// Implementation of the crowtree logging facade (plan-tree #10). See log.h for
// the contract. Split into an spdlog-backed build and a no-op build so the Rust
// FFI `cc` build (no spdlog) still compiles this translation unit cleanly.
#include "crowtree/log.h"

#ifdef CROWTREE_HAVE_SPDLOG

#    include <spdlog/async_logger.h>
#    include <spdlog/details/thread_pool.h>
#    include <spdlog/sinks/rotating_file_sink.h>
#    include <spdlog/sinks/stdout_color_sinks.h>
#    include <spdlog/spdlog.h>

#    include <atomic>
#    include <exception>
#    include <memory>
#    include <mutex>

namespace crowtree
{
namespace
{
// Process-global enabled flag (see log.h lifetime notes). Relaxed is fine: the
// macros only need eventual visibility, and init/shutdown are not on a hot path.
std::atomic<bool> g_enabled{true};

// We OWN the async logger and its thread pool (rather than using spdlog's global
// registry/thread pool) so that shutdown can join the worker BEFORE the logger's
// sinks are destroyed. spdlog::shutdown() drops loggers first and joins the pool
// second, which lets an in-flight backend flush touch a freed sink — a teardown
// race TSan flags. Owning the pool lets us enforce the safe order. g_log_mu_
// serializes init/shutdown (never on the logging hot path).
std::mutex                                    g_log_mu;
std::shared_ptr<spdlog::logger>               g_logger;
std::shared_ptr<spdlog::details::thread_pool> g_tp;
} // namespace

bool logging_enabled()
{
    return g_enabled.load(std::memory_order_relaxed);
}

void shutdown_logging()
{
    std::lock_guard<std::mutex> lk(g_log_mu);
    if (!g_enabled.exchange(false)) {
        return; // never initialized (or already shut down)
    }
    spdlog::drop_all(); // release the registry's reference to our logger
    if (g_logger) {
        g_logger->flush(); // queue a flush ahead of the terminate messages
        g_logger.reset();  // drop our ref; in-flight worker msgs keep it alive
    }
    // Destroying the thread pool joins its worker(s) after draining the queue; the
    // logger + sinks are then released single-threaded (last worker_ptr), so no
    // worker can touch a sink after it is freed.
    g_tp.reset();
}

void init_logging(const std::string &log_dir, const std::string &level, size_t max_file_mb, size_t max_files)
{
    // Reset any prior logger so a fresh init (e.g. a second open() with a
    // different dir) rebinds cleanly.
    shutdown_logging();
    std::lock_guard<std::mutex> lk(g_log_mu);
    try {
        if (log_dir.empty()) {
            // No log dir configured: log to stderr so output is visible (tests, CLI).
            auto sink = std::make_shared<spdlog::sinks::stderr_color_sink_mt>();
            g_logger  = std::make_shared<spdlog::logger>("crowtree", sink);
            g_logger->set_pattern("[%l] %v");
            g_logger->set_level(spdlog::level::from_str(level));
            spdlog::set_default_logger(g_logger);
            g_enabled.store(true, std::memory_order_relaxed);
            return;
        }
        // Async logger over our own thread pool: fixed-size ring buffer, block on
        // overflow so no message is dropped under bursty load.
        g_tp                   = std::make_shared<spdlog::details::thread_pool>(8192, 1);
        const std::string path = log_dir + "/crowtree.log";
        auto sink = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(path, max_file_mb * 1024 * 1024, max_files);
        g_logger = std::make_shared<spdlog::async_logger>("crowtree", sink, g_tp, spdlog::async_overflow_policy::block);
        // YYYYMMDD-HHMMSS.mmm [tid] [level] [crowtree] message (aligns with the
        // Rust `tracing` format).
        g_logger->set_pattern("%Y%m%d-%H%M%S.%e [%t] [%l] [%n] %v");
        g_logger->set_level(spdlog::level::from_str(level));
        g_logger->flush_on(spdlog::level::warn);
        spdlog::set_default_logger(g_logger);
        g_enabled.store(true, std::memory_order_relaxed);
    }
    catch (const std::exception &) {
        // A logging failure must never take down the engine.
        g_logger.reset();
        g_tp.reset();
        g_enabled.store(false, std::memory_order_relaxed);
    }
}

} // namespace crowtree

#else // !CROWTREE_HAVE_SPDLOG — no-op build

namespace crowtree
{

bool logging_enabled()
{
    return false;
}

void shutdown_logging()
{
}

void init_logging(const std::string & /*log_dir*/, const std::string & /*level*/, size_t /*max_file_mb*/,
                  size_t /*max_files*/)
{
}

} // namespace crowtree

#endif // CROWTREE_HAVE_SPDLOG
