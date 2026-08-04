// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// Implementation of the crow-tree logging facade (plan-tree #10). See log.h for
// the contract. Split into an spdlog-backed build and a no-op build so the Rust
// FFI `cc` build (no spdlog) still compiles this translation unit cleanly.
#include "crow-common/log.h"

#ifdef CROW_HAVE_SPDLOG

#    include "crow-common/compressing_sink.h"

#    include <pthread.h>
#    include <spdlog/async_logger.h>
#    include <spdlog/details/thread_pool.h>
#    include <spdlog/pattern_formatter.h>
#    include <spdlog/sinks/stdout_color_sinks.h>
#    include <spdlog/spdlog.h>
#    include <unistd.h>

#    include <array>
#    include <atomic>
#    include <exception>
#    include <memory>
#    include <mutex>
#    include <string>
#    include <unordered_map>

namespace crow::common
{
namespace
{
// Process-global enabled flag (see log.h lifetime notes). Relaxed is fine: the
// macros only need eventual visibility, and init/shutdown are not on a hot path.
std::atomic<bool> g_enabled{true};

} // namespace

std::mutex                              g_thread_names_mu;
std::unordered_map<size_t, std::string> g_thread_names;

void set_current_thread_name(const char *name)
{
    size_t                      tid = spdlog::details::os::thread_id();
    std::lock_guard<std::mutex> lk(g_thread_names_mu);
    g_thread_names[tid] = name;
#    if defined(__APPLE__)
    pthread_setname_np(name);
#    elif defined(__linux__)
    pthread_setname_np(pthread_self(), name);
#    endif
}

namespace
{
class thread_name_flag : public spdlog::custom_flag_formatter
{
  public:
    void format(const spdlog::details::log_msg &msg, const std::tm & /*tm*/, spdlog::memory_buf_t &dest) override
    {
        std::lock_guard<std::mutex> lk(g_thread_names_mu);
        if (auto it = g_thread_names.find(msg.thread_id); it != g_thread_names.end()) {
            dest.append(it->second.data(), it->second.data() + it->second.size());
        }
    }

    [[nodiscard]] std::unique_ptr<custom_flag_formatter> clone() const override
    {
        return std::make_unique<thread_name_flag>();
    }
};

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

void flush_logging()
{
    std::lock_guard<std::mutex> lk(g_log_mu);
    if (g_logger) {
        g_logger->flush();
    }
}

void shutdown_logging()
{
    std::lock_guard<std::mutex> lk(g_log_mu);
    if (!g_enabled.exchange(false)) {
        return; // never initialized (or already shut down)
    }
    if (g_logger) {
        g_logger->flush(); // queue a flush so buffered messages reach the sink
    }
    spdlog::drop_all(); // release the registry's reference to our logger
    if (g_logger) {
        g_logger.reset(); // drop our ref; in-flight worker msgs keep it alive
    }
    // Destroying the thread pool joins its worker(s) after draining the queue; the
    // logger + sinks are then released single-threaded (last worker_ptr), so no
    // worker can touch a sink after it is freed.
    g_tp.reset();
}

void init_logging(const std::string &log_dir, const std::string &level, size_t max_file_mb, size_t max_files,
                  const std::string &file_prefix)
{
    // Reset any prior logger so a fresh init (e.g. a second open() with a
    // different dir) rebinds cleanly.
    shutdown_logging();
    std::lock_guard<std::mutex> lk(g_log_mu);
    try {
        if (log_dir.empty()) {
            // No log dir configured: log to stderr so output is visible (tests, CLI).
            auto sink = std::make_shared<spdlog::sinks::stderr_color_sink_mt>();
            g_logger  = std::make_shared<spdlog::logger>("crow-tree", sink);
            auto stderr_fmt =
                std::make_unique<spdlog::pattern_formatter>("[%l] [%n] %v", spdlog::pattern_time_type::utc);
            stderr_fmt->add_flag<thread_name_flag>('@');
            g_logger->set_formatter(std::move(stderr_fmt));
            g_logger->set_level(spdlog::level::from_str(level));
            spdlog::set_default_logger(g_logger);
            g_enabled.store(true, std::memory_order_relaxed);
            return;
        }
        // Async logger over our own thread pool: fixed-size ring buffer, block on
        // overflow so no message is dropped under bursty load.
        g_tp = std::make_shared<spdlog::details::thread_pool>(8192, 1);
        // File name: <prefix>-{YYYYMMDD-HHMMSS.mmm}-{pid}.log (matches the
        // Rust server's crowkv-server-<ts>-<pid>.log convention so logs from
        // the same process can be correlated).
        const auto now    = std::chrono::system_clock::now();
        const auto t_time = std::chrono::system_clock::to_time_t(now);
        std::tm    tm_buf{};
        gmtime_r(&t_time, &tm_buf);
        const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count() % 1000;
        std::array<char, 128> ts{};
        std::snprintf(ts.data(), ts.size(), "%04d%02d%02d-%02d%02d%02d.%03lld", tm_buf.tm_year + 1900,
                      tm_buf.tm_mon + 1, tm_buf.tm_mday, tm_buf.tm_hour, tm_buf.tm_min, tm_buf.tm_sec,
                      static_cast<long long>(ms));
        const std::string prefix = file_prefix.empty() ? "crow-tree" : file_prefix;
        const std::string path   = log_dir + "/" + prefix + "-" + ts.data() + "-" + std::to_string(::getpid()) + ".log";
        auto              sink = std::make_shared<compressing_file_sink_mt>(path, max_file_mb * 1024 * 1024, max_files);
        g_logger =
            std::make_shared<spdlog::async_logger>("crow-tree", sink, g_tp, spdlog::async_overflow_policy::block);
        // YYYYMMDD-HHMMSS.mmm [thread] [level] [crow-tree] message
        // (PID is in the filename; thread name via custom flag; all timestamps UTC).
        auto formatter = std::make_unique<spdlog::pattern_formatter>("%Y%m%d-%H%M%S.%e [@] [%l] [%n] %v",
                                                                     spdlog::pattern_time_type::utc);
        formatter->add_flag<thread_name_flag>('@');
        g_logger->set_formatter(std::move(formatter));
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

} // namespace crow::common

#else // !CROW_HAVE_SPDLOG — no-op build

namespace crow::common
{

bool logging_enabled()
{
    return false;
}

void flush_logging()
{
}

void shutdown_logging()
{
}

void init_logging(const std::string & /*log_dir*/, const std::string & /*level*/, size_t /*max_file_mb*/,
                  size_t /*max_files*/, const std::string & /*file_prefix*/)
{
}

void set_current_thread_name(const char * /*name*/)
{
}

} // namespace crow::common

#endif // CROW_HAVE_SPDLOG
