// Crowtree logging facade (plan-tree #10).
//
// The engine logs through the CT_LOG_* macros below. When the library is built
// with spdlog (the CMake build defines CROWTREE_HAVE_SPDLOG), these expand to an
// async, rotating-file logger. In builds without spdlog — notably the Rust FFI
// `cc` build, where the Rust side already has its own `tracing` — every macro is
// a zero-cost no-op and the init/shutdown entry points do nothing.
//
// Lifetime: logging is process-global (a single spdlog default logger). It is
// (re)initialized by init_logging() (Crowtree::open() calls it when
// Options::log_dir is set) and must be torn down explicitly by the application
// via shutdown_logging() (flush + join). It is intentionally NOT stopped in
// ~Crowtree, so multiple Crowtree instances in one process share one logger
// rather than tearing each other's down; init_logging() is idempotent-safe and
// simply resets any previous logger.
#pragma once

#include <cstddef>
#include <string>

namespace crowtree {

// Initialize an async, size-rotating file logger writing to
// `<log_dir>/crowtree.log`. `level` is one of trace/debug/info/warn/error/off
// (spdlog names). No-op if `log_dir` is empty or the library was built without
// spdlog. Any failure to open the file leaves logging disabled (never throws).
void init_logging(const std::string& log_dir, const std::string& level = "info",
                  size_t max_file_mb = 100, size_t max_files = 5);

// Flush and stop the logger (joins the async thread). Safe to call when
// uninitialized and safe to call more than once.
void shutdown_logging();

// True once init_logging() has succeeded and shutdown_logging() has not run.
// Cheap (a single relaxed atomic load); used to gate the CT_LOG_* macros so
// nothing is emitted before logging is configured.
bool logging_enabled();

}  // namespace crowtree

#ifdef CROWTREE_HAVE_SPDLOG
#include <spdlog/spdlog.h>

// Each macro first checks the runtime enabled flag (no output before
// init_logging), then defers to spdlog's own compile-time level filtering
// (SPDLOG_ACTIVE_LEVEL) and runtime level. Args use fmt formatting, e.g.
// CT_LOG_INFO("open iu={} frame={}", iu, frame_bytes).
#define CT_LOG_ERROR(...)                            \
  do {                                               \
    if (::crowtree::logging_enabled())               \
    {                                                \
      SPDLOG_ERROR(__VA_ARGS__);                     \
    }                                                \
  } while (0)
#define CT_LOG_WARN(...)                             \
  do {                                               \
    if (::crowtree::logging_enabled())               \
    {                                                \
      SPDLOG_WARN(__VA_ARGS__);                      \
    }                                                \
  } while (0)
#define CT_LOG_INFO(...)                            \
  do {                                              \
    if (::crowtree::logging_enabled())              \
    {                                               \
      SPDLOG_INFO(__VA_ARGS__);                     \
    }                                               \
  } while (0)
#define CT_LOG_DEBUG(...)                           \
  do {                                              \
    if (::crowtree::logging_enabled())              \
    {                                               \
      SPDLOG_DEBUG(__VA_ARGS__);                    \
    }                                               \
  } while (0)
#define CT_LOG_TRACE(...)                           \
  do {                                              \
    if (::crowtree::logging_enabled())              \
    {                                               \
      SPDLOG_TRACE(__VA_ARGS__);                    \
    }                                               \
  } while (0)

#else  // !CROWTREE_HAVE_SPDLOG — zero-cost no-ops

#define CT_LOG_ERROR(...) \
  do {                    \
  } while (0)
#define CT_LOG_WARN(...) \
  do {                   \
  } while (0)
#define CT_LOG_INFO(...) \
  do {                   \
  } while (0)
#define CT_LOG_DEBUG(...) \
  do {                    \
  } while (0)
#define CT_LOG_TRACE(...) \
  do {                    \
  } while (0)

#endif  // CROWTREE_HAVE_SPDLOG
