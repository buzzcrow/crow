// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include <atomic>
#include <cstdint>

namespace crow::common
{

// Per-client monotonic request_id generator. Thread-safe.
// request_id is the per-frame correlation key extracted from the
// flatbuffer control message during parse. Each service client owns
// one RequestIdGen; per-client (not global) because request_id only
// needs uniqueness within one client's pending map.
class RequestIdGen
{
  public:
    RequestIdGen() = default;

    // Disable copying — the atomic counter is not copyable.
    RequestIdGen(const RequestIdGen &)            = delete;
    RequestIdGen &operator=(const RequestIdGen &) = delete;

    // Return the next request_id. Thread-safe; concurrent calls
    // never return the same value.
    uint64_t next()
    {
        return counter_.fetch_add(1, std::memory_order_relaxed);
    }

  private:
    std::atomic<uint64_t> counter_{1};
};

} // namespace crow::common
