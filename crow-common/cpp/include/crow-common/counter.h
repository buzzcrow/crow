// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// Monotonic counter with window delta and cumulative total.
// `window` is reset to 0 on each flush; `total` accumulates forever.
#pragma once

#include <atomic>
#include <cstdint>
#include <string>
#include <utility>

namespace crow::common
{

class Counter
{
  public:
    explicit Counter(std::string name) : name_(std::move(name)), window_(0), total_(0)
    {
    }

    void inc()
    {
        window_.fetch_add(1, std::memory_order_relaxed);
    }

    void inc_by(uint64_t n)
    {
        window_.fetch_add(n, std::memory_order_relaxed);
    }

    // Flush: return {window_delta, total} and reset window.
    struct Snapshot
    {
        uint64_t count;
        uint64_t total;
    };

    Snapshot flush()
    {
        uint64_t w = window_.exchange(0, std::memory_order_relaxed);
        uint64_t t = total_.fetch_add(w, std::memory_order_relaxed) + w;
        return {.count = w, .total = t};
    }

    const std::string &name() const
    {
        return name_;
    }

  private:
    std::string           name_;
    std::atomic<uint64_t> window_;
    std::atomic<uint64_t> total_;
};

} // namespace crow::common
