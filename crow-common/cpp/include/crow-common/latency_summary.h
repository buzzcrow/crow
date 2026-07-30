// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// Lightweight latency summary: count, sum, max, total_count.
#pragma once

#include <atomic>
#include <cstdint>
#include <string>
#include <utility>

namespace crow::common
{

class LatencySummary
{
  public:
    explicit LatencySummary(std::string name) : name_(std::move(name)), count_(0), sum_(0), max_(0), total_count_(0)
    {
    }

    void observe(uint64_t ns)
    {
        count_.fetch_add(1, std::memory_order_relaxed);
        sum_.fetch_add(ns, std::memory_order_relaxed);
        total_count_.fetch_add(1, std::memory_order_relaxed);
        uint64_t old_max = max_.load(std::memory_order_relaxed);
        while (ns > old_max && !max_.compare_exchange_weak(old_max, ns, std::memory_order_relaxed)) {
        }
    }

    struct Snapshot
    {
        uint64_t count;
        uint64_t sum;
        uint64_t max;
        uint64_t total_count;
    };

    Snapshot flush()
    {
        uint64_t c = count_.exchange(0, std::memory_order_relaxed);
        uint64_t s = sum_.exchange(0, std::memory_order_relaxed);
        uint64_t m = max_.exchange(0, std::memory_order_relaxed);
        uint64_t t = total_count_.load(std::memory_order_relaxed);
        return {.count = c, .sum = s, .max = m, .total_count = t};
    }

    const std::string &name() const
    {
        return name_;
    }

  private:
    std::string           name_;
    std::atomic<uint64_t> count_;
    std::atomic<uint64_t> sum_;
    std::atomic<uint64_t> max_;
    std::atomic<uint64_t> total_count_;
};

} // namespace crow::common
