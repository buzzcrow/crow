// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// Bandwidth: tracks count, byte sum (window), and total bytes.
#pragma once

#include <atomic>
#include <cstdint>
#include <string>
#include <utility>

namespace crow::common::metrics
{

class Bandwidth
{
  public:
    explicit Bandwidth(std::string name) : name_(std::move(name)), count_(0), sum_(0), total_bytes_(0)
    {
    }

    void observe(uint64_t bytes)
    {
        count_.fetch_add(1, std::memory_order_relaxed);
        sum_.fetch_add(bytes, std::memory_order_relaxed);
        total_bytes_.fetch_add(bytes, std::memory_order_relaxed);
    }

    struct Snapshot
    {
        uint64_t count;
        uint64_t sum;
        uint64_t total_bytes;
    };

    Snapshot flush()
    {
        uint64_t c = count_.exchange(0, std::memory_order_relaxed);
        uint64_t s = sum_.exchange(0, std::memory_order_relaxed);
        uint64_t t = total_bytes_.load(std::memory_order_relaxed);
        return {.count = c, .sum = s, .total_bytes = t};
    }

    const std::string &name() const
    {
        return name_;
    }

  private:
    std::string           name_;
    std::atomic<uint64_t> count_;
    std::atomic<uint64_t> sum_;
    std::atomic<uint64_t> total_bytes_;
};

} // namespace crow::common::metrics
