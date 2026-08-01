// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// Latency histogram with fixed buckets and percentile reporting.
#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace crow::common::metrics
{

class LatencyHistogram
{
  public:
    explicit LatencyHistogram(std::string name);

    void observe(uint64_t ns);

    struct Snapshot
    {
        uint64_t              count;
        uint64_t              sum;
        uint64_t              total_count;
        std::vector<uint64_t> bucket_counts;
    };

    Snapshot flush();

    static uint64_t percentile(const Snapshot &snap, double p);

    const std::string &name() const
    {
        return name_;
    }

  private:
    std::string                                         name_;
    std::vector<std::unique_ptr<std::atomic<uint64_t>>> buckets_;
    std::atomic<uint64_t>                               count_;
    std::atomic<uint64_t>                               sum_;
    std::atomic<uint64_t>                               total_count_;
};

} // namespace crow::common::metrics
