// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// Gauge: current state, can go up or down.
#pragma once

#include <atomic>
#include <cstdint>
#include <string>
#include <utility>

namespace crowdb::common::metrics
{

class Gauge
{
  public:
    explicit Gauge(std::string name) : name_(std::move(name)), value_(0)
    {
    }

    void set(uint64_t v)
    {
        value_.store(v, std::memory_order_relaxed);
    }

    uint64_t get() const
    {
        return value_.load(std::memory_order_relaxed);
    }

    const std::string &name() const
    {
        return name_;
    }

  private:
    std::string           name_;
    std::atomic<uint64_t> value_;
};

} // namespace crowdb::common::metrics
