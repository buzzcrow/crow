// Copyright 2026-present buzzcrow <126.com>
// Licensed under the Apache License, Version 2.0.

// CallbackGauge: a gauge whose value is computed on-demand via a
// callback at flush time, rather than stored in an atomic. Use for
// values that are expensive to maintain incrementally (e.g. live
// connection counts from a map) — the callback reads the current
// state when the metrics flush thread asks for it.
#pragma once

#include <cstdint>
#include <functional>
#include <string>
#include <utility>

namespace crow::common::metrics
{

class CallbackGauge
{
  public:
    using Callback = std::function<uint64_t()>;

    explicit CallbackGauge(std::string name, Callback cb) : name_(std::move(name)), cb_(std::move(cb))
    {
    }

    // Invoke the callback to get the current value. Called by the
    // metrics flush thread at report time.
    uint64_t get() const
    {
        return cb_();
    }

    const std::string &name() const
    {
        return name_;
    }

  private:
    std::string name_;
    Callback    cb_;
};

} // namespace crow::common::metrics
