// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <mutex>
#include <unordered_map>

namespace crowdb::rpc
{

// ScheduledExecutor runs deferred tasks on the worker thread. It's used
// for request timeouts and reconnect backoff. Tasks are keyed by a
// monotonic task_id; each task has a deadline (steady_clock) and a
// callback. The worker's timer event calls run_due_tasks() which fires
// all tasks whose deadline has passed.
//
// This is a simple mutex-protected map for v1. The hot path (request
// timeout) is not high-frequency enough to warrant a lock-free structure;
// the consensus hot path is the send/recv, not the timeout scheduling.
class ScheduledExecutor
{
  public:
    using Clock  = std::chrono::steady_clock;
    using TaskId = uint64_t;
    using Task   = std::function<void()>;

    ScheduledExecutor() = default;

    // Schedule a task to run after delay_ms. Returns the task_id (0 on
    // error — e.g. empty callback).
    TaskId schedule(Task task, uint32_t delay_ms);

    // Cancel a scheduled task. Returns true if the task was found and
    // removed, false if it already ran or was not found.
    bool cancel(TaskId id);

    // Run all tasks whose deadline has passed. Returns the time until the
    // next due task (in ms), or -1 if no tasks are pending. The caller
    // (worker loop) uses this to set the next timer.
    int run_due_tasks();

    // Number of pending tasks (for diagnostics).
    size_t pending_count();

  private:
    struct ScheduledTask
    {
        Clock::time_point deadline;
        Task              callback;
    };

    std::atomic<TaskId>                       next_id_{1};
    std::mutex                                mu_;
    std::unordered_map<TaskId, ScheduledTask> tasks_;
};

} // namespace crowdb::rpc
