// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#include "crow-rpc/scheduled_executor.h"

#include <algorithm>

namespace crow::rpc
{

ScheduledExecutor::TaskId ScheduledExecutor::schedule(Task task, uint32_t delay_ms)
{
    if (!task) {
        return 0;
    }
    TaskId        id = next_id_.fetch_add(1, std::memory_order_relaxed);
    ScheduledTask st;
    st.deadline = Clock::now() + std::chrono::milliseconds(delay_ms);
    st.callback = std::move(task);
    {
        std::lock_guard<std::mutex> lock(mu_);
        tasks_[id] = std::move(st);
    }
    return id;
}

bool ScheduledExecutor::cancel(TaskId id)
{
    if (id == 0) {
        return false;
    }
    std::lock_guard<std::mutex> lock(mu_);
    return tasks_.erase(id) > 0;
}

int ScheduledExecutor::run_due_tasks()
{
    std::vector<std::pair<TaskId, Task>> to_run;
    Clock::time_point                    now           = Clock::now();
    Clock::time_point                    next_deadline = Clock::time_point::max();

    {
        std::lock_guard<std::mutex> lock(mu_);
        for (auto it = tasks_.begin(); it != tasks_.end();) {
            if (it->second.deadline <= now) {
                to_run.emplace_back(it->first, std::move(it->second.callback));
                it = tasks_.erase(it);
            }
            else {
                if (it->second.deadline < next_deadline) {
                    next_deadline = it->second.deadline;
                }
                ++it;
            }
        }
    }

    // Run callbacks outside the lock (callbacks may schedule new tasks).
    for (auto &[_, cb] : to_run) {
        cb();
    }

    if (next_deadline == Clock::time_point::max()) {
        return -1; // no pending tasks
    }
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(next_deadline - now);
    return static_cast<int>(ms.count());
}

size_t ScheduledExecutor::pending_count()
{
    std::lock_guard<std::mutex> lock(mu_);
    return tasks_.size();
}

} // namespace crow::rpc
