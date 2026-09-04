// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-common/log.h"

#include <gtest/gtest.h>

#include <string>
#include <thread>
#include <vector>

TEST(LogTest, ConcurrentThreadNamePublicationAndFormatting)
{
    std::vector<std::thread> threads;
    for (int i = 0; i < 8; ++i) {
        threads.emplace_back([i] {
            const std::string name = "log-test-" + std::to_string(i);
            for (int iteration = 0; iteration < 100; ++iteration) {
                crowdb::common::set_current_thread_name(name.c_str());
                CRB_LOG_INFO("thread-name contention probe {} {}", i, iteration);
            }
        });
    }
    for (auto &thread : threads) {
        thread.join();
    }
    crowdb::common::flush_logging();
    SUCCEED();
}
