// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-common/request_id.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <set>
#include <thread>
#include <vector>

TEST(RequestIdGen, NextIsMonotonicallyIncreasing)
{
    crowdb::common::RequestIdGen gen;
    EXPECT_EQ(gen.next(), 1);
    EXPECT_EQ(gen.next(), 2);
    EXPECT_EQ(gen.next(), 3);
}

TEST(RequestIdGen, NextIsUniqueUnderConcurrency)
{
    crowdb::common::RequestIdGen         gen;
    const int                          n_threads  = 8;
    const int                          per_thread = 1000;
    std::vector<std::thread>           threads;
    std::vector<std::vector<uint64_t>> results(n_threads);

    for (int t = 0; t < n_threads; ++t) {
        threads.emplace_back([&gen, &results, t]() {
            for (int i = 0; i < per_thread; ++i) {
                results[t].push_back(gen.next());
            }
        });
    }
    for (auto &th : threads) {
        th.join();
    }

    std::set<uint64_t> all;
    for (const auto &v : results) {
        for (uint64_t id : v) {
            all.insert(id);
        }
    }
    EXPECT_EQ(all.size(), static_cast<size_t>(n_threads * per_thread));
}
