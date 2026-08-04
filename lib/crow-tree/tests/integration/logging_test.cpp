// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// Logging lifecycle integration test. The logging is now process-global:
// init_logging() is called explicitly before open(), and shutdown_logging()
// after the engine is destroyed. These tests only exercise the spdlog-backed
// CMake build.
#include "crow-common/log.h"
#include "crow-tree/crow-tree.h"
#include "crow-tree/page_store.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>

using namespace crow::tree;
using namespace crow::common;

namespace
{

namespace fs = std::filesystem;

Batch put_one(const std::string &k, const std::string &v)
{
    return Batch{{{.key = k, .kind = OpKind::kPut, .value = v}}};
}

// A unique temp directory removed on scope exit.
struct TempDir
{
    fs::path path;

    TempDir()
    {
        path = fs::temp_directory_path() /
               ("crowtree_log_" + std::to_string(::testing::UnitTest::GetInstance()->random_seed()) + "_" +
                std::to_string(reinterpret_cast<uintptr_t>(this)));
        fs::create_directories(path);
    }

    ~TempDir()
    {
        std::error_code ec;
        fs::remove_all(path, ec);
    }
};

std::string read_file(const fs::path &p)
{
    std::ifstream      in(p, std::ios::binary);
    std::ostringstream ss;
    ss << in.rdbuf();
    return ss.str();
}

} // namespace

TEST(Logging, WritesFormattedFileOnOpenAndSnapshot)
{
    TempDir      dir;
    MemPageStore store(1);
    Options      opt;
    opt.page_store = &store;

    // Logging is process-global: init before any open().
    shutdown_logging(); // clean slate from prior tests
    init_logging(dir.path.string(), "info", 30, 5, "crow-tree");
    EXPECT_TRUE(logging_enabled());

    {
        std::unique_ptr<Crowtree> t;
        ASSERT_TRUE(Crowtree::open(opt, &t).ok());
        ASSERT_TRUE(t->apply(1, put_one("a", "1")).ok());
        ASSERT_TRUE(t->flush().ok());
        ASSERT_TRUE(t->snapshot(nullptr).ok());
    }
    // Flush + join the async logger so the file is complete before we read it.
    shutdown_logging();
    EXPECT_FALSE(logging_enabled());

    // The log file name includes a timestamp and PID: crow-tree-*.log
    fs::path log;
    for (const auto &entry : fs::directory_iterator(dir.path)) {
        if (entry.path().filename().string().starts_with("crow-tree-") && entry.path().extension() == ".log") {
            log = entry.path();
            break;
        }
    }
    ASSERT_FALSE(log.empty()) << "no crow-tree-*.log file found in " << dir.path;
    std::string body = read_file(log);
    EXPECT_FALSE(body.empty());
    // Pattern: "YYYYMMDD-HHMMSS.mmm [tid] [level] [crow-tree] message"
    EXPECT_NE(body.find("[crow-tree]"), std::string::npos);
    EXPECT_NE(body.find("[info]"), std::string::npos);
    // The open() and snapshot() info lines both fired.
    EXPECT_NE(body.find("open:"), std::string::npos);
    EXPECT_NE(body.find("snapshot committed:"), std::string::npos);
}

TEST(Logging, StderrWhenNoLogDir)
{
    // Make sure a prior test's logger is torn down first.
    shutdown_logging();
    // Empty log_dir => stderr logger (enabled, no file).
    init_logging("", "info", 30, 5, "crow-tree");
    EXPECT_TRUE(logging_enabled());
    MemPageStore store(1);
    Options      opt;
    opt.page_store = &store;
    std::unique_ptr<Crowtree> t;
    ASSERT_TRUE(Crowtree::open(opt, &t).ok());
    ASSERT_TRUE(t->apply(1, put_one("k", "v")).ok());
    ASSERT_TRUE(t->flush().ok());
    shutdown_logging();
}
