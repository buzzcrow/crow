// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// Shared gtest main for all C++ test binaries. Replaces gtest_main so
// logging is initialized before any test runs: info/debug logs go to
// files under test-logs/<test-binary>-<pid>/, error-level logs are
// mirrored to stderr for CI visibility. shutdown_logging is called
// after all tests complete.

#include "crowdb-common/log.h"

#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>
#include <string>

namespace
{
// Walk up from the test binary's location (CMAKE_CURRENT_BINARY_DIR is
// the build dir, but tests run from there) to find the workspace root
// (marked by pixi.toml). Falls back to /tmp.
std::filesystem::path workspace_root()
{
    std::error_code ec;
    auto            dir = std::filesystem::current_path(ec);
    for (int i = 0; i < 20 && !ec; ++i) {
        if (std::filesystem::exists(dir / "pixi.toml", ec)) {
            return dir;
        }
        auto parent = dir.parent_path();
        if (parent == dir) {
            break;
        }
        dir = parent;
    }
    return std::filesystem::temp_directory_path();
}
} // namespace

int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);

    // Init logging to test-logs/<binary-name>-<pid>/ under the workspace root.
    const auto *binary = std::getenv("CROWDB_TEST_BINARY_NAME");
    std::string name   = binary != nullptr ? binary : "crowdb-test";
    // Use the argv[0] basename if CROWDB_TEST_BINARY_NAME is not set.
    if (binary == nullptr && argc > 0) {
        std::string p   = argv[0];
        auto        pos = p.find_last_of("/\\");
        name            = pos == std::string::npos ? p : p.substr(pos + 1);
    }

    auto            log_dir = workspace_root() / "test-logs" / (name + "-" + std::to_string(::getpid()));
    std::error_code ec;
    std::filesystem::create_directories(log_dir, ec);
    crowdb::common::init_logging(log_dir.string(), "info", 30, 5, name);
    crowdb::common::add_log_stderr("error");

    int result = RUN_ALL_TESTS();

    crowdb::common::flush_logging();
    crowdb::common::shutdown_logging();
    return result;
}
