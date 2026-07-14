// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// Shared test utility: RAII temp directory under crowtree/.test-tmp/.
// All test-generated data lives under the project tree (gitignored)
// instead of /tmp, so it can be cleaned up uniformly and never leaks
// into git commits.
#pragma once

#include <array>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <string>
#include <unistd.h>
#include <vector>

namespace crowtree_test
{

inline std::string test_tmp_root()
{
    // __FILE__ is tests/integration/*.cpp or tests/unit/*.cpp;
    // the crowtree root is two levels up from the test source dir.
    // At runtime we use a fixed path relative to the repo for determinism.
    static const char *env = std::getenv("CROWTREE_TEST_TMP");
    if (env && env[0] != '\0') {
        return env;
    }
    return ".test-tmp";
}

// RAII temp directory. Creates a unique subdirectory under test_tmp_root()
// on construction, recursively removes it on destruction.
struct TempDir
{
    std::string path;

    TempDir(const char *prefix = "ct_")
    {
        std::string root = test_tmp_root();
        std::filesystem::create_directories(root);
        std::array<char, 128> tmpl{};
        std::snprintf(tmpl.data(), tmpl.size(), "%s/%sXXXXXX", root.c_str(), prefix);
        // mkdtemp modifies in place; copy into a mutable buffer
        std::vector<char> buf(tmpl.begin(), tmpl.end());
        buf.push_back('\0');
        char *d = mkdtemp(buf.data());
        if (d != nullptr) {
            path = d;
        }
    }

    ~TempDir()
    {
        if (!path.empty()) {
            std::error_code ec;
            std::filesystem::remove_all(path, ec);
        }
    }

    TempDir(const TempDir &)            = delete;
    TempDir &operator=(const TempDir &) = delete;
};

// RAII temp file (for tests that need a single file path, not a directory).
struct TempFile
{
    std::string path;

    TempFile(const char *prefix = "ct_")
    {
        std::string root = test_tmp_root();
        std::filesystem::create_directories(root);
        std::array<char, 128> tmpl{};
        std::snprintf(tmpl.data(), tmpl.size(), "%s/%sXXXXXX", root.c_str(), prefix);
        std::vector<char> buf(tmpl.begin(), tmpl.end());
        buf.push_back('\0');
        int fd = mkstemp(buf.data());
        if (fd >= 0) {
            close(fd);
            path = buf.data();
        }
    }

    ~TempFile()
    {
        if (!path.empty()) {
            std::remove(path.c_str());
        }
    }

    TempFile(const TempFile &)            = delete;
    TempFile &operator=(const TempFile &) = delete;
};

} // namespace crowtree_test
