// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// Task 1: IoEngine + DirectIoEngine tests.
#include "crowtree/io_engine.h"
#include "test_tmp.h"

#include <gtest/gtest.h>

#include <array>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <string>
#include <unistd.h>
#include <vector>

using namespace crowtree;

namespace
{
std::string temp_path()
{
    std::string root = crowtree_test::test_tmp_root();
    std::filesystem::create_directories(root);
    std::array<char, 128> tmpl{};
    std::snprintf(tmpl.data(), tmpl.size(), "%s/io_XXXXXX", root.c_str());
    std::vector<char> buf(tmpl.begin(), tmpl.end());
    buf.push_back('\0');
    int fd = mkstemp(buf.data());
    if (fd >= 0) {
        close(fd);
    }
    return buf.data();
}
} // namespace

TEST(IoEngine, DirectReadRoundTrip)
{
    std::string path = temp_path();
    int         fd   = ::open(path.c_str(), O_RDWR | O_CREAT, 0644);
    ASSERT_GE(fd, 0);

    DirectIoEngine       engine;
    std::vector<uint8_t> in{0xAA, 0xBB, 0xCC, 0xDD, 0xEE};
    Status               write_st;
    engine.submit_write(fd, in.data(), in.size(), 0, [&write_st](Status s) { write_st = s; });
    EXPECT_TRUE(write_st.ok()) << write_st.to_string();

    Status               fsync_st;
    engine.submit_fsync(fd, [&fsync_st](Status s) { fsync_st = s; });
    EXPECT_TRUE(fsync_st.ok()) << fsync_st.to_string();

    std::vector<uint8_t> out(in.size(), 0);
    Status               read_st;
    engine.submit_read(fd, out.data(), out.size(), 0, [&read_st](Status s) { read_st = s; });
    EXPECT_TRUE(read_st.ok()) << read_st.to_string();
    EXPECT_EQ(in, out);

    ::close(fd);
    ::unlink(path.c_str());
}

TEST(IoEngine, DirectCallbacksAreImmediate)
{
    std::string path = temp_path();
    int         fd   = ::open(path.c_str(), O_RDWR | O_CREAT, 0644);
    ASSERT_GE(fd, 0);

    DirectIoEngine engine;
    bool           callback_fired = false;
    uint8_t        buf            = 0x42;

    engine.submit_write(fd, &buf, 1, 0, [&callback_fired](Status) { callback_fired = true; });
    // DirectIoEngine invokes the callback inline, so it must be true here.
    EXPECT_TRUE(callback_fired);

    ::close(fd);
    ::unlink(path.c_str());
}

TEST(IoEngine, DirectWriteAtOffset)
{
    std::string path = temp_path();
    int         fd   = ::open(path.c_str(), O_RDWR | O_CREAT, 0644);
    ASSERT_GE(fd, 0);

    DirectIoEngine       engine;
    std::vector<uint8_t> in(16, 0);
    for (size_t i = 0; i < in.size(); ++i) {
        in[i] = static_cast<uint8_t>(i);
    }

    Status st;
    engine.submit_write(fd, in.data(), in.size(), 1024, [&st](Status s) { st = s; });
    EXPECT_TRUE(st.ok());

    std::vector<uint8_t> out(in.size(), 0);
    engine.submit_read(fd, out.data(), out.size(), 1024, [&st](Status s) { st = s; });
    EXPECT_TRUE(st.ok());
    EXPECT_EQ(in, out);

    ::close(fd);
    ::unlink(path.c_str());
}
