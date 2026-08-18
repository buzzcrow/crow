// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#include "crow-rpc/framing.h"

#include <gtest/gtest.h>

#include <cstring>
#include <vector>

using crow::rpc::Frame;
using crow::rpc::FrameParser;
using crow::rpc::FramingError;
using crow::rpc::Header;
using crow::rpc::HEADER_SIZE;
using crow::rpc::MAGIC;
using crow::rpc::parse_header;
using crow::rpc::serialize_header;

// Helper: serialize a full frame (header + control + data) into a byte vec.
static std::vector<uint8_t> build_frame(uint16_t msg_type, const uint8_t *ctrl, uint32_t ctrl_len, const uint8_t *data,
                                        uint32_t data_len, uint8_t flags = 0)
{
    Header h;
    h.msg_type  = msg_type;
    h.msg_size  = static_cast<uint16_t>(ctrl_len);
    h.data_size = data_len;
    h.flags     = flags;

    std::vector<uint8_t> buf(HEADER_SIZE + ctrl_len + data_len);
    serialize_header(buf.data(), h);
    if (ctrl_len > 0) {
        std::memcpy(buf.data() + HEADER_SIZE, ctrl, ctrl_len);
    }
    if (data_len > 0) {
        std::memcpy(buf.data() + HEADER_SIZE + ctrl_len, data, data_len);
    }
    return buf;
}

TEST(FramingTest, HeaderRoundTrip)
{
    Header h;
    h.msg_type  = 42;
    h.msg_size  = 128;
    h.data_size = 1024 * 1024;
    h.flags     = 0x01;

    uint8_t buf[HEADER_SIZE];
    serialize_header(buf, h);
    Header parsed = parse_header(buf);

    EXPECT_EQ(parsed.magic, MAGIC);
    EXPECT_EQ(parsed.msg_type, 42u);
    EXPECT_EQ(parsed.msg_size, 128u);
    EXPECT_EQ(parsed.data_size, 1024u * 1024u);
    EXPECT_EQ(parsed.msg_offset, HEADER_SIZE);
    EXPECT_EQ(parsed.flags, 0x01u);
}

TEST(FramingTest, FullFrameRoundTrip)
{
    // 128-byte control + 1 MB data
    std::vector<uint8_t> ctrl(128, 0xAB);
    std::vector<uint8_t> data(1024 * 1024, 0xCD);

    auto bytes = build_frame(7, ctrl.data(), 128, data.data(), 1024 * 1024);

    FrameParser parser;
    Frame      *frame = nullptr;

    // Feed all bytes at once
    uint32_t offset = 0;
    while (offset < bytes.size()) {
        auto     target  = parser.next_read_target();
        uint32_t to_read = std::min(target.len, static_cast<uint32_t>(bytes.size() - offset));
        std::memcpy(target.ptr, bytes.data() + offset, to_read);
        offset += to_read;
        frame = parser.advance(to_read);
        if (frame)
            break;
    }

    ASSERT_NE(frame, nullptr);
    EXPECT_EQ(frame->header.msg_type, 7u);
    EXPECT_EQ(frame->header.msg_size, 128u);
    EXPECT_EQ(frame->header.data_size, 1024u * 1024u);
    EXPECT_EQ(frame->control_len, 128u);
    EXPECT_EQ(frame->data_len, 1024u * 1024u);
    EXPECT_EQ(std::memcmp(frame->control, ctrl.data(), 128), 0);
    EXPECT_EQ(std::memcmp(frame->data, data.data(), 1024 * 1024), 0);

    delete frame;
}

TEST(FramingTest, BadMagic)
{
    std::vector<uint8_t> ctrl(16, 0);
    auto                 bytes = build_frame(1, ctrl.data(), 16, nullptr, 0);
    // Corrupt magic
    bytes[0] = 0xFF;
    bytes[1] = 0xFF;

    FrameParser parser;
    auto        target = parser.next_read_target();
    std::memcpy(target.ptr, bytes.data(), HEADER_SIZE);
    Frame *frame = parser.advance(HEADER_SIZE);

    EXPECT_EQ(frame, nullptr);
    EXPECT_EQ(parser.last_error(), FramingError::BadMagic);
}

TEST(FramingTest, PartialHeader)
{
    std::vector<uint8_t> ctrl(16, 0);
    auto                 bytes = build_frame(1, ctrl.data(), 16, nullptr, 0);

    FrameParser parser;

    // Feed first 6 bytes
    auto t1 = parser.next_read_target();
    ASSERT_EQ(t1.len, HEADER_SIZE);
    std::memcpy(t1.ptr, bytes.data(), 6);
    EXPECT_EQ(parser.advance(6), nullptr);

    // Feed remaining 6 bytes
    auto t2 = parser.next_read_target();
    ASSERT_EQ(t2.len, HEADER_SIZE - 6u);
    std::memcpy(t2.ptr, bytes.data() + 6, 6);
    EXPECT_EQ(parser.advance(6), nullptr); // header done, need control

    // Feed control
    auto t3 = parser.next_read_target();
    ASSERT_EQ(t3.len, 16u);
    std::memcpy(t3.ptr, bytes.data() + HEADER_SIZE, 16);
    Frame *frame = parser.advance(16);
    ASSERT_NE(frame, nullptr);
    EXPECT_EQ(frame->header.msg_type, 1u);
    EXPECT_EQ(frame->control_len, 16u);
    delete frame;
}

TEST(FramingTest, ControlOnlyFrame)
{
    std::vector<uint8_t> ctrl(32, 0x42);
    auto                 bytes = build_frame(3, ctrl.data(), 32, nullptr, 0);

    FrameParser parser;
    uint32_t    offset = 0;
    Frame      *frame  = nullptr;
    while (offset < bytes.size()) {
        auto     target  = parser.next_read_target();
        uint32_t to_read = std::min(target.len, static_cast<uint32_t>(bytes.size() - offset));
        std::memcpy(target.ptr, bytes.data() + offset, to_read);
        offset += to_read;
        frame = parser.advance(to_read);
        if (frame)
            break;
    }

    ASSERT_NE(frame, nullptr);
    EXPECT_EQ(frame->header.data_size, 0u);
    EXPECT_EQ(frame->data_len, 0u);
    EXPECT_EQ(frame->data, nullptr);
    EXPECT_EQ(frame->control_len, 32u);
    delete frame;
}

TEST(FramingTest, DataSizeTooLarge)
{
    Header h;
    h.msg_type  = 1;
    h.msg_size  = 0;
    h.data_size = 16 * 1024 * 1024; // 16 MB, exceeds default max (4 MB)

    uint8_t buf[HEADER_SIZE];
    serialize_header(buf, h);

    FrameParser parser(4 << 20); // max 4 MB
    auto        target = parser.next_read_target();
    std::memcpy(target.ptr, buf, HEADER_SIZE);
    Frame *frame = parser.advance(HEADER_SIZE);

    EXPECT_EQ(frame, nullptr);
    EXPECT_EQ(parser.last_error(), FramingError::DataTooLarge);
}

TEST(FramingTest, OneWayFlag)
{
    std::vector<uint8_t> ctrl(8, 0);
    auto                 bytes = build_frame(2, ctrl.data(), 8, nullptr, 0, crow::rpc::FLAG_ONE_WAY);

    FrameParser parser;
    uint32_t    offset = 0;
    Frame      *frame  = nullptr;
    while (offset < bytes.size()) {
        auto     target  = parser.next_read_target();
        uint32_t to_read = std::min(target.len, static_cast<uint32_t>(bytes.size() - offset));
        std::memcpy(target.ptr, bytes.data() + offset, to_read);
        offset += to_read;
        frame = parser.advance(to_read);
        if (frame)
            break;
    }

    ASSERT_NE(frame, nullptr);
    EXPECT_EQ(frame->header.flags, crow::rpc::FLAG_ONE_WAY);
    delete frame;
}
