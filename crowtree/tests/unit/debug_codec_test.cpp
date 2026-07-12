// PT9.4: readable debug codec exact round-trip.
#include "crowtree/cell.h"
#include "crowtree/debug_codec.h"
#include "crowtree/frame_page.h"

#include <gtest/gtest.h>

#include <string>
#include <vector>

using namespace crowtree;

namespace
{
std::string make_cell(uint64_t slot, const std::string &v, bool tomb = false)
{
    return encode_cell(slot, tomb ? OpKind::kDelete : OpKind::kPut, Slice(v));
}
} // namespace

TEST(DebugCodec, LeafRoundTripExact)
{
    const uint32_t       pb = 4096;
    std::vector<uint8_t> frame(pb);
    LeafFrameBuilder     b(frame.data(), pb);
    ASSERT_TRUE(b.try_append_sorted(Slice(std::string("a\0b", 3)), Slice(make_cell(1, "A"))));
    ASSERT_TRUE(b.try_append_sorted(Slice("bb"), Slice(make_cell(2, "value"))));
    ASSERT_TRUE(b.try_append_sorted(Slice("cc"), Slice(make_cell(3, "", true))));
    b.finish(7, 42);

    std::string text = encode_frame_text(frame.data(), pb);
    EXPECT_NE(text.find("type leaf"), std::string::npos);
    std::vector<uint8_t> back;
    ASSERT_TRUE(decode_frame_text(text, &back).ok());
    EXPECT_EQ(back, frame);
    EXPECT_TRUE(frame_validate(back.data(), pb));
}

TEST(DebugCodec, InnerRoundTripExact)
{
    const uint32_t        pb = 4096;
    std::vector<uint8_t>  frame(pb);
    std::vector<uint64_t> children = {10, 11, 12};
    std::string           s0 = "m";
    std::string           s1 = "t";
    std::vector<Slice>    seps = {Slice(s0), Slice(s1)};
    ASSERT_TRUE(inner_frame_build(frame.data(), pb, 3, children, seps));

    std::string text = encode_frame_text(frame.data(), pb);
    EXPECT_NE(text.find("type inner"), std::string::npos);
    std::vector<uint8_t> back;
    ASSERT_TRUE(decode_frame_text(text, &back).ok());
    EXPECT_EQ(back, frame);
}

TEST(DebugCodec, OverflowRoundTripExact)
{
    const uint32_t       pb = 1024;
    std::vector<uint8_t> frame(pb);
    std::string          payload("chunk\0bytes", 11);
    overflow_frame_build(frame.data(), pb, 5, 9, reinterpret_cast<const uint8_t *>(payload.data()),
                         static_cast<uint32_t>(payload.size()));
    std::string text = encode_frame_text(frame.data(), pb);
    EXPECT_NE(text.find("type overflow"), std::string::npos);
    std::vector<uint8_t> back;
    ASSERT_TRUE(decode_frame_text(text, &back).ok());
    EXPECT_EQ(back, frame);
}

TEST(DebugCodec, RejectsMalformed)
{
    std::vector<uint8_t> out;
    EXPECT_EQ(decode_frame_text("no header here\n", &out).code(), Code::kInvalidArgument);
    EXPECT_EQ(decode_frame_text("crowtree-frame-text 1\nplen 4\nraw 00\n", &out).code(),
              Code::kCorruption); // raw length != plen
}
