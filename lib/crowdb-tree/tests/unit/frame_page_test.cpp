// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// PT6a: zero-copy slotted frame format + views.
#include "crowdb-tree/cell.h"
#include "crowdb-tree/frame_page.h"

#include <gtest/gtest.h>

#include <array>
#include <cstdio>
#include <string>
#include <vector>

using namespace crowdb::tree;

namespace
{
std::string make_cell(uint64_t slot, const std::string &v, bool tomb = false)
{
    return encode_cell(slot, tomb ? OpKind::kDelete : OpKind::kPut, Slice(v));
}

// buffer-cell variant for leaf_entry (whose cell is a move-only buffer).
buffer make_cell_buf(uint64_t slot, const std::string &v, bool tomb = false)
{
    return encode_cell_buf(slot, tomb ? OpKind::kDelete : OpKind::kPut, Slice(v));
}

std::string make_key(int i)
{
    std::array<char, 16> b{};
    snprintf(b.data(), b.size(), "k%05d", i);
    return b.data();
}
} // namespace

TEST(FramePage, LeafBuildViewRoundTrip)
{
    const uint32_t                                   pb = 4096;
    std::vector<uint8_t>                             frame(pb);
    std::vector<std::pair<std::string, std::string>> entries = {
        {"a", make_cell(1, "A")},
        {"b", make_cell(2, "BB")},
        {"c", make_cell(3, "", true)}
    };
    LeafFrameBuilder b(frame.data(), pb);
    for (auto &e : entries) {
        ASSERT_TRUE(b.try_append_sorted(Slice(e.first), Slice(e.second)));
    }
    b.finish(/*self_page_id=*/7, /*right_sibling=*/42);

    ASSERT_TRUE(frame_validate(frame.data(), pb));
    LeafFrameView v(frame.data(), pb);
    EXPECT_EQ(v.self_page_id(), 7U);
    EXPECT_EQ(v.right_sibling(), 42U);
    ASSERT_EQ(v.count(), 3U);
    EXPECT_EQ(v.key(0).to_string(), "a");
    EXPECT_EQ(v.key(2).to_string(), "c");
    EXPECT_EQ(CellView{v.cell(1)}.value().to_string(), "BB");
    EXPECT_TRUE(CellView{v.cell(2)}.is_tombstone());
}

TEST(FramePage, LeafFindAndLowerBound)
{
    const uint32_t       pb = 4096;
    std::vector<uint8_t> frame(pb);
    LeafFrameBuilder     b(frame.data(), pb);
    for (int i = 0; i < 50; ++i) {
        ASSERT_TRUE(b.try_append_sorted(Slice(make_key(i * 2)), Slice(make_cell(i, "v"))));
    }
    b.finish(1, kInvalidPageId);
    LeafFrameView v(frame.data(), pb);

    EXPECT_EQ(v.find(Slice(make_key(20))), 10);
    EXPECT_EQ(v.find(Slice(make_key(21))), -1); // odd keys absent
    EXPECT_EQ(v.lower_bound(Slice(make_key(21))), 11U);
    EXPECT_EQ(v.lower_bound(Slice(make_key(0))), 0U);
    CellView c;
    ASSERT_TRUE(v.lookup(Slice(make_key(40)), &c));
    EXPECT_EQ(c.slot(), 20U);
}

TEST(FramePage, LeafCapacityRejectsWhenFull)
{
    const uint32_t       pb = 256; // tiny frame
    std::vector<uint8_t> frame(pb);
    LeafFrameBuilder     b(frame.data(), pb);
    int                  appended = 0;
    for (int i = 0; i < 1000; ++i) {
        if (!b.try_append_sorted(Slice(make_key(i)), Slice(make_cell(i, "value-bytes")))) {
            break;
        }
        ++appended;
    }
    b.finish(1, kInvalidPageId);
    EXPECT_GT(appended, 0);
    EXPECT_LT(appended, 1000);
    ASSERT_TRUE(frame_validate(frame.data(), pb));
    EXPECT_EQ(LeafFrameView(frame.data(), pb).count(), static_cast<uint32_t>(appended));
}

TEST(FramePage, BinaryKeysWithNuls)
{
    const uint32_t       pb = 1024;
    std::vector<uint8_t> frame(pb);
    std::string          k1("a\0b", 3);
    std::string          k2("a\0c", 3);
    LeafFrameBuilder     b(frame.data(), pb);
    ASSERT_TRUE(b.try_append_sorted(Slice(k1), Slice(make_cell(1, std::string("x\0y", 3)))));
    ASSERT_TRUE(b.try_append_sorted(Slice(k2), Slice(make_cell(2, "z"))));
    b.finish(1, kInvalidPageId);
    LeafFrameView v(frame.data(), pb);
    EXPECT_EQ(v.key(0).to_string(), k1);
    EXPECT_EQ(v.find(Slice(k2)), 1);
}

TEST(FramePage, CrcDetectsCorruption)
{
    const uint32_t       pb = 1024;
    std::vector<uint8_t> frame(pb);
    LeafFrameBuilder     b(frame.data(), pb);
    ASSERT_TRUE(b.try_append_sorted(Slice("a"), Slice(make_cell(1, "A"))));
    b.finish(1, kInvalidPageId);
    ASSERT_TRUE(frame_validate(frame.data(), pb));
    frame[kFrameHeaderSize + 1] ^= 0xff; // flip a slot-dir byte
    EXPECT_FALSE(frame_validate(frame.data(), pb));
}

TEST(FramePage, InnerBuildViewRoundTrip)
{
    const uint32_t        pb = 4096;
    std::vector<uint8_t>  frame(pb);
    std::vector<uint64_t> children = {10, 11, 12};
    std::string           s0       = "m";
    std::string           s1       = "t";
    std::vector<Slice>    seps     = {Slice(s0), Slice(s1)};
    ASSERT_TRUE(inner_frame_build(frame.data(), pb, /*self_page_id=*/3, children, seps));
    ASSERT_TRUE(frame_validate(frame.data(), pb));

    InnerFrameView v(frame.data(), pb);
    EXPECT_EQ(v.self_page_id(), 3U);
    ASSERT_EQ(v.num_children(), 3U);
    ASSERT_EQ(v.num_separators(), 2U);
    EXPECT_EQ(v.child_at(1), 11U);
    EXPECT_EQ(v.separator_at(0).to_string(), "m");
    // Routing: keys < m -> child 0; [m,t) -> child 1; >= t -> child 2.
    EXPECT_EQ(v.child_for(Slice("a")), 10U);
    EXPECT_EQ(v.child_for(Slice("m")), 11U);
    EXPECT_EQ(v.child_for(Slice("q")), 11U);
    EXPECT_EQ(v.child_for(Slice("t")), 12U);
    EXPECT_EQ(v.child_for(Slice("z")), 12U);
}

TEST(FramePage, InnerBuildRejectsOversize)
{
    const uint32_t        pb = 128;
    std::vector<uint8_t>  frame(pb);
    std::vector<uint64_t> children = {1, 2};
    std::string           big(200, 'x');
    std::vector<Slice>    seps = {Slice(big)};
    EXPECT_FALSE(inner_frame_build(frame.data(), pb, 1, children, seps));
}

TEST(FramePage, OverflowBuildViewRoundTrip)
{
    const uint32_t       pb = 4096;
    std::vector<uint8_t> frame(pb);
    std::string          payload(overflow_chunk_cap(pb), 'z'); // full chunk
    overflow_frame_build(frame.data(), pb, /*self_page_id=*/5, /*next_page_id=*/9,
                         reinterpret_cast<const uint8_t *>(payload.data()), static_cast<uint32_t>(payload.size()));
    ASSERT_TRUE(frame_validate(frame.data(), pb));
    EXPECT_EQ(frame_page_type(frame.data()), page_type::kOverflowFrame);
    OverflowFrameView v(frame.data(), pb);
    EXPECT_EQ(v.self_page_id(), 5U);
    EXPECT_EQ(v.next_page_id(), 9U);
    EXPECT_EQ(v.chunk_len(), payload.size());
    EXPECT_EQ(v.payload().to_string(), payload);
}

TEST(FramePage, InFrameDeltaOverlay)
{
    const uint32_t       pb = 4096;
    std::vector<uint8_t> base(pb);
    LeafFrameBuilder     b(base.data(), pb);
    ASSERT_TRUE(b.try_append_sorted(Slice("a"), Slice(make_cell(1, "A1"))));
    ASSERT_TRUE(b.try_append_sorted(Slice("b"), Slice(make_cell(1, "B1"))));
    b.finish(1, kInvalidPageId);
    ASSERT_EQ(LeafFrameView(base.data(), pb).delta_count(), 0U);

    // COW-append two deltas: overwrite "a", insert "c".
    std::vector<leaf_entry> deltas;
    deltas.push_back({.key = "a", .cell = make_cell_buf(5, "A5")});
    deltas.push_back({.key = "c", .cell = make_cell_buf(5, "C5")});
    std::vector<uint8_t> out(pb);
    ASSERT_TRUE(leaf_frame_append_deltas(base.data(), pb, deltas, out.data()));
    ASSERT_TRUE(frame_validate(out.data(), pb));
    LeafFrameView v(out.data(), pb);
    EXPECT_EQ(v.count(), 2U);       // main entries unchanged
    EXPECT_EQ(v.delta_count(), 2U); // deltas appended

    CellView c;
    ASSERT_TRUE(v.lookup(Slice("a"), &c)); // delta shadows base
    EXPECT_EQ(c.value().to_string(), "A5");
    ASSERT_TRUE(v.lookup(Slice("b"), &c)); // untouched base entry
    EXPECT_EQ(c.value().to_string(), "B1");
    ASSERT_TRUE(v.lookup(Slice("c"), &c)); // delta-only key
    EXPECT_EQ(c.value().to_string(), "C5");
    EXPECT_FALSE(v.lookup(Slice("zz"), &c));
}

TEST(FramePage, InFrameDeltaRejectsWhenFull)
{
    const uint32_t       pb = 256; // tiny frame
    std::vector<uint8_t> base(pb);
    LeafFrameBuilder     b(base.data(), pb);
    ASSERT_TRUE(b.try_append_sorted(Slice("a"), Slice(make_cell(1, "A"))));
    b.finish(1, kInvalidPageId);
    std::vector<leaf_entry> big;
    big.push_back({.key = std::string(400, 'k'), .cell = make_cell_buf(2, std::string(400, 'v'))});
    std::vector<uint8_t> out(pb);
    EXPECT_FALSE(leaf_frame_append_deltas(base.data(), pb, big, out.data()));
}

TEST(FramePage, OverflowPartialChunkAndCrc)
{
    const uint32_t       pb = 1024;
    std::vector<uint8_t> frame(pb);
    std::string          payload("partial chunk \0 with nul", 24);
    overflow_frame_build(frame.data(), pb, 1, kInvalidPageId, reinterpret_cast<const uint8_t *>(payload.data()),
                         static_cast<uint32_t>(payload.size()));
    ASSERT_TRUE(frame_validate(frame.data(), pb));
    OverflowFrameView v(frame.data(), pb);
    EXPECT_EQ(v.next_page_id(), kInvalidPageId);
    EXPECT_EQ(v.payload().to_string(), payload);
    frame[kFrameHeaderSize + 2] ^= 0xff; // corrupt payload
    EXPECT_FALSE(frame_validate(frame.data(), pb));
}
