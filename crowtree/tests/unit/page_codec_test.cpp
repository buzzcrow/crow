// PT2: PageCodec round-trip + CRC validation tests.
#include "crowtree/cell.h"
#include "crowtree/page.h"
#include "crowtree/page_codec.h"

#include <gtest/gtest.h>

#include <array>
#include <memory>
#include <string>
#include <vector>

using namespace crowtree;

namespace
{
leaf_entry entry(const std::string &k, uint64_t slot, const std::string &v, bool tomb = false)
{
    return {.key = k, .cell = encode_cell_buf(slot, tomb ? OpKind::kDelete : OpKind::kPut, Slice(v))};
}

// Build a vector of move-only leaf_entry (a braced-init-list can't hold move-only).
template <class... Es> std::vector<leaf_entry> entries(Es &&...es)
{
    std::vector<leaf_entry> v;
    v.reserve(sizeof...(es));
    (v.push_back(std::forward<Es>(es)), ...);
    return v;
}
} // namespace

TEST(PageCodec, LeafRoundTrip)
{
    auto                      ents = entries(entry("a", 1, "A"), entry("b", 2, "BB"), entry("c", 3, "", true));
    std::unique_ptr<LeafBase> leaf(LeafBase::build(ents, 42));
    leaf->page_id = 7;

    auto frame = PageCodec::encode(leaf.get(), 4096);
    EXPECT_EQ(frame.size() % 4096, 0U); // IU padded

    PageBase *out = nullptr;
    ASSERT_TRUE(PageCodec::decode(frame.data(), frame.size(), &out).ok());
    ASSERT_NE(out, nullptr);
    ASSERT_EQ(out->type, page_type::kLeafBase);
    auto *got = static_cast<LeafBase *>(out);
    EXPECT_EQ(got->page_id, 7U);
    EXPECT_EQ(got->right_sibling(), 42U);
    ASSERT_EQ(got->count(), 3U);
    EXPECT_EQ(got->entry(0).key, "a");
    EXPECT_EQ(got->entry(2).key, "c");
    EXPECT_TRUE(CellView{Slice(got->entry(2).cell)}.is_tombstone()); // NOLINT(clang-analyzer-unix.Malloc)
    delete out;
}

TEST(PageCodec, InnerRoundTrip)
{
    std::unique_ptr<InnerBase> inner(InnerBase::build({"m", "t"}, {10, 11, 12}));
    inner->page_id = 3;
    auto frame     = PageCodec::encode(inner.get(), 1); // byte-addressable, no pad

    PageBase *out = nullptr;
    ASSERT_TRUE(PageCodec::decode(frame.data(), frame.size(), &out).ok());
    ASSERT_EQ(out->type, page_type::kInnerBase);
    auto *got = static_cast<InnerBase *>(out);
    EXPECT_EQ(got->page_id, 3U);
    ASSERT_EQ(got->num_children(), 3U);
    ASSERT_EQ(got->num_separators(), 2U);
    EXPECT_EQ(got->child_at(1), 11U);
    EXPECT_EQ(got->separator_at(0), "m");
    delete out;
}

TEST(PageCodec, EmptyLeaf)
{
    std::unique_ptr<LeafBase> leaf(LeafBase::build({}));
    leaf->page_id   = 0;
    auto      frame = PageCodec::encode(leaf.get(), 1);
    PageBase *out   = nullptr;
    ASSERT_TRUE(PageCodec::decode(frame.data(), frame.size(), &out).ok());
    EXPECT_EQ(static_cast<LeafBase *>(out)->count(), 0U);
    delete out;
}

TEST(PageCodec, BinaryKeysWithNuls)
{
    std::string               k("a\0b", 3);
    auto                      ents = entries(entry(k, 5, std::string("v\0w", 3)));
    std::unique_ptr<LeafBase> leaf(LeafBase::build(ents));
    leaf->page_id   = 1;
    auto      frame = PageCodec::encode(leaf.get(), 1);
    PageBase *out   = nullptr;
    ASSERT_TRUE(PageCodec::decode(frame.data(), frame.size(), &out).ok());
    auto *got = static_cast<LeafBase *>(out);
    ASSERT_EQ(got->count(), 1U);
    EXPECT_EQ(got->entry(0).key, k);
    delete out;
}

TEST(PageCodec, BitFlipFailsCrc)
{
    auto                      ents = entries(entry("a", 1, "A"), entry("b", 2, "B"));
    std::unique_ptr<LeafBase> leaf(LeafBase::build(ents));
    leaf->page_id = 1;
    auto frame    = PageCodec::encode(leaf.get(), 1);
    frame[kPageFrameHeaderSize + 3] ^= 0xff; // corrupt a body byte
    PageBase *out = nullptr;
    Status    s   = PageCodec::decode(frame.data(), frame.size(), &out);
    EXPECT_EQ(s.code(), Code::kCorruption);
    EXPECT_EQ(out, nullptr);
}

TEST(PageCodec, ShortBufferRejected)
{
    std::array<uint8_t, 4> b = {0, 0, 0, 0};
    PageBase *out  = nullptr;
    EXPECT_EQ(PageCodec::decode(b.data(), b.size(), &out).code(), Code::kInvalidArgument);
}
