// PT2: PageCodec round-trip + CRC validation tests.
#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "crowtree/cell.h"
#include "crowtree/page.h"
#include "crowtree/page_codec.h"

using namespace crowtree;

namespace {
LeafEntry E(const std::string& k, uint64_t slot, const std::string& v,
            bool tomb = false) {
  return LeafEntry{k, EncodeCell(slot, tomb ? OpKind::kDelete : OpKind::kPut,
                                 Slice(v))};
}
}  // namespace

TEST(PageCodec, LeafRoundTrip) {
  std::vector<LeafEntry> entries{E("a", 1, "A"), E("b", 2, "BB"),
                                 E("c", 3, "", true)};
  std::unique_ptr<LeafBase> leaf(LeafBase::Build(entries, 42));
  leaf->pid = 7;

  auto frame = PageCodec::Encode(leaf.get(), 4096);
  EXPECT_EQ(frame.size() % 4096, 0u);  // IU padded

  PageBase* out = nullptr;
  ASSERT_TRUE(PageCodec::Decode(frame.data(), frame.size(), &out).ok());
  ASSERT_NE(out, nullptr);
  ASSERT_EQ(out->type, PageType::kLeafBase);
  auto* got = static_cast<LeafBase*>(out);
  EXPECT_EQ(got->pid, 7u);
  EXPECT_EQ(got->right_sibling(), 42u);
  ASSERT_EQ(got->count(), 3u);
  EXPECT_EQ(got->entry(0).key, "a");
  EXPECT_EQ(got->entry(2).key, "c");
  EXPECT_TRUE(CellView{Slice(got->entry(2).cell)}.is_tombstone());
  delete out;
}

TEST(PageCodec, InnerRoundTrip) {
  std::unique_ptr<InnerBase> inner(
      InnerBase::Build({"m", "t"}, {10, 11, 12}));
  inner->pid = 3;
  auto frame = PageCodec::Encode(inner.get(), 1);  // byte-addressable, no pad

  PageBase* out = nullptr;
  ASSERT_TRUE(PageCodec::Decode(frame.data(), frame.size(), &out).ok());
  ASSERT_EQ(out->type, PageType::kInnerBase);
  auto* got = static_cast<InnerBase*>(out);
  EXPECT_EQ(got->pid, 3u);
  ASSERT_EQ(got->num_children(), 3u);
  ASSERT_EQ(got->num_separators(), 2u);
  EXPECT_EQ(got->child_at(1), 11u);
  EXPECT_EQ(got->separator_at(0), "m");
  delete out;
}

TEST(PageCodec, EmptyLeaf) {
  std::unique_ptr<LeafBase> leaf(LeafBase::Build({}));
  leaf->pid = 0;
  auto frame = PageCodec::Encode(leaf.get(), 1);
  PageBase* out = nullptr;
  ASSERT_TRUE(PageCodec::Decode(frame.data(), frame.size(), &out).ok());
  EXPECT_EQ(static_cast<LeafBase*>(out)->count(), 0u);
  delete out;
}

TEST(PageCodec, BinaryKeysWithNuls) {
  std::string k("a\0b", 3);
  std::vector<LeafEntry> entries{E(k, 5, std::string("v\0w", 3))};
  std::unique_ptr<LeafBase> leaf(LeafBase::Build(entries));
  leaf->pid = 1;
  auto frame = PageCodec::Encode(leaf.get(), 1);
  PageBase* out = nullptr;
  ASSERT_TRUE(PageCodec::Decode(frame.data(), frame.size(), &out).ok());
  auto* got = static_cast<LeafBase*>(out);
  ASSERT_EQ(got->count(), 1u);
  EXPECT_EQ(got->entry(0).key, k);
  delete out;
}

TEST(PageCodec, BitFlipFailsCrc) {
  std::vector<LeafEntry> entries{E("a", 1, "A"), E("b", 2, "B")};
  std::unique_ptr<LeafBase> leaf(LeafBase::Build(entries));
  leaf->pid = 1;
  auto frame = PageCodec::Encode(leaf.get(), 1);
  frame[kPageFrameHeaderSize + 3] ^= 0xff;  // corrupt a body byte
  PageBase* out = nullptr;
  Status s = PageCodec::Decode(frame.data(), frame.size(), &out);
  EXPECT_EQ(s.code(), Code::kCorruption);
  EXPECT_EQ(out, nullptr);
}

TEST(PageCodec, ShortBufferRejected) {
  uint8_t b[4] = {0, 0, 0, 0};
  PageBase* out = nullptr;
  EXPECT_EQ(PageCodec::Decode(b, sizeof(b), &out).code(), Code::kInvalidArgument);
}
