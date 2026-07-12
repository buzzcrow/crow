// PT2: PageCodec round-trip + CRC validation tests.
#include "crowtree/page_codec.h"

#include "crowtree/cell.h"
#include "crowtree/page.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

using namespace crowtree;

namespace {
leaf_entry E(const std::string& k, uint64_t slot, const std::string& v, bool tomb = false) {
  return leaf_entry{k, encode_cell_buf(slot, tomb ? OpKind::kDelete : OpKind::kPut, Slice(v))};
}
// Build a vector of move-only leaf_entry (a braced-init-list can't hold move-only).
template <class... Es>
std::vector<leaf_entry> Entries(Es&&... es) {
  std::vector<leaf_entry> v;
  v.reserve(sizeof...(es));
  (v.push_back(std::forward<Es>(es)), ...);
  return v;
}
}  // namespace

TEST(PageCodec, LeafRoundTrip) {
  auto entries = Entries(E("a", 1, "A"), E("b", 2, "BB"), E("c", 3, "", true));
  std::unique_ptr<LeafBase> leaf(LeafBase::build(entries, 42));
  leaf->page_id = 7;

  auto frame = PageCodec::encode(leaf.get(), 4096);
  EXPECT_EQ(frame.size() % 4096, 0u);  // IU padded

  PageBase* out = nullptr;
  ASSERT_TRUE(PageCodec::decode(frame.data(), frame.size(), &out).ok());
  ASSERT_NE(out, nullptr);
  ASSERT_EQ(out->type, page_type::kLeafBase);
  auto* got = static_cast<LeafBase*>(out);
  EXPECT_EQ(got->page_id, 7u);
  EXPECT_EQ(got->right_sibling(), 42u);
  ASSERT_EQ(got->count(), 3u);
  EXPECT_EQ(got->entry(0).key, "a");
  EXPECT_EQ(got->entry(2).key, "c");
  EXPECT_TRUE(CellView{Slice(got->entry(2).cell)}.is_tombstone());
  delete out;
}

TEST(PageCodec, InnerRoundTrip) {
  std::unique_ptr<InnerBase> inner(InnerBase::build({"m", "t"}, {10, 11, 12}));
  inner->page_id = 3;
  auto frame = PageCodec::encode(inner.get(), 1);  // byte-addressable, no pad

  PageBase* out = nullptr;
  ASSERT_TRUE(PageCodec::decode(frame.data(), frame.size(), &out).ok());
  ASSERT_EQ(out->type, page_type::kInnerBase);
  auto* got = static_cast<InnerBase*>(out);
  EXPECT_EQ(got->page_id, 3u);
  ASSERT_EQ(got->num_children(), 3u);
  ASSERT_EQ(got->num_separators(), 2u);
  EXPECT_EQ(got->child_at(1), 11u);
  EXPECT_EQ(got->separator_at(0), "m");
  delete out;
}

TEST(PageCodec, EmptyLeaf) {
  std::unique_ptr<LeafBase> leaf(LeafBase::build({}));
  leaf->page_id = 0;
  auto frame = PageCodec::encode(leaf.get(), 1);
  PageBase* out = nullptr;
  ASSERT_TRUE(PageCodec::decode(frame.data(), frame.size(), &out).ok());
  EXPECT_EQ(static_cast<LeafBase*>(out)->count(), 0u);
  delete out;
}

TEST(PageCodec, BinaryKeysWithNuls) {
  std::string k("a\0b", 3);
  auto entries = Entries(E(k, 5, std::string("v\0w", 3)));
  std::unique_ptr<LeafBase> leaf(LeafBase::build(entries));
  leaf->page_id = 1;
  auto frame = PageCodec::encode(leaf.get(), 1);
  PageBase* out = nullptr;
  ASSERT_TRUE(PageCodec::decode(frame.data(), frame.size(), &out).ok());
  auto* got = static_cast<LeafBase*>(out);
  ASSERT_EQ(got->count(), 1u);
  EXPECT_EQ(got->entry(0).key, k);
  delete out;
}

TEST(PageCodec, BitFlipFailsCrc) {
  auto entries = Entries(E("a", 1, "A"), E("b", 2, "B"));
  std::unique_ptr<LeafBase> leaf(LeafBase::build(entries));
  leaf->page_id = 1;
  auto frame = PageCodec::encode(leaf.get(), 1);
  frame[kPageFrameHeaderSize + 3] ^= 0xff;  // corrupt a body byte
  PageBase* out = nullptr;
  Status s = PageCodec::decode(frame.data(), frame.size(), &out);
  EXPECT_EQ(s.code(), Code::kCorruption);
  EXPECT_EQ(out, nullptr);
}

TEST(PageCodec, ShortBufferRejected) {
  uint8_t b[4] = {0, 0, 0, 0};
  PageBase* out = nullptr;
  EXPECT_EQ(PageCodec::decode(b, sizeof(b), &out).code(), Code::kInvalidArgument);
}
