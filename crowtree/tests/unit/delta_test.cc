// CT8: delta record tests (build, FindKey, chain resolve, tombstone shadow).
#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "crowtree/cell.h"
#include "crowtree/delta.h"
#include "crowtree/page.h"

using namespace crowtree;

namespace {
LeafEntry E(const std::string& k, uint64_t slot, const std::string& v) {
  return LeafEntry{k, EncodeCell(slot, OpKind::kPut, Slice(v))};
}
LeafEntry Tomb(const std::string& k, uint64_t slot) {
  return LeafEntry{k, EncodeCell(slot, OpKind::kDelete)};
}
// Free an entire chain (deltas + base).
void FreeChain(PageBase* head) {
  while (head != nullptr) {
    PageBase* next = head->next;
    delete head;
    head = next;
  }
}
}  // namespace

TEST(Delta, BuildAndFindKey) {
  auto* d = BatchDelta::Build(5, {E("a", 5, "x"), E("c", 5, "y"), E("e", 5, "z")},
                              nullptr);
  std::unique_ptr<BatchDelta> guard(d);
  EXPECT_EQ(d->slot(), 5u);
  EXPECT_EQ(d->count(), 3u);
  EXPECT_EQ(d->FindKey("a"), 0);
  EXPECT_EQ(d->FindKey("c"), 1);
  EXPECT_EQ(d->FindKey("e"), 2);
  EXPECT_EQ(d->FindKey("b"), -1);
  EXPECT_EQ(d->FindKey("z"), -1);
}

TEST(Delta, ChainLinkage) {
  auto* base = LeafBase::Build({E("a", 1, "a1")});
  auto* d1 = BatchDelta::Build(2, {E("b", 2, "b2")}, base);
  auto* d2 = BatchDelta::Build(3, {E("c", 3, "c3")}, d1);
  EXPECT_EQ(d2->delta_len, 2u);
  EXPECT_GT(d2->chain_bytes, d1->chain_bytes);
  EXPECT_EQ(d2->next, d1);
  EXPECT_EQ(d1->next, base);
  FreeChain(d2);
}

TEST(Delta, ResolveHighestSlotWins) {
  // base has a@1; d1 overwrites a@4; d2 has b@5.
  auto* base = LeafBase::Build({E("a", 1, "old")});
  auto* d1 = BatchDelta::Build(4, {E("a", 4, "new")}, base);
  auto* d2 = BatchDelta::Build(5, {E("b", 5, "b5")}, d1);

  CellView v;
  ASSERT_TRUE(ResolveChain(d2, "a", &v));
  EXPECT_EQ(v.slot(), 4u);
  EXPECT_EQ(v.value().ToString(), "new");

  ASSERT_TRUE(ResolveChain(d2, "b", &v));
  EXPECT_EQ(v.value().ToString(), "b5");

  EXPECT_FALSE(ResolveChain(d2, "missing", &v));
  FreeChain(d2);
}

TEST(Delta, ResolveOutOfOrderSlots) {
  // A lower-slot delta prepended above a higher-slot base entry: highest wins.
  auto* base = LeafBase::Build({E("a", 10, "ten")});
  auto* d1 = BatchDelta::Build(4, {E("a", 4, "four")}, base);  // stale re-apply
  CellView v;
  ASSERT_TRUE(ResolveChain(d1, "a", &v));
  EXPECT_EQ(v.slot(), 10u);  // base's higher slot still wins
  EXPECT_EQ(v.value().ToString(), "ten");
  FreeChain(d1);
}

TEST(Delta, TombstoneShadows) {
  auto* base = LeafBase::Build({E("a", 1, "v")});
  auto* d1 = BatchDelta::Build(2, {Tomb("a", 2)}, base);
  CellView v;
  ASSERT_TRUE(ResolveChain(d1, "a", &v));
  EXPECT_TRUE(v.is_tombstone());
  EXPECT_EQ(v.slot(), 2u);
  FreeChain(d1);
}

TEST(Delta, ChainLeafBaseHelper) {
  auto* base = LeafBase::Build({E("a", 1, "v")});
  auto* d1 = BatchDelta::Build(2, {E("b", 2, "w")}, base);
  EXPECT_EQ(ChainLeafBase(d1), base);
  EXPECT_EQ(ChainLeafBase(base), base);
  FreeChain(d1);
}
