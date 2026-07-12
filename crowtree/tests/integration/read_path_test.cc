// CT13: read path (get, multi_get, scan with L0 overlay, iter_all via snapshot).
#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "crowtree/crowtree.h"
#include "crowtree/env.h"

using namespace crowtree;

namespace {
Batch Put1(const std::string& k, const std::string& v) {
  return Batch{{BatchOp{k, OpKind::kPut, v}}};
}
Batch Del1(const std::string& k) {
  return Batch{{BatchOp{k, OpKind::kDelete, ""}}};
}
std::string Key(int i) {
  char buf[16];
  snprintf(buf, sizeof(buf), "k%04d", i);
  return buf;
}
}  // namespace

TEST(ReadPath, GetAfterPutAndDelete) {
  CrowtreeEnv env;
  Crowtree t(env);
  ASSERT_TRUE(t.Apply(1, Put1("a", "A"), 1).ok());
  ASSERT_TRUE(t.Flush().ok());
  std::string v;
  uint64_t s;
  ASSERT_TRUE(t.Get(Slice("a"), &s, &v));
  EXPECT_EQ(v, "A");
  ASSERT_TRUE(t.Apply(2, Del1("a"), 2).ok());
  ASSERT_TRUE(t.Flush().ok());
  EXPECT_FALSE(t.Get(Slice("a"), &s, &v));
}

TEST(ReadPath, L0OverridesL1) {
  CrowtreeEnv env;
  Crowtree t(env);
  ASSERT_TRUE(t.Apply(1, Put1("a", "A1"), 1).ok());
  ASSERT_TRUE(t.Flush().ok());  // A1 in L1
  ASSERT_TRUE(t.Apply(2, Put1("a", "A2"), 2).ok());  // A2 in L0 (not flushed)
  std::string v;
  uint64_t s;
  ASSERT_TRUE(t.Get(Slice("a"), &s, &v));
  EXPECT_EQ(v, "A2");
  EXPECT_EQ(s, 2u);
  // Scan reflects L0 too.
  std::vector<ScanEntry> out;
  bool trunc;
  ASSERT_TRUE(t.Scan(Slice(""), 0, &out, &trunc).ok());
  ASSERT_EQ(out.size(), 1u);
  EXPECT_EQ(out[0].value, "A2");
}

TEST(ReadPath, L0TombstoneHidesL1) {
  CrowtreeEnv env;
  Crowtree t(env);
  ASSERT_TRUE(t.Apply(1, Put1("a", "A1"), 1).ok());
  ASSERT_TRUE(t.Flush().ok());
  ASSERT_TRUE(t.Apply(2, Del1("a"), 2).ok());  // tombstone in L0
  std::string v;
  uint64_t s;
  EXPECT_FALSE(t.Get(Slice("a"), &s, &v));
  std::vector<ScanEntry> out;
  bool trunc;
  ASSERT_TRUE(t.Scan(Slice(""), 0, &out, &trunc).ok());
  EXPECT_TRUE(out.empty());  // tombstone excluded
}

TEST(ReadPath, ScanOrderLimitTruncatedAcrossLeaves) {
  Options opt;
  opt.max_delta_len = 1;
  opt.leaf_split_bytes = 120;  // force multiple leaves
  CrowtreeEnv env;
  Crowtree t(env, opt);
  const int N = 100;
  for (int i = 0; i < N; ++i) {
    uint64_t s = i + 1;
    ASSERT_TRUE(t.Apply(s, Put1(Key(i), "val" + std::to_string(i)), s).ok());
    ASSERT_TRUE(t.Flush().ok());
  }
  ASSERT_GT(t.LeafCount(), 1u);

  // Full scan: sorted, complete.
  std::vector<ScanEntry> out;
  bool trunc;
  ASSERT_TRUE(t.Scan(Slice(""), 0, &out, &trunc).ok());
  ASSERT_EQ(out.size(), static_cast<size_t>(N));
  EXPECT_FALSE(trunc);
  for (int i = 0; i < N; ++i) EXPECT_EQ(out[i].key, Key(i));

  // Limited scan: truncated.
  ASSERT_TRUE(t.Scan(Slice(""), 10, &out, &trunc).ok());
  EXPECT_EQ(out.size(), 10u);
  EXPECT_TRUE(trunc);
  EXPECT_EQ(out[0].key, Key(0));
  EXPECT_EQ(out[9].key, Key(9));
}

TEST(ReadPath, ScanPrefix) {
  CrowtreeEnv env;
  Crowtree t(env);
  ASSERT_TRUE(t.Apply(1, Put1("apple", "1"), 1).ok());
  ASSERT_TRUE(t.Apply(2, Put1("apricot", "2"), 2).ok());
  ASSERT_TRUE(t.Apply(3, Put1("banana", "3"), 3).ok());
  ASSERT_TRUE(t.Flush().ok());
  std::vector<ScanEntry> out;
  bool trunc;
  ASSERT_TRUE(t.Scan(Slice("ap"), 0, &out, &trunc).ok());
  ASSERT_EQ(out.size(), 2u);
  EXPECT_EQ(out[0].key, "apple");
  EXPECT_EQ(out[1].key, "apricot");
}

TEST(ReadPath, MultiGet) {
  CrowtreeEnv env;
  Crowtree t(env);
  ASSERT_TRUE(t.Apply(1, Put1("a", "A"), 1).ok());
  ASSERT_TRUE(t.Apply(2, Put1("c", "C"), 2).ok());
  ASSERT_TRUE(t.Flush().ok());
  std::vector<Slice> keys = {Slice("a"), Slice("b"), Slice("c")};
  auto res = t.MultiGet(keys);
  ASSERT_EQ(res.size(), 3u);
  EXPECT_TRUE(res[0].found);
  EXPECT_EQ(res[0].value, "A");
  EXPECT_FALSE(res[1].found);
  EXPECT_TRUE(res[2].found);
  EXPECT_EQ(res[2].value, "C");
}

TEST(ReadPath, IterAllIncludesTombstones) {
  CrowtreeEnv env;
  Crowtree t(env);
  ASSERT_TRUE(t.Apply(1, Put1("a", "A"), 1).ok());
  ASSERT_TRUE(t.Apply(1, Put1("b", "B"), 1).ok());
  ASSERT_TRUE(t.Flush().ok());
  ASSERT_TRUE(t.Apply(2, Del1("a"), 2).ok());
  ASSERT_TRUE(t.Flush().ok());
  auto snap = t.SnapshotView();
  // iter_all (entries) includes the tombstone for "a".
  EXPECT_EQ(snap->size(), 2u);
  // Scan (live) excludes it.
  std::vector<ScanEntry> out;
  bool trunc;
  ASSERT_TRUE(t.Scan(Slice(""), 0, &out, &trunc).ok());
  ASSERT_EQ(out.size(), 1u);
  EXPECT_EQ(out[0].key, "b");
}
