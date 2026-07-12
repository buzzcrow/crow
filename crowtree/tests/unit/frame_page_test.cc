// PT6a: zero-copy slotted frame format + views.
#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "crowtree/cell.h"
#include "crowtree/frame_page.h"

using namespace crowtree;

namespace {
std::string Cell(uint64_t slot, const std::string& v, bool tomb = false) {
  return EncodeCell(slot, tomb ? OpKind::kDelete : OpKind::kPut, Slice(v));
}
std::string Key(int i) {
  char b[16];
  snprintf(b, sizeof(b), "k%05d", i);
  return b;
}
}  // namespace

TEST(FramePage, LeafBuildViewRoundTrip) {
  const uint32_t pb = 4096;
  std::vector<uint8_t> frame(pb);
  std::vector<std::pair<std::string, std::string>> entries = {
      {"a", Cell(1, "A")}, {"b", Cell(2, "BB")}, {"c", Cell(3, "", true)}};
  LeafFrameBuilder b(frame.data(), pb);
  for (auto& e : entries) ASSERT_TRUE(b.TryAppendSorted(Slice(e.first), Slice(e.second)));
  b.Finish(/*self_pid=*/7, /*right_sibling=*/42);

  ASSERT_TRUE(FrameValidate(frame.data(), pb));
  LeafFrameView v(frame.data(), pb);
  EXPECT_EQ(v.self_pid(), 7u);
  EXPECT_EQ(v.right_sibling(), 42u);
  ASSERT_EQ(v.count(), 3u);
  EXPECT_EQ(v.key(0).ToString(), "a");
  EXPECT_EQ(v.key(2).ToString(), "c");
  EXPECT_EQ(CellView{v.cell(1)}.value().ToString(), "BB");
  EXPECT_TRUE(CellView{v.cell(2)}.is_tombstone());
}

TEST(FramePage, LeafFindAndLowerBound) {
  const uint32_t pb = 4096;
  std::vector<uint8_t> frame(pb);
  LeafFrameBuilder b(frame.data(), pb);
  for (int i = 0; i < 50; ++i) {
    ASSERT_TRUE(b.TryAppendSorted(Slice(Key(i * 2)), Slice(Cell(i, "v"))));
  }
  b.Finish(1, kInvalidPID);
  LeafFrameView v(frame.data(), pb);

  EXPECT_EQ(v.Find(Slice(Key(20))), 10);
  EXPECT_EQ(v.Find(Slice(Key(21))), -1);  // odd keys absent
  EXPECT_EQ(v.LowerBound(Slice(Key(21))), 11u);
  EXPECT_EQ(v.LowerBound(Slice(Key(0))), 0u);
  CellView c;
  ASSERT_TRUE(v.Lookup(Slice(Key(40)), &c));
  EXPECT_EQ(c.slot(), 20u);
}

TEST(FramePage, LeafCapacityRejectsWhenFull) {
  const uint32_t pb = 256;  // tiny frame
  std::vector<uint8_t> frame(pb);
  LeafFrameBuilder b(frame.data(), pb);
  int appended = 0;
  for (int i = 0; i < 1000; ++i) {
    if (!b.TryAppendSorted(Slice(Key(i)), Slice(Cell(i, "value-bytes")))) break;
    ++appended;
  }
  b.Finish(1, kInvalidPID);
  EXPECT_GT(appended, 0);
  EXPECT_LT(appended, 1000);
  ASSERT_TRUE(FrameValidate(frame.data(), pb));
  EXPECT_EQ(LeafFrameView(frame.data(), pb).count(), static_cast<uint32_t>(appended));
}

TEST(FramePage, BinaryKeysWithNuls) {
  const uint32_t pb = 1024;
  std::vector<uint8_t> frame(pb);
  std::string k1("a\0b", 3), k2("a\0c", 3);
  LeafFrameBuilder b(frame.data(), pb);
  ASSERT_TRUE(b.TryAppendSorted(Slice(k1), Slice(Cell(1, std::string("x\0y", 3)))));
  ASSERT_TRUE(b.TryAppendSorted(Slice(k2), Slice(Cell(2, "z"))));
  b.Finish(1, kInvalidPID);
  LeafFrameView v(frame.data(), pb);
  EXPECT_EQ(v.key(0).ToString(), k1);
  EXPECT_EQ(v.Find(Slice(k2)), 1);
}

TEST(FramePage, CrcDetectsCorruption) {
  const uint32_t pb = 1024;
  std::vector<uint8_t> frame(pb);
  LeafFrameBuilder b(frame.data(), pb);
  ASSERT_TRUE(b.TryAppendSorted(Slice("a"), Slice(Cell(1, "A"))));
  b.Finish(1, kInvalidPID);
  ASSERT_TRUE(FrameValidate(frame.data(), pb));
  frame[kFrameHeaderSize + 1] ^= 0xff;  // flip a slot-dir byte
  EXPECT_FALSE(FrameValidate(frame.data(), pb));
}

TEST(FramePage, InnerBuildViewRoundTrip) {
  const uint32_t pb = 4096;
  std::vector<uint8_t> frame(pb);
  std::vector<uint64_t> children = {10, 11, 12};
  std::string s0 = "m", s1 = "t";
  std::vector<Slice> seps = {Slice(s0), Slice(s1)};
  ASSERT_TRUE(InnerFrameBuild(frame.data(), pb, /*self_pid=*/3, children, seps));
  ASSERT_TRUE(FrameValidate(frame.data(), pb));

  InnerFrameView v(frame.data(), pb);
  EXPECT_EQ(v.self_pid(), 3u);
  ASSERT_EQ(v.num_children(), 3u);
  ASSERT_EQ(v.num_separators(), 2u);
  EXPECT_EQ(v.child_at(1), 11u);
  EXPECT_EQ(v.separator_at(0).ToString(), "m");
  // Routing: keys < m -> child 0; [m,t) -> child 1; >= t -> child 2.
  EXPECT_EQ(v.ChildFor(Slice("a")), 10u);
  EXPECT_EQ(v.ChildFor(Slice("m")), 11u);
  EXPECT_EQ(v.ChildFor(Slice("q")), 11u);
  EXPECT_EQ(v.ChildFor(Slice("t")), 12u);
  EXPECT_EQ(v.ChildFor(Slice("z")), 12u);
}

TEST(FramePage, InnerBuildRejectsOversize) {
  const uint32_t pb = 128;
  std::vector<uint8_t> frame(pb);
  std::vector<uint64_t> children = {1, 2};
  std::string big(200, 'x');
  std::vector<Slice> seps = {Slice(big)};
  EXPECT_FALSE(InnerFrameBuild(frame.data(), pb, 1, children, seps));
}
