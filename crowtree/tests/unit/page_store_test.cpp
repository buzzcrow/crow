// PT1: PageStore backend tests (MemPageStore + FilePageStore).
#include "crowtree/page_store.h"

#include <gtest/gtest.h>

#include <cstdio>
#include <cstdlib>
#include <string>
#include <vector>

using namespace crowtree;

namespace {
std::string TempPath() {
  char tmpl[] = "/tmp/crowtree_ps_XXXXXX";
  int fd = mkstemp(tmpl);
  if (fd >= 0) {
    close(fd);
  }
  return std::string(tmpl);
}
}  // namespace

TEST(PageStore, MemRoundTrip) {
  MemPageStore s(1);
  std::vector<uint8_t> in{1, 2, 3, 4, 5};
  ASSERT_TRUE(s.write_at(100, in.data(), in.size()).ok());
  std::vector<uint8_t> out(in.size(), 0);
  ASSERT_TRUE(s.read_at(100, out.data(), out.size()).ok());
  EXPECT_EQ(in, out);
  EXPECT_GE(s.size(), 105u);
}

TEST(PageStore, MemReadPastEndFails) {
  MemPageStore s(1);
  uint8_t b[4];
  EXPECT_FALSE(s.read_at(0, b, sizeof(b)).ok());
}

TEST(PageStore, MemOverwrite) {
  MemPageStore s(1);
  std::vector<uint8_t> a{9, 9, 9};
  ASSERT_TRUE(s.write_at(0, a.data(), a.size()).ok());
  std::vector<uint8_t> b{1, 2};
  ASSERT_TRUE(s.write_at(0, b.data(), b.size()).ok());
  std::vector<uint8_t> out(3, 0);
  ASSERT_TRUE(s.read_at(0, out.data(), out.size()).ok());
  EXPECT_EQ(out[0], 1);
  EXPECT_EQ(out[1], 2);
  EXPECT_EQ(out[2], 9);
}

TEST(PageStore, FileRoundTripAcrossReopen) {
  std::string path = TempPath();
  std::vector<uint8_t> in{10, 20, 30, 40};
  {
    std::unique_ptr<FilePageStore> s;
    ASSERT_TRUE(FilePageStore::open(path, 4096, &s).ok());
    ASSERT_TRUE(s->write_at(8, in.data(), in.size()).ok());
    ASSERT_TRUE(s->sync().ok());
    EXPECT_EQ(s->iu_size(), 4096u);
  }
  {
    std::unique_ptr<FilePageStore> s;
    ASSERT_TRUE(FilePageStore::open(path, 4096, &s).ok());
    std::vector<uint8_t> out(in.size(), 0);
    ASSERT_TRUE(s->read_at(8, out.data(), out.size()).ok());
    EXPECT_EQ(in, out);
  }
  std::remove(path.c_str());
}
