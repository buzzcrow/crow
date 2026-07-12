// PT10: page compression (LZ4 default, identity fallback).
#include <gtest/gtest.h>

#include <random>
#include <string>
#include <vector>

#include "crowtree/cell.h"
#include "crowtree/compressor.h"
#include "crowtree/frame_page.h"

using namespace crowtree;

namespace {
constexpr uint32_t kPb = 4096;
std::string Key(int i) {
  char b[16];
  snprintf(b, sizeof(b), "k%05d", i);
  return b;
}
// A highly-compressible leaf frame (repetitive values).
std::vector<uint8_t> CompressibleFrame() {
  std::vector<uint8_t> f(kPb);
  LeafFrameBuilder b(f.data(), kPb);
  for (int i = 0; i < 40; ++i) {
    std::string cell = EncodeCell(i, OpKind::kPut, Slice(std::string(20, 'a')));
    b.TryAppendSorted(Slice(Key(i)), Slice(cell));
  }
  b.Finish(1, kInvalidPID);
  return f;
}
}  // namespace

TEST(PageCompression, RoundTripPreservesFrame) {
  auto frame = CompressibleFrame();
  std::vector<uint8_t> blob;
  EncodeDurablePage(frame.data(), kPb, CompressAlgo::kLz4, &blob);

  std::vector<uint8_t> back(kPb);
  ASSERT_TRUE(DecodeDurablePage(blob.data(), blob.size(), back.data(), kPb).ok());
  EXPECT_EQ(frame, back);
  ASSERT_TRUE(FrameValidate(back.data(), kPb));
}

TEST(PageCompression, CompressibleShrinksWhenLz4Available) {
  auto frame = CompressibleFrame();
  std::vector<uint8_t> blob;
  EncodeDurablePage(frame.data(), kPb, CompressAlgo::kLz4, &blob);
  if (Lz4Available()) {
    EXPECT_LT(blob.size(), static_cast<size_t>(kPb));  // actually compressed
  } else {
    EXPECT_GE(blob.size(), static_cast<size_t>(kPb));  // stored raw
  }
}

TEST(PageCompression, IncompressibleStoredRaw) {
  std::vector<uint8_t> frame(kPb);
  std::mt19937 rng(1234);
  for (auto& b : frame) b = static_cast<uint8_t>(rng());
  std::vector<uint8_t> blob;
  EncodeDurablePage(frame.data(), kPb, CompressAlgo::kLz4, &blob);
  // Random data does not shrink -> stored raw (algo byte 0 = kNone).
  EXPECT_EQ(blob[0], static_cast<uint8_t>(CompressAlgo::kNone));
  std::vector<uint8_t> back(kPb);
  ASSERT_TRUE(DecodeDurablePage(blob.data(), blob.size(), back.data(), kPb).ok());
  EXPECT_EQ(frame, back);
}

TEST(PageCompression, NonePreferStoresRaw) {
  auto frame = CompressibleFrame();
  std::vector<uint8_t> blob;
  EncodeDurablePage(frame.data(), kPb, CompressAlgo::kNone, &blob);
  EXPECT_EQ(blob[0], static_cast<uint8_t>(CompressAlgo::kNone));
  std::vector<uint8_t> back(kPb);
  ASSERT_TRUE(DecodeDurablePage(blob.data(), blob.size(), back.data(), kPb).ok());
  EXPECT_EQ(frame, back);
}

TEST(PageCompression, CrcTamperRejected) {
  auto frame = CompressibleFrame();
  std::vector<uint8_t> blob;
  EncodeDurablePage(frame.data(), kPb, CompressAlgo::kLz4, &blob);
  blob[kDurableBlobHeader + 1] ^= 0xff;  // flip a stored byte
  std::vector<uint8_t> back(kPb);
  EXPECT_EQ(DecodeDurablePage(blob.data(), blob.size(), back.data(), kPb).code(),
            Code::kCorruption);
}

TEST(PageCompression, ShortBlobRejected) {
  uint8_t b[4] = {0};
  std::vector<uint8_t> back(kPb);
  EXPECT_EQ(DecodeDurablePage(b, sizeof(b), back.data(), kPb).code(),
            Code::kInvalidArgument);
}
