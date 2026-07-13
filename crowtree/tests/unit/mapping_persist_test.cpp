// #14c: segment image + segment directory on-disk format encode/decode.
#include "crowtree/mapping_persist.h"
#include "crowtree/mapping_slot.h"

#include <gtest/gtest.h>

using namespace crowtree;

TEST(MappingPersist, SegmentImageRoundTrip)
{
    SegmentImageHeader hdr;
    hdr.seg_idx    = 7;
    hdr.generation = 3;
    hdr.slot_count = 8;
    hdr.live_count = 2;

    std::vector<uint64_t> words(8, slot_word::kEmpty);
    words[1] = slot_word::pack_unloaded(42, 5);
    words[6] = slot_word::pack_unloaded(0, 1);

    std::vector<uint8_t> buf;
    encode_segment_image(hdr, words, &buf);
    EXPECT_EQ(buf.size(), segment_image_encoded_size(hdr.slot_count));

    SegmentImageHeader    got_hdr;
    std::vector<uint64_t> got_words;
    ASSERT_TRUE(decode_segment_image(buf.data(), buf.size(), &got_hdr, &got_words).ok());
    EXPECT_EQ(got_hdr.seg_idx, hdr.seg_idx);
    EXPECT_EQ(got_hdr.generation, hdr.generation);
    EXPECT_EQ(got_hdr.slot_count, hdr.slot_count);
    EXPECT_EQ(got_hdr.live_count, hdr.live_count);
    EXPECT_EQ(got_words, words);
}

TEST(MappingPersist, SegmentImageToleratesTrailingPadding)
{
    SegmentImageHeader hdr;
    hdr.seg_idx    = 0;
    hdr.generation = 1;
    hdr.slot_count = 4;
    hdr.live_count = 0;
    std::vector<uint64_t> words(4, 0);

    std::vector<uint8_t> buf;
    encode_segment_image(hdr, words, &buf);
    buf.resize(buf.size() + 100, 0xAA); // IU padding a real PageStore extent would add

    SegmentImageHeader    got_hdr;
    std::vector<uint64_t> got_words;
    ASSERT_TRUE(decode_segment_image(buf.data(), buf.size(), &got_hdr, &got_words).ok());
    EXPECT_EQ(got_words, words);
}

TEST(MappingPersist, SegmentImageRejectsBadMagic)
{
    SegmentImageHeader hdr;
    hdr.slot_count = 2;
    std::vector<uint64_t> words(2, 0);
    std::vector<uint8_t>  buf;
    encode_segment_image(hdr, words, &buf);
    buf[0] ^= 0xff;

    SegmentImageHeader    got_hdr;
    std::vector<uint64_t> got_words;
    EXPECT_FALSE(decode_segment_image(buf.data(), buf.size(), &got_hdr, &got_words).ok());
}

TEST(MappingPersist, SegmentImageRejectsHeaderCrcTamper)
{
    SegmentImageHeader hdr;
    hdr.seg_idx    = 1;
    hdr.slot_count = 2;
    std::vector<uint64_t> words(2, 0);
    std::vector<uint8_t>  buf;
    encode_segment_image(hdr, words, &buf);
    buf[8] ^= 0xff; // seg_idx byte, inside the header, after header_crc was computed

    SegmentImageHeader    got_hdr;
    std::vector<uint64_t> got_words;
    EXPECT_FALSE(decode_segment_image(buf.data(), buf.size(), &got_hdr, &got_words).ok());
}

TEST(MappingPersist, SegmentImageRejectsBodyCrcTamper)
{
    SegmentImageHeader hdr;
    hdr.slot_count              = 4;
    std::vector<uint64_t> words = {0, slot_word::pack_unloaded(1, 1), 0, 0};
    std::vector<uint8_t>  buf;
    encode_segment_image(hdr, words, &buf);
    // Flip a byte inside the body (past the fixed header).
    buf[32] ^= 0xff;

    SegmentImageHeader    got_hdr;
    std::vector<uint64_t> got_words;
    EXPECT_FALSE(decode_segment_image(buf.data(), buf.size(), &got_hdr, &got_words).ok());
}

TEST(MappingPersist, SegmentImageRejectsShortBuffer)
{
    SegmentImageHeader hdr;
    hdr.slot_count = 4;
    std::vector<uint64_t> words(4, 0);
    std::vector<uint8_t>  buf;
    encode_segment_image(hdr, words, &buf);
    buf.resize(buf.size() - 5); // truncate into the body

    SegmentImageHeader    got_hdr;
    std::vector<uint64_t> got_words;
    EXPECT_FALSE(decode_segment_image(buf.data(), buf.size(), &got_hdr, &got_words).ok());
}

TEST(MappingPersist, SegmentDirectoryRoundTrip)
{
    std::vector<DirEntry> entries = {
        DirEntry{.seg_idx = 0, .generation = 1, .image_addr = 4096,  .image_len = 8224, .image_crc = 0x1234},
        DirEntry{.seg_idx = 5, .generation = 9, .image_addr = 20480, .image_len = 8224, .image_crc = 0x5678},
    };
    std::vector<uint8_t> buf;
    encode_segment_directory(entries, &buf);

    std::vector<DirEntry> got;
    ASSERT_TRUE(decode_segment_directory(buf.data(), buf.size(), &got).ok());
    ASSERT_EQ(got.size(), entries.size());
    for (size_t i = 0; i < entries.size(); ++i) {
        EXPECT_EQ(got[i].seg_idx, entries[i].seg_idx);
        EXPECT_EQ(got[i].generation, entries[i].generation);
        EXPECT_EQ(got[i].image_addr, entries[i].image_addr);
        EXPECT_EQ(got[i].image_len, entries[i].image_len);
        EXPECT_EQ(got[i].image_crc, entries[i].image_crc);
    }
}

TEST(MappingPersist, SegmentDirectoryEmpty)
{
    std::vector<uint8_t> buf;
    encode_segment_directory({}, &buf);
    std::vector<DirEntry> got;
    ASSERT_TRUE(decode_segment_directory(buf.data(), buf.size(), &got).ok());
    EXPECT_TRUE(got.empty());
}

TEST(MappingPersist, SegmentDirectoryRejectsBodyCrcTamper)
{
    std::vector<DirEntry> entries = {
        DirEntry{.seg_idx = 3, .generation = 1, .image_addr = 0, .image_len = 8224, .image_crc = 7},
    };
    std::vector<uint8_t> buf;
    encode_segment_directory(entries, &buf);
    buf[16] ^= 0xff; // inside the body (past the 16-byte header)

    std::vector<DirEntry> got;
    EXPECT_FALSE(decode_segment_directory(buf.data(), buf.size(), &got).ok());
}
