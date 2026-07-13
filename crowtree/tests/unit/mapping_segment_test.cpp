// #14a: standalone mapping-table segment struct.
#include "crowtree/mapping_segment.h"
#include "crowtree/mapping_slot.h"

#include <gtest/gtest.h>

using namespace crowtree;

TEST(MappingSegment, ConstructsAllSlotsEmpty)
{
    MappingSegment seg(128);
    EXPECT_EQ(seg.slot_count, 128U);
    for (uint32_t i = 0; i < seg.slot_count; ++i) {
        EXPECT_TRUE(slot_word::is_empty(seg.slots[i].load()));
    }
    EXPECT_EQ(seg.live_count.load(), 0U);
    EXPECT_EQ(seg.generation.load(), 0U);
    EXPECT_FALSE(seg.is_dirty());
}

TEST(MappingSegment, SlotsAreIndependentlyMutable)
{
    MappingSegment seg(4);
    uint64_t       w = slot_word::pack_unloaded(42, 7);
    seg.slots[2].store(w);
    seg.live_count.fetch_add(1);
    seg.write_seq.fetch_add(1);

    EXPECT_TRUE(slot_word::is_empty(seg.slots[0].load()));
    EXPECT_TRUE(slot_word::is_empty(seg.slots[1].load()));
    EXPECT_TRUE(slot_word::is_unloaded(seg.slots[2].load()));
    EXPECT_EQ(slot_word::unloaded_iu_index(seg.slots[2].load()), 42U);
    EXPECT_EQ(slot_word::unloaded_iu_count(seg.slots[2].load()), 7U);
    EXPECT_TRUE(slot_word::is_empty(seg.slots[3].load()));
    EXPECT_EQ(seg.live_count.load(), 1U);
    EXPECT_TRUE(seg.is_dirty());
}

TEST(MappingSegment, GenerationBumpsIndependentlyOfSlots)
{
    MappingSegment seg(8);
    seg.generation.fetch_add(1);
    seg.generation.fetch_add(1);
    EXPECT_EQ(seg.generation.load(), 2U);
    // Bumping generation does not itself touch slots or live_count.
    EXPECT_EQ(seg.live_count.load(), 0U);
    for (uint32_t i = 0; i < seg.slot_count; ++i) {
        EXPECT_TRUE(slot_word::is_empty(seg.slots[i].load()));
    }
}

TEST(MappingSegment, DefaultSizeMatchesOptionsDefault)
{
    // Options.mapping_segment_slots default is 1024 (design-crowtree-mappingtable.md §4).
    MappingSegment seg(1024);
    EXPECT_EQ(seg.slot_count, 1024U);
}
