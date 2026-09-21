#include <gtest/gtest.h>
#include "kronosdb/util/block_cache.hpp"
#include <string>
#include <vector>

using namespace kronos;

static std::vector<uint8_t> make_block(size_t sz, uint8_t fill = 0xAB) {
    return std::vector<uint8_t>(sz, fill);
}

TEST(BlockCacheTest, GetMissReturnsNullptr) {
    BlockCache cache(1024 * 1024);
    EXPECT_EQ(cache.get("nonexistent.sst", 0), nullptr);
    EXPECT_EQ(cache.misses(), 1u);
    EXPECT_EQ(cache.hits(),   0u);
}

TEST(BlockCacheTest, PutThenGetHits) {
    BlockCache cache(1024 * 1024);
    auto block = make_block(1024);
    cache.put("file.sst", 0, block);

    auto* result = cache.get("file.sst", 0);
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(*result, block);
    EXPECT_EQ(cache.hits(), 1u);
}

TEST(BlockCacheTest, DifferentOffsetsDifferentEntries) {
    BlockCache cache(1024 * 1024);
    cache.put("file.sst", 0,     make_block(512, 0x11));
    cache.put("file.sst", 512,   make_block(512, 0x22));
    cache.put("file.sst", 1024,  make_block(512, 0x33));

    EXPECT_EQ(cache.get("file.sst",  0)->front(),    0x11);
    EXPECT_EQ(cache.get("file.sst",  512)->front(),  0x22);
    EXPECT_EQ(cache.get("file.sst", 1024)->front(),  0x33);
}

TEST(BlockCacheTest, EvictsLRUWhenFull) {
    // Cache fits exactly 2 blocks of 64 bytes
    BlockCache cache(128);
    cache.put("f.sst", 0,   make_block(64, 0xAA));
    cache.put("f.sst", 64,  make_block(64, 0xBB));

    // Access block 0 to make it most-recently-used
    cache.get("f.sst", 0);

    // Adding a third block should evict block 64 (least recently used)
    cache.put("f.sst", 128, make_block(64, 0xCC));

    EXPECT_NE(cache.get("f.sst",   0), nullptr);   // MRU — still present
    EXPECT_EQ(cache.get("f.sst",  64), nullptr);   // LRU — evicted
    EXPECT_NE(cache.get("f.sst", 128), nullptr);   // just inserted
}

TEST(BlockCacheTest, EvictByPath) {
    BlockCache cache(1024 * 1024);
    cache.put("a.sst", 0, make_block(64));
    cache.put("a.sst", 64, make_block(64));
    cache.put("b.sst", 0, make_block(64));

    cache.evict("a.sst");

    EXPECT_EQ(cache.get("a.sst",  0), nullptr);
    EXPECT_EQ(cache.get("a.sst", 64), nullptr);
    EXPECT_NE(cache.get("b.sst",  0), nullptr);
}

TEST(BlockCacheTest, HitRateAccurate) {
    BlockCache cache(1024 * 1024);
    cache.put("x.sst", 0, make_block(64));

    cache.get("x.sst", 0);     // hit
    cache.get("x.sst", 0);     // hit
    cache.get("x.sst", 999);   // miss

    double hr = cache.hit_rate();
    EXPECT_NEAR(hr, 2.0 / 3.0, 0.01);
}

TEST(BlockCacheTest, SizeBytesTracked) {
    BlockCache cache(1024 * 1024);
    EXPECT_EQ(cache.size_bytes(), 0u);
    cache.put("f.sst", 0, make_block(1000));
    EXPECT_EQ(cache.size_bytes(), 1000u);
    cache.put("f.sst", 1000, make_block(500));
    EXPECT_EQ(cache.size_bytes(), 1500u);
    cache.evict("f.sst");
    EXPECT_EQ(cache.size_bytes(), 0u);
}

TEST(BlockCacheTest, OverwriteUpdatesSize) {
    BlockCache cache(1024 * 1024);
    cache.put("f.sst", 0, make_block(100));
    EXPECT_EQ(cache.size_bytes(), 100u);
    // Overwrite with smaller block
    cache.put("f.sst", 0, make_block(40));
    EXPECT_EQ(cache.size_bytes(), 40u);
}
