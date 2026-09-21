#include <gtest/gtest.h>
#include "kronosdb/db/compaction.hpp"
#include "kronosdb/table/sstable.hpp"
#include "kronosdb/storage/memtable.hpp"
#include <filesystem>
#include <algorithm>
#include <vector>

using namespace kronos;

class CompactionTest : public ::testing::Test {
protected:
    std::filesystem::path dir_;
    void SetUp() override {
        dir_ = std::filesystem::temp_directory_path() / "kronos_compact_test";
        std::filesystem::remove_all(dir_);
        std::filesystem::create_directories(dir_);
    }
    void TearDown() override {
        std::filesystem::remove_all(dir_);
    }

    static Event make_event(uint32_t sym, uint64_t ts, uint64_t seq) {
        Event e{};
        e.symbol_id    = sym;
        e.timestamp_ns = ts;
        e.sequence_id  = seq;
        e.event_type   = EventType::Trade;
        e.price_ticks  = 100;
        return e;
    }

    SSTableInfo write_sst(const std::string& name, const std::vector<Event>& events,
                          uint32_t level = 0, uint64_t seq = 1) {
        auto path = dir_ / name;
        SSTableWriter writer(path, level, seq);
        for (auto& e : events) writer.add(e);
        return writer.finish();
    }
};

TEST_F(CompactionTest, MergePreservesSortedOrder) {
    // Build two disjoint SSTables
    std::vector<Event> a_events, b_events;
    for (int i = 0; i < 50; ++i)
        a_events.push_back(make_event(1, static_cast<uint64_t>(i)*2,   static_cast<uint64_t>(i)));
    for (int i = 0; i < 50; ++i)
        b_events.push_back(make_event(1, static_cast<uint64_t>(i)*2+1, static_cast<uint64_t>(50+i)));

    auto a_info = write_sst("a.sst", a_events, 0, 1);
    auto b_info = write_sst("b.sst", b_events, 0, 2);

    auto out_dir = dir_ / "l1";
    auto outputs = Compaction::run({a_info, b_info}, out_dir, 1, 100);

    ASSERT_FALSE(outputs.empty());

    std::vector<Event> merged;
    for (auto& info : outputs) {
        SSTableReader reader(info.path);
        reader.scan(Key{1,0,0}, Key{1,UINT64_MAX,UINT64_MAX},
            [&](const Event& e) { merged.push_back(e); return true; });
    }

    EXPECT_EQ(merged.size(), 100u);
    for (size_t i = 1; i < merged.size(); ++i)
        EXPECT_LE(merged[i-1].key(), merged[i].key());
}

TEST_F(CompactionTest, CompactionStatsPopulated) {
    std::vector<Event> events;
    for (int i = 0; i < 100; ++i)
        events.push_back(make_event(1, static_cast<uint64_t>(i), static_cast<uint64_t>(i)));

    auto info = write_sst("input.sst", events, 0, 1);

    CompactionStats stats{};
    Compaction::run({info}, dir_ / "out", 1, 200, 64*1024*1024, &stats);

    EXPECT_EQ(stats.input_files, 1u);
    EXPECT_GT(stats.records_in, 0u);
    EXPECT_GT(stats.input_bytes, 0u);
    EXPECT_GT(stats.output_bytes, 0u);
    EXPECT_GT(stats.duration_ms, 0.0);
}

TEST_F(CompactionTest, CompactionScoreL0) {
    // L0: 4 files -> score = 4/8 = 0.5
    EXPECT_DOUBLE_EQ(Compaction::compaction_score(0, 4, 0), 0.5);
    // L0: 8 files -> score = 1.0
    EXPECT_DOUBLE_EQ(Compaction::compaction_score(0, 8, 0), 1.0);
    // L0: 16 files -> score = 2.0
    EXPECT_DOUBLE_EQ(Compaction::compaction_score(0, 16, 0), 2.0);
}

TEST_F(CompactionTest, CompactionScoreL1) {
    size_t target = Compaction::kL1SizeTarget;
    EXPECT_DOUBLE_EQ(Compaction::compaction_score(1, 0, target), 1.0);
    EXPECT_DOUBLE_EQ(Compaction::compaction_score(1, 0, target/2), 0.5);
}

TEST_F(CompactionTest, MultipleInputFilesMerged) {
    std::vector<SSTableInfo> inputs;
    for (int f = 0; f < 5; ++f) {
        std::vector<Event> events;
        for (int i = 0; i < 20; ++i)
            events.push_back(make_event(1,
                static_cast<uint64_t>(f*20+i)*1000,
                static_cast<uint64_t>(f*20+i)));
        std::string name = "f" + std::to_string(f) + ".sst";
        inputs.push_back(write_sst(name, events, 0, static_cast<uint64_t>(f+1)));
    }

    auto out_dir = dir_ / "merged";
    auto outputs = Compaction::run(inputs, out_dir, 1, 100);
    ASSERT_FALSE(outputs.empty());

    size_t total = 0;
    for (auto& info : outputs) {
        SSTableReader r(info.path);
        r.scan(Key{1,0,0}, Key{1,UINT64_MAX,UINT64_MAX},
            [&](const Event&) { ++total; return true; });
    }
    EXPECT_EQ(total, 100u);
}
