#include <gtest/gtest.h>
#include "kronosdb/table/sstable.hpp"
#include "kronosdb/storage/memtable.hpp"
#include <filesystem>
#include <vector>

using namespace kronos;

class SSTableTest : public ::testing::Test {
protected:
    std::filesystem::path dir_;
    void SetUp() override {
        dir_ = std::filesystem::temp_directory_path() / "kronos_sst_test";
        std::filesystem::remove_all(dir_);
        std::filesystem::create_directories(dir_);
    }
    void TearDown() override {
        std::filesystem::remove_all(dir_);
    }

    static Event make_event(uint32_t sym, uint64_t ts, uint64_t seq, EventType et = EventType::Trade) {
        Event e{};
        e.symbol_id    = sym;
        e.timestamp_ns = ts;
        e.sequence_id  = seq;
        e.event_type   = et;
        e.price_ticks  = static_cast<int64_t>(ts % 10000);
        e.bid_qty      = 100;
        e.ask_qty      = 200;
        return e;
    }

    std::filesystem::path sst_path(int n = 0) {
        return dir_ / (std::string("test_") + std::to_string(n) + ".sst");
    }
};

TEST_F(SSTableTest, WriteReadRoundTrip) {
    std::vector<Event> events;
    for (int i = 0; i < 100; ++i)
        events.push_back(make_event(1, static_cast<uint64_t>(i) * 1000, static_cast<uint64_t>(i)));

    {
        SSTableWriter writer(sst_path(), 0, 1);
        for (auto& e : events) writer.add(e);
        writer.finish();
    }

    SSTableReader reader(sst_path());
    EXPECT_EQ(reader.info().meta.event_count, 100u);

    std::vector<Event> read_back;
    reader.scan(Key{1,0,0}, Key{1, UINT64_MAX, UINT64_MAX},
        [&](const Event& e) { read_back.push_back(e); return true; });

    ASSERT_EQ(read_back.size(), 100u);
    for (int i = 0; i < 100; ++i) {
        EXPECT_EQ(read_back[i].timestamp_ns, static_cast<uint64_t>(i) * 1000);
        EXPECT_EQ(read_back[i].sequence_id,  static_cast<uint64_t>(i));
    }
}

TEST_F(SSTableTest, MayContainSkipsWrongSymbol) {
    for (int i = 0; i < 50; ++i) {
        SSTableWriter writer(sst_path(), 0, 1);
        for (int j = 0; j < 10; ++j)
            writer.add(make_event(2, static_cast<uint64_t>(j), static_cast<uint64_t>(j)));
        writer.finish();
    }

    SSTableReader reader(sst_path());
    // Symbol 1 not in this table (only symbol 2)
    EXPECT_FALSE(reader.may_contain(Key{1, 0, 0}, Key{1, UINT64_MAX, UINT64_MAX}));
    EXPECT_TRUE(reader.may_contain(Key{2, 0, 0}, Key{2, UINT64_MAX, UINT64_MAX}));
}

TEST_F(SSTableTest, RangeScanRespectsLimits) {
    SSTableWriter writer(sst_path(), 0, 1);
    for (uint64_t ts = 0; ts < 10000; ts += 100)
        writer.add(make_event(1, ts, ts / 100));
    writer.finish();

    SSTableReader reader(sst_path());
    std::vector<Event> result;
    reader.scan(Key{1, 2000, 0}, Key{1, 5000, UINT64_MAX},
        [&](const Event& e) { result.push_back(e); return true; });

    EXPECT_FALSE(result.empty());
    for (auto& e : result) {
        EXPECT_GE(e.timestamp_ns, 2000u);
        EXPECT_LE(e.timestamp_ns, 5000u);
    }
}

TEST_F(SSTableTest, FlushFromMemtable) {
    MemTable mem;
    for (int sym = 1; sym <= 3; ++sym)
        for (int t = 0; t < 100; ++t)
            mem.insert(make_event(static_cast<uint32_t>(sym),
                                  static_cast<uint64_t>(t) * 1000,
                                  static_cast<uint64_t>(sym * 1000 + t)));

    auto events = mem.to_sorted_events();
    SSTableWriter writer(sst_path(), 0, 1);
    for (auto& e : events) writer.add(e);
    auto info = writer.finish();

    EXPECT_EQ(info.meta.event_count, 300u);

    SSTableReader reader(sst_path());
    int count = 0;
    reader.scan(Key{2, 0, 0}, Key{2, UINT64_MAX, UINT64_MAX},
        [&](const Event& e) { ++count; EXPECT_EQ(e.symbol_id, 2u); return true; });
    EXPECT_EQ(count, 100);
}

TEST_F(SSTableTest, EarlyTermination) {
    SSTableWriter writer(sst_path(), 0, 1);
    for (int i = 0; i < 1000; ++i)
        writer.add(make_event(1, static_cast<uint64_t>(i), static_cast<uint64_t>(i)));
    writer.finish();

    SSTableReader reader(sst_path());
    int count = 0;
    reader.scan(Key{1, 0, 0}, Key{1, UINT64_MAX, UINT64_MAX},
        [&](const Event&) { return ++count < 10; });
    EXPECT_EQ(count, 10);
}
