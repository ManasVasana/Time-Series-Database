#include <gtest/gtest.h>
#include "kronosdb/storage/wal.hpp"
#include "kronosdb/storage/memtable.hpp"
#include "kronosdb/db/recovery.hpp"
#include <filesystem>
#include <vector>

using namespace kronos;

class WalTest : public ::testing::Test {
protected:
    std::filesystem::path dir_;
    void SetUp() override {
        dir_ = std::filesystem::temp_directory_path() / "kronos_wal_test";
        std::filesystem::remove_all(dir_);
        std::filesystem::create_directories(dir_);
    }
    void TearDown() override {
        std::filesystem::remove_all(dir_);
    }

    static Event make_event(uint64_t ts, uint32_t sym, uint64_t seq) {
        Event e{};
        e.timestamp_ns = ts;
        e.symbol_id    = sym;
        e.sequence_id  = seq;
        e.event_type   = EventType::Trade;
        e.price_ticks  = 100;
        return e;
    }
};

TEST_F(WalTest, AppendAndRecover) {
    const int N = 1000;
    {
        WALWriter writer(dir_);
        for (int i = 0; i < N; ++i)
            writer.append(make_event(static_cast<uint64_t>(i) * 1000, 1, static_cast<uint64_t>(i)));
    }
    WALReader reader(dir_);
    auto records = reader.read_all();
    EXPECT_EQ(records.size(), static_cast<size_t>(N));
    for (int i = 0; i < N; ++i) {
        EXPECT_EQ(records[i].event.timestamp_ns, static_cast<uint64_t>(i) * 1000);
        EXPECT_EQ(records[i].event.sequence_id, static_cast<uint64_t>(i));
    }
}

TEST_F(WalTest, RecoveryIntoMemtable) {
    const int N = 500;
    {
        WALWriter writer(dir_);
        for (int i = 0; i < N; ++i)
            writer.append(make_event(1000ULL * i, 1, static_cast<uint64_t>(i)));
    }
    MemTable mem;
    auto result = Recovery::recover(dir_, mem);
    EXPECT_EQ(result.records_recovered, static_cast<uint64_t>(N));
    EXPECT_EQ(mem.count(), static_cast<size_t>(N));
    EXPECT_FALSE(result.torn_detected);
}

TEST_F(WalTest, EmptyDirRecovery) {
    std::filesystem::path empty = dir_ / "empty_wal";
    MemTable mem;
    auto result = Recovery::recover(empty, mem);
    EXPECT_EQ(result.records_recovered, 0u);
    EXPECT_EQ(mem.count(), 0u);
}

TEST_F(WalTest, SegmentRotation) {
    // Force segment rotation by using a tiny max size
    // 78 bytes per record, 10 records per segment = 780 bytes
    WALWriter writer(dir_, 78 * 10);
    for (int i = 0; i < 25; ++i)
        writer.append(make_event(static_cast<uint64_t>(i), 1, static_cast<uint64_t>(i)));

    // Should have created at least 2 segments
    int seg_count = 0;
    for (auto& entry : std::filesystem::directory_iterator(dir_)) {
        if (entry.is_regular_file()) ++seg_count;
    }
    EXPECT_GT(seg_count, 1);

    // All records recoverable
    WALReader reader(dir_);
    auto records = reader.read_all();
    EXPECT_EQ(records.size(), 25u);
}

TEST_F(WalTest, RecoveryPreservesOrder) {
    const int N = 100;
    {
        WALWriter writer(dir_);
        for (int i = 0; i < N; ++i)
            writer.append(make_event(static_cast<uint64_t>(i), 1, static_cast<uint64_t>(i)));
    }
    WALReader reader(dir_);
    auto records = reader.read_all();
    ASSERT_EQ(records.size(), static_cast<size_t>(N));
    for (size_t i = 1; i < records.size(); ++i)
        EXPECT_LE(records[i-1].file_offset, records[i].file_offset);
}
