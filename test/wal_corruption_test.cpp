#include <gtest/gtest.h>
#include "kronosdb/storage/wal.hpp"
#include "kronosdb/db/recovery.hpp"
#include <filesystem>
#include <fstream>
#include <vector>
#include <cstring>

using namespace kronos;

class WalCorruptionTest : public ::testing::Test {
protected:
    std::filesystem::path dir_;
    void SetUp() override {
        dir_ = std::filesystem::temp_directory_path() / "kronos_corrupt_test";
        std::filesystem::remove_all(dir_);
        std::filesystem::create_directories(dir_);
    }
    void TearDown() override {
        std::filesystem::remove_all(dir_);
    }

    static Event make_event(uint64_t seq) {
        Event e{};
        e.timestamp_ns = seq * 1000;
        e.symbol_id    = 1;
        e.sequence_id  = seq;
        e.event_type   = EventType::Trade;
        e.price_ticks  = 200;
        return e;
    }

    // Truncate the WAL file to simulate a torn write
    void truncate_wal(size_t remove_bytes) {
        for (auto& entry : std::filesystem::directory_iterator(dir_)) {
            if (!entry.is_regular_file()) continue;
            auto sz = std::filesystem::file_size(entry.path());
            if (sz > remove_bytes)
                std::filesystem::resize_file(entry.path(), sz - remove_bytes);
        }
    }

    // Flip bytes at offset in the WAL file to corrupt a CRC
    void corrupt_bytes(size_t offset, size_t len) {
        for (auto& entry : std::filesystem::directory_iterator(dir_)) {
            if (!entry.is_regular_file()) continue;
            std::fstream f(entry.path(), std::ios::in | std::ios::out | std::ios::binary);
            f.seekp(static_cast<std::streamoff>(offset));
            for (size_t i = 0; i < len; ++i) {
                char c = '\xAA';
                f.write(&c, 1);
            }
        }
    }
};

TEST_F(WalCorruptionTest, TruncatedTailRecovery) {
    const int N = 100;
    {
        WALWriter writer(dir_);
        for (int i = 0; i < N; ++i)
            writer.append(make_event(static_cast<uint64_t>(i)));
    }

    // Remove last 39 bytes (half a record)
    truncate_wal(39);

    size_t torn_bytes = 0;
    bool   torn = false;
    WALReader reader(dir_);
    auto records = reader.read_all(&torn_bytes, &torn);

    EXPECT_TRUE(torn);
    EXPECT_GT(torn_bytes, 0u);
    // All complete records before the torn one must be recovered
    EXPECT_GE(records.size(), static_cast<size_t>(N - 1));
}

TEST_F(WalCorruptionTest, ChecksumFailureStopsReplay) {
    const int N = 50;
    {
        WALWriter writer(dir_);
        for (int i = 0; i < N; ++i)
            writer.append(make_event(static_cast<uint64_t>(i)));
    }

    // Corrupt bytes at offset 20 (in the middle of record 0, hitting CRC field)
    // Record format: magic(4)+version(1)+len(4)+crc(4)+payload(65)=78 bytes
    // CRC field starts at offset 9
    corrupt_bytes(9, 4);

    size_t torn_bytes = 0;
    bool   torn = false;
    WALReader reader(dir_);
    auto records = reader.read_all(&torn_bytes, &torn);

    // First record has bad CRC -> replay stops at record 0
    EXPECT_TRUE(torn);
    EXPECT_EQ(records.size(), 0u);  // first record is corrupt, none recovered
}

TEST_F(WalCorruptionTest, BadMagicDetected) {
    {
        WALWriter writer(dir_);
        writer.append(make_event(0));
        writer.append(make_event(1));
    }

    // Zero out the magic of the first record
    corrupt_bytes(0, 4);

    size_t torn_bytes = 0;
    bool   torn = false;
    WALReader reader(dir_);
    auto records = reader.read_all(&torn_bytes, &torn);

    EXPECT_TRUE(torn);
    EXPECT_EQ(records.size(), 0u);
}

TEST_F(WalCorruptionTest, PartialRecordAtEnd) {
    const int N = 20;
    {
        WALWriter writer(dir_);
        for (int i = 0; i < N; ++i)
            writer.append(make_event(static_cast<uint64_t>(i)));
    }

    // Append some garbage at the end to simulate OS crash mid-write
    for (auto& entry : std::filesystem::directory_iterator(dir_)) {
        if (!entry.is_regular_file()) continue;
        std::ofstream f(entry.path(), std::ios::binary | std::ios::app);
        const uint8_t garbage[] = {0x4B, 0x52, 0x4F, 0x4E, 0x01, 0x41, 0x00};
        f.write(reinterpret_cast<const char*>(garbage), sizeof(garbage));
    }

    size_t torn_bytes = 0;
    bool   torn = false;
    WALReader reader(dir_);
    auto records = reader.read_all(&torn_bytes, &torn);

    EXPECT_TRUE(torn);
    EXPECT_EQ(records.size(), static_cast<size_t>(N));  // all valid records recovered
}
