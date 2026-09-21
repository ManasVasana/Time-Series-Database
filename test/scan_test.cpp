#include <gtest/gtest.h>
#include "kronosdb/db/db.hpp"
#include <filesystem>
#include <chrono>

using namespace kronos;

class ScanTest : public ::testing::Test {
protected:
    std::filesystem::path dir_;
    void SetUp() override {
        dir_ = std::filesystem::temp_directory_path() / "kronos_scan_test";
        std::filesystem::remove_all(dir_);
    }
    void TearDown() override {
        std::filesystem::remove_all(dir_);
    }
};

TEST_F(ScanTest, BasicQueryFromMemtable) {
    DBConfig cfg;
    cfg.path       = dir_;
    cfg.wal_enabled = false;
    cfg.background_compaction = false;
    DB db(cfg);

    uint64_t base_ns = 1'746'000'000'000'000'000ULL;
    for (int i = 0; i < 100; ++i) {
        db.append("AAPL", base_ns + static_cast<uint64_t>(i) * 1'000'000,
                  EventType::Quote, 0, 100, 102, 0, 100, 100, 0);
    }

    QueryRequest req;
    req.symbol   = "AAPL";
    req.start_ns = base_ns;
    req.end_ns   = base_ns + 200'000'000;

    auto result = db.query(req);
    EXPECT_TRUE(result.ok);
    EXPECT_GE(result.rows.size(), 1u);
    EXPECT_LE(result.rows.size(), 100u);
    for (auto& e : result.rows) {
        EXPECT_GE(e.timestamp_ns, base_ns);
        EXPECT_LE(e.timestamp_ns, base_ns + 200'000'000);
    }
}

TEST_F(ScanTest, QueryNonExistentSymbolReturnsEmpty) {
    DBConfig cfg;
    cfg.path       = dir_;
    cfg.wal_enabled = false;
    cfg.background_compaction = false;
    DB db(cfg);

    db.append("AAPL", 1'000'000, EventType::Trade, 100, 99, 101, 10, 10, 10, 1);

    QueryRequest req;
    req.symbol   = "MSFT";
    req.start_ns = 0;
    req.end_ns   = UINT64_MAX;
    auto result = db.query(req);
    EXPECT_TRUE(result.ok);
    EXPECT_EQ(result.rows.size(), 0u);
}

TEST_F(ScanTest, LimitRespected) {
    DBConfig cfg;
    cfg.path       = dir_;
    cfg.wal_enabled = false;
    cfg.background_compaction = false;
    DB db(cfg);

    for (int i = 0; i < 500; ++i)
        db.append("AAPL", static_cast<uint64_t>(i) * 1000,
                  EventType::Trade, 100, 99, 101, 10, 10, 10, static_cast<uint64_t>(i));

    QueryRequest req;
    req.symbol   = "AAPL";
    req.start_ns = 0;
    req.end_ns   = UINT64_MAX;
    req.limit    = 50;
    auto result = db.query(req);
    EXPECT_TRUE(result.ok);
    EXPECT_EQ(result.rows.size(), 50u);
}

TEST_F(ScanTest, EventTypeFilter) {
    DBConfig cfg;
    cfg.path       = dir_;
    cfg.wal_enabled = false;
    cfg.background_compaction = false;
    DB db(cfg);

    for (int i = 0; i < 100; ++i) {
        EventType et = (i % 2 == 0) ? EventType::Trade : EventType::Quote;
        db.append("AAPL", static_cast<uint64_t>(i) * 1000,
                  et, 100, 99, 101, 10, 10, 10, static_cast<uint64_t>(i));
    }

    QueryRequest req;
    req.symbol     = "AAPL";
    req.start_ns   = 0;
    req.end_ns     = UINT64_MAX;
    req.event_type = EventType::Quote;
    auto result = db.query(req);
    EXPECT_TRUE(result.ok);
    for (auto& e : result.rows)
        EXPECT_EQ(e.event_type, EventType::Quote);
}

TEST_F(ScanTest, QueryAfterFlushToSSTable) {
    DBConfig cfg;
    cfg.path                  = dir_;
    cfg.wal_enabled           = false;
    cfg.background_compaction = false;
    cfg.memtable_flush_bytes  = 1; // force immediate flush
    DB db(cfg);

    for (int i = 0; i < 10; ++i)
        db.append("NVDA", static_cast<uint64_t>(i) * 1'000'000,
                  EventType::Trade, 500, 499, 501, 10, 10, 10, static_cast<uint64_t>(i));

    db.flush_now();

    QueryRequest req;
    req.symbol   = "NVDA";
    req.start_ns = 0;
    req.end_ns   = UINT64_MAX;
    auto result = db.query(req);
    EXPECT_TRUE(result.ok);
    EXPECT_GT(result.rows.size(), 0u);
}
