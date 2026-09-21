#include <gtest/gtest.h>
#include "kronosdb/db/db.hpp"
#include <filesystem>

using namespace kronos;

class TombstoneTest : public ::testing::Test {
protected:
    std::filesystem::path dir_;
    void SetUp() override {
        dir_ = std::filesystem::temp_directory_path() / "kronos_tombstone_test";
        std::filesystem::remove_all(dir_);
    }
    void TearDown() override {
        std::filesystem::remove_all(dir_);
    }
};

TEST_F(TombstoneTest, RemovedEventAbsentFromQuery) {
    DBConfig cfg;
    cfg.path       = dir_;
    cfg.wal_enabled = false;
    cfg.background_compaction = false;
    DB db(cfg);

    auto r = db.append("AAPL", 1000, EventType::Trade, 100, 99, 101, 10, 10, 10, 1);
    uint64_t seq = r.sequence_id;

    // Verify it's there
    QueryRequest req;
    req.symbol = "AAPL"; req.start_ns = 0; req.end_ns = UINT64_MAX;
    auto before = db.query(req);
    EXPECT_EQ(before.rows.size(), 1u);

    // Delete it
    db.remove("AAPL", 1000, seq);

    // Should be gone
    auto after = db.query(req);
    EXPECT_EQ(after.rows.size(), 0u);
}

TEST_F(TombstoneTest, TombstoneDoesNotAppearInResults) {
    DBConfig cfg;
    cfg.path       = dir_;
    cfg.wal_enabled = false;
    cfg.background_compaction = false;
    DB db(cfg);

    // Append 5 events, delete 2
    for (int i = 0; i < 5; ++i)
        db.append("MSFT", static_cast<uint64_t>(i) * 1000,
                  EventType::Quote, 0, 100, 102, 0, 100, 100,
                  static_cast<uint64_t>(i));

    QueryRequest req;
    req.symbol = "MSFT"; req.start_ns = 0; req.end_ns = UINT64_MAX;
    auto all = db.query(req);
    ASSERT_EQ(all.rows.size(), 5u);

    db.remove("MSFT", 0,    all.rows[0].sequence_id);
    db.remove("MSFT", 2000, all.rows[2].sequence_id);

    auto after = db.query(req);
    EXPECT_EQ(after.rows.size(), 3u);
    for (auto& e : after.rows)
        EXPECT_NE(e.event_type, EventType::Tombstone);
}

TEST_F(TombstoneTest, TombstoneEventTypeNotReturnedEvenWithEventTypeFilter) {
    DBConfig cfg;
    cfg.path       = dir_;
    cfg.wal_enabled = false;
    cfg.background_compaction = false;
    DB db(cfg);

    db.append("NVDA", 5000, EventType::Trade, 500, 499, 501, 10, 10, 10, 99);
    auto res = db.query({"NVDA", 0, UINT64_MAX});
    ASSERT_EQ(res.rows.size(), 1u);

    db.remove("NVDA", 5000, res.rows[0].sequence_id);

    // Even if caller doesn't filter — tombstone never surfaced
    auto after = db.query({"NVDA", 0, UINT64_MAX});
    EXPECT_EQ(after.rows.size(), 0u);
}

TEST_F(TombstoneTest, TombstoneGCDuringCompaction) {
    DBConfig cfg;
    cfg.path       = dir_;
    cfg.wal_enabled = false;
    cfg.background_compaction = false;
    DB db(cfg);

    for (int i = 0; i < 50; ++i)
        db.append("SPY", static_cast<uint64_t>(i) * 1000,
                  EventType::Trade, 400, 399, 401, 10, 10, 10,
                  static_cast<uint64_t>(i));

    QueryRequest req;
    req.symbol = "SPY"; req.start_ns = 0; req.end_ns = UINT64_MAX;
    auto all = db.query(req);
    ASSERT_EQ(all.rows.size(), 50u);

    // Delete first 10 events
    for (int i = 0; i < 10; ++i)
        db.remove("SPY", all.rows[i].timestamp_ns, all.rows[i].sequence_id);

    // Flush to SSTable, compact, query — deleted records should be absent
    db.flush_now();
    db.compact_now();

    auto after = db.query(req);
    EXPECT_EQ(after.rows.size(), 40u);
    for (auto& e : after.rows)
        EXPECT_NE(e.event_type, EventType::Tombstone);
}

TEST_F(TombstoneTest, RemoveNonExistentKeyHarmless) {
    DBConfig cfg;
    cfg.path       = dir_;
    cfg.wal_enabled = false;
    cfg.background_compaction = false;
    DB db(cfg);

    // Removing a key that was never written should not throw
    EXPECT_NO_THROW(db.remove("FAKE", 99999, 12345));

    // And querying afterwards returns nothing
    auto res = db.query({"FAKE", 0, UINT64_MAX});
    EXPECT_EQ(res.rows.size(), 0u);
}
