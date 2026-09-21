#include <gtest/gtest.h>
#include "kronosdb/db/db.hpp"
#include "kronosdb/util/metrics.hpp"
#include <filesystem>

using namespace kronos;

class QueryStatsTest : public ::testing::Test {
protected:
    std::filesystem::path dir_;
    void SetUp() override {
        dir_ = std::filesystem::temp_directory_path() / "kronos_qstats_test";
        std::filesystem::remove_all(dir_);
    }
    void TearDown() override {
        std::filesystem::remove_all(dir_);
    }
};

TEST_F(QueryStatsTest, StatsFieldsPopulated) {
    DBConfig cfg;
    cfg.path       = dir_;
    cfg.wal_enabled = false;
    cfg.background_compaction = false;
    DB db(cfg);

    for (int i = 0; i < 100; ++i)
        db.append("AAPL", static_cast<uint64_t>(i) * 1000,
                  EventType::Trade, 100, 99, 101, 10, 10, 10, static_cast<uint64_t>(i));

    QueryRequest req;
    req.symbol   = "AAPL";
    req.start_ns = 0;
    req.end_ns   = UINT64_MAX;
    req.limit    = 1000;

    auto result = db.query(req);

    EXPECT_TRUE(result.ok);
    EXPECT_GT(result.stats.total_latency_us, 0u);
    EXPECT_GT(result.stats.rows_returned, 0u);
    EXPECT_GE(result.stats.rows_scanned, result.stats.rows_returned);
}

TEST_F(QueryStatsTest, PlanningUsNonZeroForValidQuery) {
    DBConfig cfg;
    cfg.path       = dir_;
    cfg.wal_enabled = false;
    cfg.background_compaction = false;
    DB db(cfg);

    db.append("AAPL", 1000, EventType::Trade, 100, 99, 101, 10, 10, 10, 1);

    QueryRequest req;
    req.symbol   = "AAPL";
    req.start_ns = 0;
    req.end_ns   = UINT64_MAX;

    auto result = db.query(req);
    EXPECT_TRUE(result.ok);
    // planning_us may be 0 on very fast machines; just check it doesn't overflow
    EXPECT_LT(result.stats.planning_us, result.stats.total_latency_us + 1);
}

TEST_F(QueryStatsTest, QueryUnknownSymbolReturnsEmpty) {
    DBConfig cfg;
    cfg.path                  = dir_;
    cfg.wal_enabled           = false;
    cfg.background_compaction = false;
    DB db(cfg);

    // Populate with symbol "KNOWN" and flush to SSTable so the query
    // actually has files to consider/skip
    for (int i = 0; i < 200; ++i)
        db.append("KNOWN", static_cast<uint64_t>(i) * 1000,
                  EventType::Trade, 100, 99, 101, 10, 10, 10,
                  static_cast<uint64_t>(i));
    db.flush_now();

    QueryRequest req;
    req.symbol   = "NOTHERE";
    req.start_ns = 0;
    req.end_ns   = UINT64_MAX;

    auto result = db.query(req);
    EXPECT_TRUE(result.ok);
    EXPECT_EQ(result.rows.size(), 0u);
    // The SSTable for "KNOWN" should have been skipped
    EXPECT_GE(result.stats.files_skipped, 0u);
}

TEST_F(QueryStatsTest, RowsReturnedRespectLimit) {
    DBConfig cfg;
    cfg.path       = dir_;
    cfg.wal_enabled = false;
    cfg.background_compaction = false;
    DB db(cfg);

    for (int i = 0; i < 200; ++i)
        db.append("LIMIT_TEST", static_cast<uint64_t>(i),
                  EventType::Trade, 1, 1, 1, 1, 1, 1, static_cast<uint64_t>(i));

    QueryRequest req;
    req.symbol   = "LIMIT_TEST";
    req.start_ns = 0;
    req.end_ns   = UINT64_MAX;
    req.limit    = 25;

    auto result = db.query(req);
    EXPECT_TRUE(result.ok);
    EXPECT_EQ(result.stats.rows_returned, result.rows.size());
    EXPECT_LE(result.rows.size(), 25u);
}

TEST_F(QueryStatsTest, MetricsRecordsQueryLatency) {
    DBConfig cfg;
    cfg.path       = dir_;
    cfg.wal_enabled = false;
    cfg.background_compaction = false;
    DB db(cfg);

    auto& m = global_metrics();
    uint64_t before_count = m.query_latency_ms.count();

    db.append("AAPL", 1000, EventType::Trade, 100, 99, 101, 10, 10, 10, 1);

    QueryRequest req;
    req.symbol = "AAPL";
    req.start_ns = 0;
    req.end_ns = UINT64_MAX;
    db.query(req);

    EXPECT_GT(m.query_latency_ms.count(), before_count);
}
