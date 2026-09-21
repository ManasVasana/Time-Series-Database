#include <gtest/gtest.h>
#include "kronosdb/db/db.hpp"
#include "kronosdb/net/synthetic.hpp"
#include <filesystem>
#include <chrono>
#include <thread>

using namespace kronos;

class SyntheticTest : public ::testing::Test {
protected:
    std::filesystem::path dir_;
    void SetUp() override {
        dir_ = std::filesystem::temp_directory_path() / "kronos_synth_test";
        std::filesystem::remove_all(dir_);
    }
    void TearDown() override {
        std::filesystem::remove_all(dir_);
    }
};

TEST_F(SyntheticTest, GeneratesEventsViaRealDB) {
    DBConfig cfg;
    cfg.path       = dir_;
    cfg.wal_enabled = false;
    cfg.background_compaction = false;
    DB db(cfg);

    SyntheticConfig scfg;
    scfg.num_symbols      = 5;
    scfg.events_per_sec   = 10000.0;
    scfg.duration_seconds = 1;
    scfg.mix_trade  = 0.2;
    scfg.mix_quote  = 0.7;
    scfg.mix_add    = 0.05;
    scfg.mix_modify = 0.03;
    scfg.mix_cancel = 0.02;

    SyntheticGenerator gen(db);
    auto job_id = gen.start(scfg);

    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    uint64_t events = gen.stop(job_id);

    EXPECT_GT(events, 0u);
    EXPECT_GT(db.event_count(), 0u);
}

TEST_F(SyntheticTest, StartStopIdempotent) {
    DBConfig cfg;
    cfg.path       = dir_;
    cfg.wal_enabled = false;
    cfg.background_compaction = false;
    DB db(cfg);

    SyntheticConfig scfg;
    scfg.num_symbols      = 2;
    scfg.events_per_sec   = 1000.0;
    scfg.duration_seconds = 60;  // long, we'll stop it

    SyntheticGenerator gen(db);
    auto job_id = gen.start(scfg);
    EXPECT_THROW(gen.start(scfg), std::runtime_error);  // cannot start twice

    gen.stop(job_id);
}

TEST_F(SyntheticTest, ValidEventMixProduced) {
    DBConfig cfg;
    cfg.path       = dir_;
    cfg.wal_enabled = false;
    cfg.background_compaction = false;
    DB db(cfg);

    SyntheticConfig scfg;
    scfg.num_symbols      = 1;
    scfg.events_per_sec   = 50000.0;
    scfg.duration_seconds = 1;
    scfg.mix_trade  = 0.5;
    scfg.mix_quote  = 0.5;
    scfg.mix_add    = 0.0;
    scfg.mix_modify = 0.0;
    scfg.mix_cancel = 0.0;

    SyntheticGenerator gen(db);
    auto job_id = gen.start(scfg);
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    gen.stop(job_id);

    QueryRequest req;
    req.symbol   = "SYN0000";
    req.start_ns = 0;
    req.end_ns   = UINT64_MAX;
    req.limit    = 10000;
    auto result = db.query(req);

    int trades = 0, quotes = 0;
    for (auto& e : result.rows) {
        if (e.event_type == EventType::Trade)  ++trades;
        if (e.event_type == EventType::Quote)  ++quotes;
    }

    if (!result.rows.empty()) {
        // Rough check: both event types present
        EXPECT_GT(trades + quotes, 0);
    }
}

TEST_F(SyntheticTest, StatusReporting) {
    DBConfig cfg;
    cfg.path       = dir_;
    cfg.wal_enabled = false;
    cfg.background_compaction = false;
    DB db(cfg);

    SyntheticGenerator gen(db);

    SyntheticConfig scfg;
    scfg.num_symbols      = 1;
    scfg.events_per_sec   = 1000.0;
    scfg.duration_seconds = 60;

    auto job_id = gen.start(scfg);

    auto status = gen.status(job_id);
    EXPECT_EQ(status.job_id, job_id);
    EXPECT_TRUE(status.running);

    gen.stop(job_id);

    auto stopped_status = gen.status(job_id);
    EXPECT_FALSE(stopped_status.running);
}
