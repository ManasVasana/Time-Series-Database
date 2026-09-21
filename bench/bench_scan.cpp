#include <benchmark/benchmark.h>
#include "kronosdb/db/db.hpp"
#include "kronosdb/table/sstable.hpp"
#include <filesystem>

static std::filesystem::path bench_dir() {
    return std::filesystem::temp_directory_path() / "kronos_bench_scan";
}

// Pre-build a DB with data once per process
struct ScanFixture {
    std::filesystem::path dir;
    kronos::DB*           db{nullptr};

    ScanFixture() {
        dir = bench_dir();
        std::filesystem::remove_all(dir);
        kronos::DBConfig cfg;
        cfg.path                  = dir;
        cfg.wal_enabled           = false;
        cfg.background_compaction = false;
        cfg.memtable_flush_bytes  = 256 * 1024 * 1024;
        db = new kronos::DB(cfg);

        // Insert 1M events across 10 symbols
        for (uint64_t i = 0; i < 1'000'000; ++i) {
            char sym[16];
            snprintf(sym, sizeof(sym), "SYM%04u", (unsigned)(i % 10));
            db->append(sym, i * 1000, kronos::EventType::Trade,
                       100, 99, 101, 10, 10, 10, i);
        }
        db->flush_now();
    }

    ~ScanFixture() {
        delete db;
        std::filesystem::remove_all(dir);
    }
};

static ScanFixture& fixture() {
    static ScanFixture f;
    return f;
}

static void BM_FullSymbolScan(benchmark::State& state) {
    auto& f = fixture();
    for (auto _ : state) {
        kronos::QueryRequest req;
        req.symbol   = "SYM0000";
        req.start_ns = 0;
        req.end_ns   = UINT64_MAX;
        req.limit    = 10'000;
        auto result = f.db->query(req);
        benchmark::DoNotOptimize(result.rows.size());
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * 100'000LL);
}

static void BM_NarrowRangeScan(benchmark::State& state) {
    auto& f = fixture();
    for (auto _ : state) {
        kronos::QueryRequest req;
        req.symbol   = "SYM0001";
        req.start_ns = 1'000'000;
        req.end_ns   = 2'000'000;
        req.limit    = 10'000;
        auto result = f.db->query(req);
        benchmark::DoNotOptimize(result.rows.size());
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()));
}

static void BM_ScanWithEventTypeFilter(benchmark::State& state) {
    auto& f = fixture();
    for (auto _ : state) {
        kronos::QueryRequest req;
        req.symbol     = "SYM0002";
        req.start_ns   = 0;
        req.end_ns     = UINT64_MAX;
        req.event_type = kronos::EventType::Trade;
        req.limit      = 1000;
        auto result = f.db->query(req);
        benchmark::DoNotOptimize(result.rows.size());
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()));
}

static void BM_SSTableDirectScan(benchmark::State& state) {
    auto& f = fixture();
    auto dir = f.dir / "l0";
    if (!std::filesystem::exists(dir)) dir = f.dir;

    std::filesystem::path sst_path;
    for (auto& entry : std::filesystem::recursive_directory_iterator(f.dir)) {
        if (entry.path().extension() == ".sst") {
            sst_path = entry.path();
            break;
        }
    }
    if (sst_path.empty()) {
        state.SkipWithError("no SSTable found");
        return;
    }

    kronos::Key start{0, 0, 0};
    kronos::Key end{UINT32_MAX, UINT64_MAX, UINT64_MAX};

    for (auto _ : state) {
        kronos::SSTableReader reader(sst_path);
        size_t count = 0;
        reader.scan(start, end, [&](const kronos::Event&) {
            ++count;
            return count < 10000;
        });
        benchmark::DoNotOptimize(count);
    }
}

BENCHMARK(BM_FullSymbolScan)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_NarrowRangeScan)->Unit(benchmark::kMicrosecond);
BENCHMARK(BM_ScanWithEventTypeFilter)->Unit(benchmark::kMicrosecond);
BENCHMARK(BM_SSTableDirectScan)->Unit(benchmark::kMillisecond);
