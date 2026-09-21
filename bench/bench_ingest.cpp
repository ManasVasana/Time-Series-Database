#include <benchmark/benchmark.h>
#include "kronosdb/db/db.hpp"
#include <filesystem>
#include <cstdio>

static std::filesystem::path bench_dir() {
    return std::filesystem::temp_directory_path() / "kronos_bench_ingest";
}

static void BM_AppendSingleThread(benchmark::State& state) {
    auto dir = bench_dir();
    std::filesystem::remove_all(dir);

    kronos::DBConfig cfg;
    cfg.path                  = dir;
    cfg.wal_enabled           = true;
    cfg.background_compaction = false;
    cfg.memtable_flush_bytes  = 256 * 1024 * 1024;
    kronos::DB db(cfg);

    uint64_t seq = 0;
    for (auto _ : state) {
        const uint64_t s = seq++;
        db.append("AAPL", s * 1000, kronos::EventType::Trade,
                  100, 99, 101, 100, 200, 200, s);
    }

    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()));
    state.SetBytesProcessed(
        static_cast<int64_t>(state.iterations()) *
        static_cast<int64_t>(kronos::Event::kEncodedSize));

    std::filesystem::remove_all(dir);
}

static void BM_AppendNoWAL(benchmark::State& state) {
    auto dir = bench_dir();
    std::filesystem::remove_all(dir);

    kronos::DBConfig cfg;
    cfg.path                  = dir;
    cfg.wal_enabled           = false;
    cfg.background_compaction = false;
    cfg.memtable_flush_bytes  = 256 * 1024 * 1024;
    kronos::DB db(cfg);

    uint64_t seq = 0;
    for (auto _ : state) {
        const uint64_t s = seq++;
        db.append("AAPL", s * 1000, kronos::EventType::Trade,
                  100, 99, 101, 100, 200, 200, s);
    }

    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()));
    std::filesystem::remove_all(dir);
}

static void BM_AppendMultipleSymbols(benchmark::State& state) {
    auto dir = bench_dir();
    std::filesystem::remove_all(dir);

    kronos::DBConfig cfg;
    cfg.path                  = dir;
    cfg.wal_enabled           = false;
    cfg.background_compaction = false;
    cfg.memtable_flush_bytes  = 256 * 1024 * 1024;
    kronos::DB db(cfg);

    const int kSymbols = 100;
    uint64_t seq = 0;
    for (auto _ : state) {
        const uint64_t s = seq++;
        char sym[16];
        snprintf(sym, sizeof(sym), "SYM%04u", static_cast<unsigned>(s % kSymbols));
        db.append(sym, s * 1000, kronos::EventType::Quote,
                  0, 100, 102, 0, 200, 300, s);
    }

    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()));
    std::filesystem::remove_all(dir);
}

BENCHMARK(BM_AppendSingleThread)
    ->Unit(benchmark::kMicrosecond)
    ->Iterations(1'000'000);

BENCHMARK(BM_AppendNoWAL)
    ->Unit(benchmark::kMicrosecond)
    ->Iterations(1'000'000);

BENCHMARK(BM_AppendMultipleSymbols)
    ->Unit(benchmark::kMicrosecond)
    ->Iterations(1'000'000);
