#include <benchmark/benchmark.h>
#include "kronosdb/storage/wal.hpp"
#include "kronosdb/db/recovery.hpp"
#include "kronosdb/storage/memtable.hpp"
#include <filesystem>

static std::filesystem::path wal_dir() {
    return std::filesystem::temp_directory_path() / "kronos_bench_wal";
}

static void write_wal(const std::filesystem::path& dir, uint64_t count) {
    std::filesystem::remove_all(dir);
    kronos::WALWriter writer(dir);
    for (uint64_t i = 0; i < count; ++i) {
        kronos::Event e{};
        e.timestamp_ns = i * 1000;
        e.symbol_id    = static_cast<uint32_t>(i % 100);
        e.sequence_id  = i;
        e.event_type   = kronos::EventType::Trade;
        e.price_ticks  = 100;
        writer.append(e);
    }
}

static void BM_WALAppend(benchmark::State& state) {
    auto dir = wal_dir();
    std::filesystem::remove_all(dir);
    kronos::WALWriter writer(dir);

    uint64_t seq = 0;
    for (auto _ : state) {
        kronos::Event e{};
        e.timestamp_ns = seq * 1000;
        e.symbol_id    = 1;
        e.sequence_id  = seq++;
        e.event_type   = kronos::EventType::Trade;
        e.price_ticks  = 100;
        writer.append(e);
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()));
    state.SetBytesProcessed(
        static_cast<int64_t>(state.iterations()) *
        static_cast<int64_t>(kronos::kWalHeaderSize + kronos::Event::kEncodedSize));

    std::filesystem::remove_all(dir);
}

static void BM_Recovery100K(benchmark::State& state) {
    auto dir = wal_dir();
    write_wal(dir, 100'000);

    for (auto _ : state) {
        kronos::MemTable mem;
        kronos::Recovery::recover(dir, mem);
        benchmark::DoNotOptimize(mem.count());
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * 100'000LL);

    std::filesystem::remove_all(dir);
}

static void BM_Recovery1M(benchmark::State& state) {
    auto dir = wal_dir();
    write_wal(dir, 1'000'000);

    for (auto _ : state) {
        kronos::MemTable mem;
        kronos::Recovery::recover(dir, mem);
        benchmark::DoNotOptimize(mem.count());
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * 1'000'000LL);

    std::filesystem::remove_all(dir);
}

static void BM_WALRead(benchmark::State& state) {
    auto dir = wal_dir();
    write_wal(dir, 100'000);

    for (auto _ : state) {
        kronos::WALReader reader(dir);
        auto records = reader.read_all();
        benchmark::DoNotOptimize(records.size());
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * 100'000LL);

    std::filesystem::remove_all(dir);
}

BENCHMARK(BM_WALAppend)->Unit(benchmark::kMicrosecond)->Iterations(500'000);
BENCHMARK(BM_WALRead)->Unit(benchmark::kMillisecond)->Iterations(5);
BENCHMARK(BM_Recovery100K)->Unit(benchmark::kMillisecond)->Iterations(5);
BENCHMARK(BM_Recovery1M)->Unit(benchmark::kMillisecond)->Iterations(3);
