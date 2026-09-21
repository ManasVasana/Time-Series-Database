#include <benchmark/benchmark.h>
#include "kronosdb/table/sstable.hpp"
#include "kronosdb/storage/memtable.hpp"
#include <filesystem>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>

static std::filesystem::path bench_dir() {
    return std::filesystem::temp_directory_path() / "kronos_bench_io";
}

// Build a test SSTable with N events
static kronos::SSTableInfo build_sst(const std::filesystem::path& path, size_t n) {
    kronos::SSTableWriter writer(path, 0, 1);
    for (size_t i = 0; i < n; ++i) {
        kronos::Event e{};
        e.symbol_id    = 1;
        e.timestamp_ns = i * 1000;
        e.sequence_id  = i;
        e.event_type   = kronos::EventType::Trade;
        e.price_ticks  = 100;
        writer.add(e);
    }
    return writer.finish();
}

static void BM_PreadScan(benchmark::State& state) {
    auto dir = bench_dir();
    std::filesystem::create_directories(dir);
    auto path = dir / "pread_bench.sst";

    // Build once
    static bool built = false;
    if (!built) {
        build_sst(path, 500'000);
        built = true;
    }

    kronos::Key start{1, 0, 0};
    kronos::Key end{1, UINT64_MAX, UINT64_MAX};

    for (auto _ : state) {
        kronos::SSTableReader reader(path);
        size_t count = 0;
        reader.scan(start, end, [&](const kronos::Event&) {
            ++count;
            return true;
        });
        benchmark::DoNotOptimize(count);
        state.SetItemsProcessed(static_cast<int64_t>(count));
    }

    std::filesystem::remove_all(dir);
}

static void BM_MmapScan(benchmark::State& state) {
    // mmap-based scan: map the whole file, read sequentially
    auto dir = bench_dir();
    std::filesystem::create_directories(dir);
    auto path = dir / "mmap_bench.sst";

    {
        kronos::SSTableWriter writer(path, 0, 1);
        for (size_t i = 0; i < 500'000; ++i) {
            kronos::Event e{};
            e.symbol_id    = 1;
            e.timestamp_ns = i * 1000;
            e.sequence_id  = i;
            e.event_type   = kronos::EventType::Trade;
            writer.add(e);
        }
        writer.finish();
    }

    for (auto _ : state) {
        int fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) { state.SkipWithError("open failed"); return; }

        struct stat st{};
        ::fstat(fd, &st);
        size_t sz = static_cast<size_t>(st.st_size);

        void* mapped = ::mmap(nullptr, sz, PROT_READ, MAP_PRIVATE, fd, 0);
        ::madvise(mapped, sz, MADV_SEQUENTIAL);

        // Count bytes to simulate scan
        volatile size_t sum = 0;
        const uint8_t* p = static_cast<const uint8_t*>(mapped);
        for (size_t i = 0; i < sz; i += 64) sum += p[i];

        ::munmap(mapped, sz);
        ::close(fd);
        benchmark::DoNotOptimize(sum);
    }

    std::filesystem::remove_all(dir);
}

static void BM_PreadVsMmapSmallBlock(benchmark::State& state) {
    // Compare pread vs mmap for a small random-access pattern
    auto dir = bench_dir();
    std::filesystem::create_directories(dir);
    auto path = dir / "rnd_bench.sst";
    build_sst(path, 10'000);

    int fd = ::open(path.c_str(), O_RDONLY);
    struct stat st{};
    ::fstat(fd, &st);

    uint8_t buf[65];
    for (auto _ : state) {
        // 100 random pread calls
        for (int i = 0; i < 100; ++i) {
            off_t offset = static_cast<off_t>((i * 78) % (st.st_size - 78));
            ::pread(fd, buf, 65, offset + 13);
            benchmark::DoNotOptimize(buf[0]);
        }
    }
    ::close(fd);
    std::filesystem::remove_all(dir);
}

BENCHMARK(BM_PreadScan)->Unit(benchmark::kMillisecond)->Iterations(5);
BENCHMARK(BM_MmapScan)->Unit(benchmark::kMillisecond)->Iterations(5);
BENCHMARK(BM_PreadVsMmapSmallBlock)->Unit(benchmark::kMicrosecond)->Iterations(10000);
