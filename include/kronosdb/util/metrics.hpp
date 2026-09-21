#pragma once
#include <atomic>
#include <array>
#include <cstdint>
#include <chrono>

namespace kronos {

// Power-of-2 histogram with 64 buckets (microseconds)
// bucket[i] covers [2^(i-1), 2^i) for i>=1, [0,1) for i=0
class LatencyHistogram {
public:
    void record(uint64_t us) noexcept;
    double percentile(double p) const noexcept;   // p in [0,1]
    uint64_t count() const noexcept { return total_.load(std::memory_order_relaxed); }
    void reset() noexcept;

private:
    static constexpr int kBuckets = 64;
    std::atomic<uint64_t> buckets_[kBuckets]{};
    std::atomic<uint64_t> total_{0};
    std::atomic<uint64_t> sum_{0};

    static int bucket_for(uint64_t us) noexcept;
};

// Exponential moving average for throughput (events/s, MB/s)
class ThroughputTracker {
public:
    void add(uint64_t n) noexcept;  // add n events/bytes
    double rate() const noexcept;   // approximate events/s or bytes/s
    void reset() noexcept;

private:
    mutable std::atomic<double>   ema_{0.0};
    mutable std::atomic<uint64_t> last_ns_{0};
    mutable std::atomic<uint64_t> pending_{0};
};

struct Metrics {
    // counters
    std::atomic<uint64_t> events_ingested_total{0};
    std::atomic<uint64_t> wal_bytes_written{0};
    std::atomic<uint64_t> memtable_bytes{0};
    std::atomic<uint32_t> immutable_memtable_count{0};
    std::atomic<uint64_t> flush_count{0};
    std::atomic<uint64_t> compaction_count{0};
    std::atomic<uint64_t> compaction_debt_bytes{0};
    std::atomic<uint64_t> block_cache_hits{0};
    std::atomic<uint64_t> block_cache_lookups{0};

    // amplification (stored as scaled ints, divide by 100 for float)
    std::atomic<uint32_t> write_amp_x100{100};  // 1.00
    std::atomic<uint32_t> read_amp_x100{100};
    std::atomic<uint32_t> space_amp_x100{100};

    // histograms
    LatencyHistogram append_latency_us;
    LatencyHistogram query_latency_ms;

    // throughput trackers
    ThroughputTracker ingest_rate;   // events/s
    ThroughputTracker wal_rate;      // bytes/s

    // server start time
    std::chrono::steady_clock::time_point start_time{std::chrono::steady_clock::now()};

    double uptime_seconds() const noexcept;
    double block_cache_hit_rate() const noexcept;
};

// Global metrics instance used by DB and Server
Metrics& global_metrics();

} // namespace kronos
