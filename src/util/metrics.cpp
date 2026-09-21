#include "kronosdb/util/metrics.hpp"
#include <algorithm>

namespace kronos {

// ── LatencyHistogram ─────────────────────────────────────────────────────────

// bucket_for maps a latency in microseconds to a power-of-2 bucket index.
// bucket[0] = [0, 1), bucket[1] = [1, 2), bucket[2] = [2, 4), ...,
// bucket[i] = [2^(i-1), 2^i) for i >= 1.
int LatencyHistogram::bucket_for(uint64_t us) noexcept {
    if (us == 0) return 0;
    // Number of significant bits in us, clamped to [1, kBuckets-1]
    int b = static_cast<int>(64 - __builtin_clzll(us));  // floor(log2(us)) + 1
    return std::min(b, kBuckets - 1);
}

void LatencyHistogram::record(uint64_t us) noexcept {
    buckets_[bucket_for(us)].fetch_add(1, std::memory_order_relaxed);
    total_.fetch_add(1, std::memory_order_relaxed);
    sum_.fetch_add(us, std::memory_order_relaxed);
}

double LatencyHistogram::percentile(double p) const noexcept {
    // Clamp to valid range to avoid UB from negative or >1.0 inputs
    if (p <= 0.0) return 0.0;
    if (p >= 1.0) p = 1.0;

    uint64_t total = total_.load(std::memory_order_relaxed);
    if (total == 0) return 0.0;

    // Round up to avoid returning p0 as 0 when there is at least one sample
    uint64_t target = static_cast<uint64_t>(p * static_cast<double>(total));
    if (target == 0) target = 1;

    uint64_t cumulative = 0;
    for (int i = 0; i < kBuckets; ++i) {
        cumulative += buckets_[i].load(std::memory_order_relaxed);
        if (cumulative >= target) {
            if (i == 0) return 0.5;
            uint64_t lo = 1ULL << (i - 1);
            uint64_t hi = 1ULL << i;
            return static_cast<double>((lo + hi) / 2);
        }
    }
    return static_cast<double>(1ULL << (kBuckets - 1));
}

void LatencyHistogram::reset() noexcept {
    for (auto& b : buckets_) b.store(0, std::memory_order_relaxed);
    total_.store(0, std::memory_order_relaxed);
    sum_.store(0,   std::memory_order_relaxed);
}

// ── ThroughputTracker ────────────────────────────────────────────────────────
//
// Tracks an exponential moving average of throughput (events/s or bytes/s).
// At most one thread updates the EMA at a time (CAS on last_ns_ acts as a
// mutex for the update window).  Counts accumulated by losing threads are not
// lost — they are added back to pending_ for the next winner to pick up.
// The EMA itself is updated with relaxed ordering; it is an approximate metric
// that only needs to be eventually visible, not sequentially consistent.

void ThroughputTracker::add(uint64_t n) noexcept {
    pending_.fetch_add(n, std::memory_order_relaxed);

    uint64_t now = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count());

    uint64_t last = last_ns_.load(std::memory_order_relaxed);
    if (last == 0) {
        // First call: initialise the timestamp and return; we don't yet have
        // enough data to compute a meaningful rate.
        last_ns_.compare_exchange_strong(last, now,
                                          std::memory_order_release,
                                          std::memory_order_relaxed);
        return;
    }

    double dt_s = static_cast<double>(now - last) / 1e9;
    if (dt_s < 0.1) return;  // update at most every 100 ms

    // Try to win the update window.  Use acq_rel so that the winner sees all
    // pending_ additions from before its compare_exchange, and other threads
    // see the timestamp update before their own loads.
    if (!last_ns_.compare_exchange_strong(last, now,
                                           std::memory_order_acq_rel,
                                           std::memory_order_relaxed))
        return;  // another thread is updating — our pending_ will be included next time

    uint64_t cnt = pending_.exchange(0, std::memory_order_relaxed);
    if (cnt == 0) return;

    double rate    = static_cast<double>(cnt) / dt_s;
    double old_ema = ema_.load(std::memory_order_relaxed);
    // alpha = 0.3: responds to bursts while smoothing noise
    ema_.store(old_ema * 0.7 + rate * 0.3, std::memory_order_relaxed);
}

double ThroughputTracker::rate() const noexcept {
    return ema_.load(std::memory_order_relaxed);
}

void ThroughputTracker::reset() noexcept {
    ema_.store(0.0,   std::memory_order_relaxed);
    last_ns_.store(0, std::memory_order_relaxed);
    pending_.store(0, std::memory_order_relaxed);
}

// ── Metrics ──────────────────────────────────────────────────────────────────

double Metrics::uptime_seconds() const noexcept {
    return std::chrono::duration<double>(
        std::chrono::steady_clock::now() - start_time).count();
}

double Metrics::block_cache_hit_rate() const noexcept {
    uint64_t lookups = block_cache_lookups.load(std::memory_order_relaxed);
    uint64_t hits    = block_cache_hits.load(std::memory_order_relaxed);
    if (lookups == 0) return 0.0;
    return static_cast<double>(hits) / static_cast<double>(lookups);
}

Metrics& global_metrics() {
    static Metrics instance;
    return instance;
}

} // namespace kronos
