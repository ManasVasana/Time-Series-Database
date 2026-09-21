#include "kronosdb/net/synthetic.hpp"
#include "kronosdb/db/db.hpp"
#include <random>
#include <chrono>
#include <stdexcept>
#include <mutex>

namespace kronos {

SyntheticGenerator::SyntheticGenerator(DB& db) : db_(db) {}

SyntheticGenerator::~SyntheticGenerator() {
    // Signal and join before destroying so run_loop() doesn't access a dead db_
    bool was_running = running_.exchange(false, std::memory_order_acq_rel);
    if (was_running && worker_.joinable()) worker_.join();
}

std::string SyntheticGenerator::start(const SyntheticConfig& cfg) {
    std::lock_guard lock(control_mu_);
    if (running_.load(std::memory_order_acquire))
        throw std::runtime_error("generator already running");

    // Write config and reset counter BEFORE starting the thread.
    // std::thread construction provides a happens-before edge so the thread
    // will observe these stores without additional fences.
    config_           = cfg;
    current_job_id_   = "synthetic-001";
    events_generated_.store(0, std::memory_order_relaxed);
    running_.store(true, std::memory_order_release);

    worker_ = std::thread([this]() { run_loop(); });
    return current_job_id_;
}

uint64_t SyntheticGenerator::stop(const std::string& job_id) {
    std::lock_guard lock(control_mu_);
    if (job_id != current_job_id_)
        throw std::invalid_argument("unknown job_id: " + job_id);

    running_.store(false, std::memory_order_release);
    if (worker_.joinable()) worker_.join();
    return events_generated_.load(std::memory_order_relaxed);
}

SyntheticJobStatus SyntheticGenerator::status(const std::string& job_id) const {
    if (job_id != current_job_id_)
        return {job_id, false, 0};
    return {current_job_id_,
            running_.load(std::memory_order_acquire),
            events_generated_.load(std::memory_order_relaxed)};
}

void SyntheticGenerator::run_loop() {
    // Take a local copy of config so run_loop doesn't race with a concurrent
    // stop() + start() cycle (which would write config_ while this loop reads it)
    const SyntheticConfig cfg = config_;

    // Pre-build symbol names
    std::vector<std::string> symbols;
    symbols.reserve(cfg.num_symbols);
    for (uint32_t i = 0; i < cfg.num_symbols; ++i) {
        char buf[16];
        snprintf(buf, sizeof(buf), "SYN%04u", i);
        symbols.emplace_back(buf);
    }

    // Build cumulative event-type distribution
    std::array<double, 5> weights = {
        cfg.mix_trade, cfg.mix_quote, cfg.mix_add, cfg.mix_modify, cfg.mix_cancel
    };
    double total_w = 0.0;
    for (auto w : weights) total_w += w;
    std::array<double, 5> cum{};
    double running = 0.0;
    for (int i = 0; i < 5; ++i) {
        running  += weights[i] / total_w;
        cum[i]    = running;
    }

    std::mt19937_64 rng(std::random_device{}());
    std::uniform_real_distribution<double>   udist(0.0, 1.0);
    std::uniform_int_distribution<uint32_t>  sym_dist(0, cfg.num_symbols - 1);
    std::uniform_int_distribution<uint32_t>  qty_dist(1, 5000);
    std::uniform_int_distribution<uint64_t>  oid_dist(1, 1'000'000);

    // Volatility → price spread multiplier
    double vol_mult = 1.0;
    switch (cfg.volatility) {
        case VolatilityRegime::HighVol:    vol_mult = 5.0;  break;
        case VolatilityRegime::FlashCrash: vol_mult = 20.0; break;
        default: break;
    }

    const int64_t  kBasePrice    = 1'000'000;   // 100.0000 in ticks
    const double   ns_per_event  = 1e9 / cfg.events_per_sec;
    const auto     deadline      = std::chrono::steady_clock::now() +
                                   std::chrono::seconds(cfg.duration_seconds);

    auto last_event_time = std::chrono::steady_clock::now();

    while (running_.load(std::memory_order_acquire)) {
        if (std::chrono::steady_clock::now() >= deadline) break;

        uint32_t sym_idx = sym_dist(rng);

        // Event type from cumulative distribution
        double r = udist(rng);
        EventType etype = EventType::Cancel;
        for (int i = 0; i < 5; ++i) {
            if (r <= cum[i]) { etype = static_cast<EventType>(i); break; }
        }

        // Price with vol-regime spread; clamp to avoid negative ticks
        std::normal_distribution<double> px_dist(
            static_cast<double>(kBasePrice),
            static_cast<double>(kBasePrice) * 0.001 * vol_mult);
        int64_t px  = std::max<int64_t>(1, static_cast<int64_t>(px_dist(rng)));
        int64_t spd = std::max<int64_t>(1, px / 10000);  // 1 bp spread

        uint64_t ts_ns = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count());

        try {
            db_.append(
                symbols[sym_idx], ts_ns, etype,
                (etype == EventType::Trade) ? px : 0,
                px - spd, px + spd,
                qty_dist(rng), qty_dist(rng), qty_dist(rng),
                oid_dist(rng));
        } catch (...) {
            // A single failed append must not kill the generator
        }
        events_generated_.fetch_add(1, std::memory_order_relaxed);

        // Rate control: spin-wait until next event slot
        auto next = last_event_time +
                    std::chrono::nanoseconds(static_cast<uint64_t>(ns_per_event));
        while (std::chrono::steady_clock::now() < next &&
               running_.load(std::memory_order_relaxed)) {
            std::this_thread::yield();
        }
        last_event_time = std::chrono::steady_clock::now();
    }

    running_.store(false, std::memory_order_release);
}

} // namespace kronos
