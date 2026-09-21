#pragma once
#include <string>
#include <atomic>
#include <thread>
#include <mutex>
#include <cstdint>

namespace kronos {

class DB;

enum class VolatilityRegime { Normal, HighVol, FlashCrash };

struct SyntheticConfig {
    uint32_t         num_symbols{100};
    double           events_per_sec{100000.0};
    uint32_t         duration_seconds{30};
    double           mix_trade{0.2};
    double           mix_quote{0.7};
    double           mix_add{0.05};
    double           mix_modify{0.03};
    double           mix_cancel{0.02};
    VolatilityRegime volatility{VolatilityRegime::Normal};
};

struct SyntheticJobStatus {
    std::string job_id;
    bool        running{false};
    uint64_t    events_generated{0};
};

class SyntheticGenerator {
public:
    explicit SyntheticGenerator(DB& db);
    ~SyntheticGenerator();

    SyntheticGenerator(const SyntheticGenerator&)            = delete;
    SyntheticGenerator& operator=(const SyntheticGenerator&) = delete;

    // Start a generation job.  Throws if a job is already running.
    // Returns the job_id to pass to stop().
    std::string start(const SyntheticConfig& cfg);

    // Stop the running job and block until the worker exits.
    // Returns total events generated.
    uint64_t stop(const std::string& job_id);

    SyntheticJobStatus status(const std::string& job_id) const;

private:
    DB&                   db_;
    mutable std::mutex    control_mu_;   // guards start()/stop() against concurrent calls
    std::string           current_job_id_;
    std::atomic<bool>     running_{false};
    std::atomic<uint64_t> events_generated_{0};
    std::thread           worker_;
    SyntheticConfig       config_;

    void run_loop();
};

} // namespace kronos
