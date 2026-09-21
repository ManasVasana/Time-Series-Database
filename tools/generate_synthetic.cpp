#include "kronosdb/db/db.hpp"
#include "kronosdb/net/synthetic.hpp"
#include <iostream>
#include <string>
#include <csignal>
#include <thread>
#include <chrono>
#include <atomic>

static std::atomic<bool> g_stop{false};

static void signal_handler(int) { g_stop.store(true); }

static void usage(const char* prog) {
    std::cerr <<
        "Usage: " << prog << " [options]\n"
        "  --db <path>            data directory (default: ./data)\n"
        "  --symbols <n>          number of symbols (default: 100)\n"
        "  --rate <n>             events per second (default: 100000)\n"
        "  --duration <s>         duration in seconds, 0 = until Ctrl+C (default: 30)\n"
        "  --regime <r>           normal|high_vol|flash_crash (default: normal)\n"
        "  --no-wal               disable WAL\n";
}

int main(int argc, char* argv[]) {
    std::string db_path      = "./data";
    uint32_t    num_symbols  = 100;
    double      rate         = 100000.0;
    uint32_t    duration     = 30;
    std::string regime       = "normal";
    bool        wal          = true;

    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if      (a == "--db"       && i+1 < argc) db_path     = argv[++i];
        else if (a == "--symbols"  && i+1 < argc) num_symbols = std::stoul(argv[++i]);
        else if (a == "--rate"     && i+1 < argc) rate        = std::stod(argv[++i]);
        else if (a == "--duration" && i+1 < argc) duration    = std::stoul(argv[++i]);
        else if (a == "--regime"   && i+1 < argc) regime      = argv[++i];
        else if (a == "--no-wal")                  wal         = false;
        else if (a == "--help")    { usage(argv[0]); return 0; }
        else { std::cerr << "Unknown: " << a << "\n"; usage(argv[0]); return 1; }
    }

    std::signal(SIGINT,  signal_handler);
    std::signal(SIGTERM, signal_handler);

    kronos::DBConfig cfg;
    cfg.path        = db_path;
    cfg.wal_enabled = wal;

    std::cout << "Opening DB at " << db_path << "\n";
    kronos::DB db(cfg);

    kronos::SyntheticConfig scfg;
    scfg.num_symbols      = num_symbols;
    scfg.events_per_sec   = rate;
    scfg.duration_seconds = (duration == 0) ? 86400 : duration;  // 24h if 0
    if (regime == "high_vol")    scfg.volatility = kronos::VolatilityRegime::HighVol;
    else if (regime == "flash_crash") scfg.volatility = kronos::VolatilityRegime::FlashCrash;

    kronos::SyntheticGenerator gen(db);
    auto job_id = gen.start(scfg);

    std::cout << "Generating events at " << rate << " eps, "
              << num_symbols << " symbols, regime=" << regime << "\n"
              << "Press Ctrl+C to stop.\n";

    auto t0 = std::chrono::steady_clock::now();
    while (!g_stop.load()) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        auto status = gen.status(job_id);
        if (!status.running) break;

        double elapsed = std::chrono::duration<double>(
            std::chrono::steady_clock::now() - t0).count();
        double actual_rate = elapsed > 0
            ? static_cast<double>(status.events_generated) / elapsed : 0.0;

        std::cout << "\r  " << status.events_generated
                  << " events  (" << static_cast<uint64_t>(actual_rate) << " eps)    "
                  << std::flush;
    }

    g_stop.store(true);
    uint64_t total = gen.stop(job_id);
    std::cout << "\nDone. Total events: " << total << "\n";
    return 0;
}
