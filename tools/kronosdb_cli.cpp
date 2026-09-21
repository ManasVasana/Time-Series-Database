#include "kronosdb/db/db.hpp"
#include "kronosdb/db/recovery.hpp"
#include <iostream>
#include <string>
#include <sstream>
#include <charconv>
#include <cstring>

// Lightweight HTTP client for API calls (uses /v1/* endpoints)
#include "httplib.h"
#include <nlohmann/json.hpp>
using json = nlohmann::json;

static void usage() {
    std::cerr <<
R"(Usage: kronosdb-cli <command> [options]

Commands:
  status              --host <h> --port <p>
  append              --host <h> --port <p> --symbol <s> --event-type <t>
                      --timestamp-ns <ns> [--price <p>] [--bid <b>] [--ask <a>]
  query               --host <h> --port <p> --symbol <s>
                      --start-ns <ns> --end-ns <ns> [--type <t>] [--limit <n>]
  inspect-levels      --host <h> --port <p>
  recover             --db <path>
  bench               ingest --events <n> [--symbols <n>] [--host <h>] [--port <p>]

)";
}

struct CliOpts {
    std::string host      = "127.0.0.1";
    int         port      = 7420;
    std::string db_path   = "./data";
    std::string symbol;
    std::string event_type = "trade";
    uint64_t    timestamp_ns = 0;
    int64_t     price      = 0;
    int64_t     bid        = 0;
    int64_t     ask        = 0;
    uint32_t    qty        = 100;
    uint64_t    start_ns   = 0;
    uint64_t    end_ns     = UINT64_MAX;
    std::string filter_type;
    uint32_t    limit      = 100;
    uint64_t    events     = 1'000'000;
    uint32_t    symbols    = 100;
};

static std::string get(const std::string& host, int port, const std::string& path) {
    httplib::Client cli(host, port);
    cli.set_connection_timeout(5);
    auto res = cli.Get(path.c_str());
    if (!res) return R"({"ok":false,"error":{"code":"CONNECT","message":"connection failed"}})";
    return res->body;
}

static std::string post(const std::string& host, int port,
                        const std::string& path, const std::string& body) {
    httplib::Client cli(host, port);
    cli.set_connection_timeout(5);
    auto res = cli.Post(path.c_str(), body, "application/json");
    if (!res) return R"({"ok":false,"error":{"code":"CONNECT","message":"connection failed"}})";
    return res->body;
}

static int cmd_status(const CliOpts& o) {
    auto body = get(o.host, o.port, "/v1/status");
    auto j = json::parse(body, nullptr, false);
    if (j.is_discarded()) { std::cerr << body << "\n"; return 1; }
    std::cout << j.dump(2) << "\n";
    return 0;
}

static int cmd_append(const CliOpts& o) {
    json req = {
        {"symbol",        o.symbol},
        {"timestamp_ns",  o.timestamp_ns},
        {"event_type",    o.event_type},
        {"price_ticks",   o.price},
        {"bid_px_ticks",  o.bid},
        {"ask_px_ticks",  o.ask},
        {"quantity",      o.qty}
    };
    auto body = post(o.host, o.port, "/v1/ingest/event", req.dump());
    std::cout << json::parse(body, nullptr, false).dump(2) << "\n";
    return 0;
}

static int cmd_query(const CliOpts& o) {
    json req = {
        {"symbol",   o.symbol},
        {"start_ns", o.start_ns},
        {"end_ns",   o.end_ns},
        {"limit",    o.limit}
    };
    if (!o.filter_type.empty()) req["event_type"] = o.filter_type;
    auto body = post(o.host, o.port, "/v1/query", req.dump());
    auto j = json::parse(body, nullptr, false);
    if (j.is_discarded()) { std::cerr << body << "\n"; return 1; }
    if (j.contains("rows")) {
        std::cout << "Returned " << j["rows"].size() << " row(s)\n";
        if (j.contains("stats"))
            std::cout << "Stats: " << j["stats"].dump() << "\n";
    } else {
        std::cout << j.dump(2) << "\n";
    }
    return 0;
}

static int cmd_inspect_levels(const CliOpts& o) {
    auto body = get(o.host, o.port, "/v1/storage/levels");
    std::cout << json::parse(body, nullptr, false).dump(2) << "\n";
    return 0;
}

static int cmd_recover(const CliOpts& o) {
    std::cout << "Recovering WAL from: " << o.db_path << "\n";
    kronos::MemTable mem;
    auto result = kronos::Recovery::recover(
        std::filesystem::path(o.db_path) / "wal", mem);
    std::cout << "  records_recovered : " << result.records_recovered << "\n"
              << "  torn_detected     : " << result.torn_detected     << "\n"
              << "  torn_bytes        : " << result.torn_bytes         << "\n"
              << "  duration_ms       : " << result.duration_ms        << "\n";
    return 0;
}

static int cmd_bench_ingest(const CliOpts& o) {
    json req = {
        {"benchmark", "ingest"},
        {"events",    o.events},
        {"symbols",   o.symbols}
    };
    auto body = post(o.host, o.port, "/v1/benchmark/run", req.dump());
    std::cout << json::parse(body, nullptr, false).dump(2) << "\n";
    return 0;
}

int main(int argc, char* argv[]) {
    if (argc < 2) { usage(); return 1; }

    std::string cmd = argv[1];
    CliOpts opts;
    std::string sub_cmd;

    for (int i = 2; i < argc; ++i) {
        std::string a = argv[i];
        if      (a == "--host"         && i+1 < argc) opts.host         = argv[++i];
        else if (a == "--port"         && i+1 < argc) opts.port         = std::stoi(argv[++i]);
        else if (a == "--db"           && i+1 < argc) opts.db_path      = argv[++i];
        else if (a == "--symbol"       && i+1 < argc) opts.symbol       = argv[++i];
        else if (a == "--event-type"   && i+1 < argc) opts.event_type   = argv[++i];
        else if (a == "--timestamp-ns" && i+1 < argc) opts.timestamp_ns = std::stoull(argv[++i]);
        else if (a == "--price"        && i+1 < argc) opts.price        = std::stoll(argv[++i]);
        else if (a == "--bid"          && i+1 < argc) opts.bid          = std::stoll(argv[++i]);
        else if (a == "--ask"          && i+1 < argc) opts.ask          = std::stoll(argv[++i]);
        else if (a == "--start-ns"     && i+1 < argc) opts.start_ns     = std::stoull(argv[++i]);
        else if (a == "--end-ns"       && i+1 < argc) opts.end_ns       = std::stoull(argv[++i]);
        else if (a == "--type"         && i+1 < argc) opts.filter_type  = argv[++i];
        else if (a == "--limit"        && i+1 < argc) opts.limit        = std::stoul(argv[++i]);
        else if (a == "--events"       && i+1 < argc) opts.events       = std::stoull(argv[++i]);
        else if (a == "--symbols"      && i+1 < argc) opts.symbols      = std::stoul(argv[++i]);
        else { sub_cmd = a; }
    }

    if (cmd == "status")          return cmd_status(opts);
    if (cmd == "append")          return cmd_append(opts);
    if (cmd == "query")           return cmd_query(opts);
    if (cmd == "inspect-levels")  return cmd_inspect_levels(opts);
    if (cmd == "recover")         return cmd_recover(opts);
    if (cmd == "bench" && sub_cmd == "ingest") return cmd_bench_ingest(opts);

    std::cerr << "Unknown command: " << cmd << "\n";
    usage();
    return 1;
}
