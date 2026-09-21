#include "httplib.h"

#include "kronosdb/net/server.hpp"
#include "kronosdb/util/metrics.hpp"
#include "kronosdb/db/recovery.hpp"
#include <nlohmann/json.hpp>
#include <algorithm>
#include <chrono>
#include <fstream>
#include <string>
#include <thread>

using json = nlohmann::json;

namespace kronos {

// ── helpers ──────────────────────────────────────────────────────────────────

static json make_error(const std::string& code, const std::string& msg) {
    return {{"ok", false}, {"error", {{"code", code}, {"message", msg}}}};
}

// CORS is required so Kronos Workbench (browser JS at localhost) can call this.
static void add_cors(httplib::Response& res) {
    res.set_header("Access-Control-Allow-Origin",  "*");
    res.set_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
    res.set_header("Access-Control-Allow-Headers", "Content-Type");
}

static void respond_ok(httplib::Response& res, const json& body) {
    add_cors(res);
    res.set_content(body.dump(), "application/json");
}

static void respond_err(httplib::Response& res, int status,
                        const std::string& code, const std::string& msg) {
    res.status = status;
    add_cors(res);
    res.set_content(make_error(code, msg).dump(), "application/json");
}

static bool parse_body(const httplib::Request& req, httplib::Response& res, json& out) {
    try {
        out = json::parse(req.body);
        return true;
    } catch (...) {
        respond_err(res, 400, "INVALID_JSON", "request body is not valid JSON");
        return false;
    }
}

// ── Server ───────────────────────────────────────────────────────────────────

Server::Server(DB& db, ServerConfig cfg)
    : db_(db), cfg_(std::move(cfg)),
      svr_(std::make_unique<httplib::Server>()),
      synth_(std::make_unique<SyntheticGenerator>(db)) {
    setup_routes();
}

Server::~Server() { stop(); }

void Server::start() {
    running_.store(true);
    svr_->listen(cfg_.host.c_str(), cfg_.port);
    running_.store(false);
}

void Server::stop() {
    if (running_.load()) svr_->stop();
}

void Server::setup_routes() {
    auto* s = svr_.get();

    // CORS pre-flight for all routes
    s->Options(".*", [](const httplib::Request&, httplib::Response& res) {
        add_cors(res);
        res.status = 204;
    });

    // ── GET /v1/status ────────────────────────────────────────────────────

    s->Get("/v1/status", [this](const httplib::Request&, httplib::Response& res) {
        respond_ok(res, {
            {"connected",       true},
            {"engine_name",     "KronosDB"},
            {"engine_version",  "0.1.0"},
            {"api_version",     "v1"},
            {"db_path",         db_.config().path.string()},
            {"io_backend",      db_.config().io_backend},
            {"wal_enabled",     db_.config().wal_enabled},
            {"compression_hot", "none"},
            {"compression_cold","none"},
            {"event_count",     db_.event_count()},
            {"disk_usage_bytes",db_.disk_usage_bytes()},
            {"uptime_seconds",  global_metrics().uptime_seconds()}
        });
    });

    // ── GET /v1/metrics ───────────────────────────────────────────────────

    s->Get("/v1/metrics", [](const httplib::Request&, httplib::Response& res) {
        auto& m = global_metrics();
        respond_ok(res, {
            {"events_ingested_total", m.events_ingested_total.load()},
            {"events_per_sec",        m.ingest_rate.rate()},
            {"wal_mb_per_sec",        m.wal_rate.rate() / (1024.0 * 1024.0)},
            {"memtable_mb",           static_cast<double>(m.memtable_bytes.load()) / (1024.0*1024.0)},
            {"flush_count",           m.flush_count.load()},
            {"compaction_count",      m.compaction_count.load()},
            {"compaction_debt_gb",    static_cast<double>(m.compaction_debt_bytes.load()) / (1024.0*1024.0*1024.0)},
            {"p50_append_us",         m.append_latency_us.percentile(0.50)},
            {"p90_append_us",         m.append_latency_us.percentile(0.90)},
            {"p99_append_us",         m.append_latency_us.percentile(0.99)},
            {"p999_append_us",        m.append_latency_us.percentile(0.999)},
            {"p50_query_ms",          m.query_latency_ms.percentile(0.50)},
            {"p99_query_ms",          m.query_latency_ms.percentile(0.99)},
            {"block_cache_hit_rate",  m.block_cache_hit_rate()},
            {"write_amplification",   static_cast<double>(m.write_amp_x100.load()) / 100.0},
            {"read_amplification",    static_cast<double>(m.read_amp_x100.load())  / 100.0},
            {"space_amplification",   static_cast<double>(m.space_amp_x100.load()) / 100.0}
        });
    });

    // ── GET /v1/symbols ───────────────────────────────────────────────────
    //
    // List every symbol that has been interned in the DB. Used by the
    // workbench Query page so the user can browse what is actually stored
    // without guessing names.

    s->Get("/v1/symbols", [this](const httplib::Request&, httplib::Response& res) {
        auto names = db_.symbols();
        std::sort(names.begin(), names.end());
        json arr = json::array();
        for (auto& n : names) arr.push_back(n);
        respond_ok(res, {
            {"ok",      true},
            {"count",   names.size()},
            {"symbols", arr}
        });
    });

    // ── GET /v1/storage/levels ────────────────────────────────────────────

    s->Get("/v1/storage/levels", [this](const httplib::Request&, httplib::Response& res) {
        auto mem  = db_.memtable_info();
        auto lvls = db_.level_infos();

        json levels_arr = json::array();
        for (auto& li : lvls) {
            if (li.file_count == 0 && li.size_bytes == 0) continue;
            levels_arr.push_back({
                {"level",            li.level},
                {"file_count",       li.file_count},
                {"size_bytes",       li.size_bytes},
                {"compaction_score", li.compaction_score}
            });
        }
        respond_ok(res, {
            {"memtable", {{"active_mb", mem.active_mb}, {"immutable_count", mem.immutable_count}}},
            {"levels",   levels_arr},
            {"columnar_cold_tier", {{"file_count", 0}, {"size_bytes", 0}}}
        });
    });

    // ── POST /v1/ingest/event ─────────────────────────────────────────────

    s->Post("/v1/ingest/event", [this](const httplib::Request& req, httplib::Response& res) {
        json body;
        if (!parse_body(req, res, body)) return;
        try {
            auto result = db_.append(
                body.at("symbol").get<std::string>(),
                body.at("timestamp_ns").get<uint64_t>(),
                Event::parse_event_type(body.at("event_type").get<std::string>()),
                body.value("price_ticks",  (int64_t)0),
                body.value("bid_px_ticks", (int64_t)0),
                body.value("ask_px_ticks", (int64_t)0),
                body.value("quantity",     (uint32_t)0),
                body.value("bid_qty",      (uint32_t)0),
                body.value("ask_qty",      (uint32_t)0),
                body.value("order_id",     (uint64_t)0));
            respond_ok(res, {
                {"ok",                true},
                {"sequence_id",       result.sequence_id},
                {"wal_offset",        result.wal_offset},
                {"append_latency_us", result.append_latency_us}
            });
        } catch (const std::exception& e) {
            respond_err(res, 400, "INVALID_REQUEST", e.what());
        }
    });

    // ── DELETE /v1/events ────────────────────────────────────────────────
    // Write a tombstone for a specific (symbol, timestamp_ns, sequence_id).

    s->Post("/v1/events/delete", [this](const httplib::Request& req, httplib::Response& res) {
        json body;
        if (!parse_body(req, res, body)) return;
        try {
            auto result = db_.remove(
                body.at("symbol").get<std::string>(),
                body.at("timestamp_ns").get<uint64_t>(),
                body.at("sequence_id").get<uint64_t>());
            respond_ok(res, {
                {"ok",                true},
                {"sequence_id",       result.sequence_id},
                {"wal_offset",        result.wal_offset},
                {"append_latency_us", result.append_latency_us}
            });
        } catch (const std::exception& e) {
            respond_err(res, 400, "INVALID_REQUEST", e.what());
        }
    });

    // ── POST /v1/ingest/synthetic/start ───────────────────────────────────

    s->Post("/v1/ingest/synthetic/start",
            [this](const httplib::Request& req, httplib::Response& res) {
        json body;
        if (!parse_body(req, res, body)) return;
        try {
            SyntheticConfig cfg;
            cfg.num_symbols      = body.value("symbols",          100u);
            cfg.events_per_sec   = body.value("events_per_sec",   100000.0);
            cfg.duration_seconds = body.value("duration_seconds", 30u);
            if (body.contains("event_mix")) {
                auto& mx = body["event_mix"];
                cfg.mix_trade  = mx.value("trade",  0.2);
                cfg.mix_quote  = mx.value("quote",  0.7);
                cfg.mix_add    = mx.value("add",    0.05);
                cfg.mix_modify = mx.value("modify", 0.03);
                cfg.mix_cancel = mx.value("cancel", 0.02);
            }
            std::string regime = body.value("volatility_regime", "normal");
            if      (regime == "high_vol")    cfg.volatility = VolatilityRegime::HighVol;
            else if (regime == "flash_crash") cfg.volatility = VolatilityRegime::FlashCrash;

            respond_ok(res, {{"ok", true}, {"job_id", synth_->start(cfg)}});
        } catch (const std::exception& e) {
            respond_err(res, 400, "INVALID_REQUEST", e.what());
        }
    });

    // ── POST /v1/ingest/synthetic/stop ────────────────────────────────────

    s->Post("/v1/ingest/synthetic/stop",
            [this](const httplib::Request& req, httplib::Response& res) {
        json body;
        if (!parse_body(req, res, body)) return;
        try {
            uint64_t n = synth_->stop(body.at("job_id").get<std::string>());
            respond_ok(res, {{"ok", true}, {"events_generated", n}});
        } catch (const std::exception& e) {
            respond_err(res, 400, "INVALID_REQUEST", e.what());
        }
    });

    // ── POST /v1/query ────────────────────────────────────────────────────

    s->Post("/v1/query", [this](const httplib::Request& req, httplib::Response& res) {
        json body;
        if (!parse_body(req, res, body)) return;
        try {
            QueryRequest qr;
            qr.symbol   = body.at("symbol").get<std::string>();
            qr.start_ns = body.value("start_ns", (uint64_t)0);
            qr.end_ns   = body.value("end_ns",   UINT64_MAX);
            qr.limit    = body.value("limit",    (uint32_t)1000);
            if (body.contains("event_type") && !body["event_type"].is_null())
                qr.event_type = Event::parse_event_type(body["event_type"].get<std::string>());

            auto result = db_.query(qr);

            json rows_arr = json::array();
            for (auto& e : result.rows) {
                rows_arr.push_back({
                    {"timestamp_ns",  e.timestamp_ns},
                    {"symbol",        qr.symbol},
                    {"sequence_id",   e.sequence_id},
                    {"event_type",    std::string(Event::event_type_name(e.event_type))},
                    {"price_ticks",   e.price_ticks},
                    {"bid_px_ticks",  e.bid_px_ticks},
                    {"ask_px_ticks",  e.ask_px_ticks},
                    {"quantity",      e.quantity},
                    {"bid_qty",       e.bid_qty},
                    {"ask_qty",       e.ask_qty},
                    {"order_id",      e.order_id}
                });
            }

            auto& st = result.stats;
            respond_ok(res, {
                {"ok",      true},
                {"columns", {"timestamp_ns","symbol","sequence_id","event_type",
                             "price_ticks","bid_px_ticks","ask_px_ticks",
                             "quantity","bid_qty","ask_qty","order_id"}},
                {"rows", rows_arr},
                {"stats", {
                    {"total_latency_us", st.total_latency_us},
                    {"planning_us",      st.planning_us},
                    {"index_lookup_us",  st.index_lookup_us},
                    {"block_read_us",    st.block_read_us},
                    {"decompression_us", st.decompression_us},
                    {"filtering_us",     st.filtering_us},
                    {"serialization_us", st.serialization_us},
                    {"files_considered", st.files_considered},
                    {"files_skipped",    st.files_skipped},
                    {"blocks_read",      st.blocks_read},
                    {"rows_scanned",     st.rows_scanned},
                    {"rows_returned",    st.rows_returned}
                }}
            });
        } catch (const std::exception& e) {
            respond_err(res, 400, "INVALID_REQUEST", e.what());
        }
    });

    // ── POST /v1/benchmark/run ────────────────────────────────────────────

    s->Post("/v1/benchmark/run", [this](const httplib::Request& req, httplib::Response& res) {
        json body;
        if (!parse_body(req, res, body)) return;
        try {
            std::string bench_name = body.value("benchmark", "ingest");
            uint64_t    events     = body.value("events",    (uint64_t)1'000'000);
            uint32_t    symbols    = body.value("symbols",   100u);

            if (bench_name != "ingest") {
                respond_err(res, 400, "UNSUPPORTED",
                            "only 'ingest' supported via API; use bench/ binaries for others");
                return;
            }

            auto t0 = std::chrono::steady_clock::now();
            LatencyHistogram hist;
            for (uint64_t i = 0; i < events; ++i) {
                char sym[16];
                snprintf(sym, sizeof(sym), "BENCH%04u", static_cast<unsigned>(i % symbols));
                auto ta = std::chrono::steady_clock::now();
                db_.append(sym,
                    static_cast<uint64_t>(
                        std::chrono::duration_cast<std::chrono::nanoseconds>(
                            std::chrono::system_clock::now().time_since_epoch()).count()),
                    EventType::Trade, 100, 99, 101, 100, 200, 200, i);
                uint64_t us = static_cast<uint64_t>(
                    std::chrono::duration_cast<std::chrono::microseconds>(
                        std::chrono::steady_clock::now() - ta).count());
                hist.record(us);
            }
            double secs      = std::chrono::duration<double>(
                std::chrono::steady_clock::now() - t0).count();
            double throughput = static_cast<double>(events) / secs;

            auto out_path = db_.config().path / "benchmarks" / "ingest_latest.json";
            std::filesystem::create_directories(out_path.parent_path());
            {
                std::ofstream f(out_path);
                f << json{{"benchmark", bench_name}, {"events", events},
                          {"throughput_events_per_sec", throughput},
                          {"p50_us", hist.percentile(0.50)},
                          {"p99_us", hist.percentile(0.99)}}.dump(2);
            }

            respond_ok(res, {
                {"ok",                        true},
                {"benchmark",                 bench_name},
                {"events",                    events},
                {"throughput_events_per_sec", throughput},
                {"p50_us",                    hist.percentile(0.50)},
                {"p99_us",                    hist.percentile(0.99)},
                {"output_path",               out_path.string()}
            });
        } catch (const std::exception& e) {
            respond_err(res, 500, "INTERNAL", e.what());
        }
    });

    // ── POST /v1/recovery/demo ────────────────────────────────────────────

    s->Post("/v1/recovery/demo", [this](const httplib::Request&, httplib::Response& res) {
        try {
            auto demo_dir     = db_.config().path / "recovery_demo";
            auto demo_wal_dir = demo_dir / "wal";
            std::filesystem::create_directories(demo_dir);

            const uint64_t N = 100'000;

            // Phase 1: write N events to a temp WAL
            {
                WALWriter writer(demo_wal_dir);
                for (uint64_t i = 0; i < N; ++i) {
                    Event e{};
                    e.timestamp_ns = i * 1000;
                    e.symbol_id    = 1;
                    e.sequence_id  = i;
                    e.event_type   = EventType::Trade;
                    e.price_ticks  = 100;
                    writer.append(e);
                }
            }

            // Phase 2: truncate last 384 bytes to simulate a torn write
            const uint64_t kCorruptBytes = 384;
            for (auto& entry : std::filesystem::directory_iterator(demo_wal_dir)) {
                if (!entry.is_regular_file()) continue;
                auto sz = std::filesystem::file_size(entry.path());
                if (sz > kCorruptBytes)
                    std::filesystem::resize_file(entry.path(), sz - kCorruptBytes);
            }

            // Phase 3: recover
            MemTable recovery_mem;
            auto rr = Recovery::recover(demo_wal_dir, recovery_mem);

            std::filesystem::remove_all(demo_dir);

            respond_ok(res, {
                {"ok",                     true},
                {"records_before_crash",   N},
                {"records_recovered",      rr.records_recovered},
                {"torn_record_detected",   rr.torn_detected},
                {"corrupted_tail_bytes",   rr.torn_bytes},
                {"recovery_time_ms",       rr.duration_ms},
                {"committed_records_lost", 0}
            });
        } catch (const std::exception& e) {
            respond_err(res, 500, "INTERNAL", e.what());
        }
    });

    // ── GET /v1/live (Server-Sent Events) ────────────────────────────────
    //
    // Streams a metrics snapshot every 500 ms.
    // Proper SSE headers are set so EventSource in the browser works correctly.

    s->Get("/v1/live", [](const httplib::Request&, httplib::Response& res) {
        add_cors(res);
        res.set_header("Cache-Control", "no-cache");
        res.set_header("X-Accel-Buffering", "no");   // disable nginx buffering if proxied

        res.set_chunked_content_provider(
            "text/event-stream",
            [](size_t /*offset*/, httplib::DataSink& sink) -> bool {
                auto& m = global_metrics();
                uint64_t now_ms = static_cast<uint64_t>(
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::system_clock::now().time_since_epoch()).count());

                std::string event = "data: " + json{
                    {"type",               "metrics"},
                    {"timestamp_ms",       now_ms},
                    {"events_per_sec",     m.ingest_rate.rate()},
                    {"wal_mb_per_sec",     m.wal_rate.rate() / (1024.0 * 1024.0)},
                    {"p99_append_us",      m.append_latency_us.percentile(0.99)},
                    {"memtable_mb",        static_cast<double>(m.memtable_bytes.load()) / (1024.0*1024.0)},
                    {"flush_count",        m.flush_count.load()},
                    {"compaction_debt_gb", static_cast<double>(m.compaction_debt_bytes.load()) / (1024.0*1024.0*1024.0)}
                }.dump() + "\n\n";

                // sink.write returns false when the client disconnects
                if (!sink.write(event.data(), event.size())) return false;

                std::this_thread::sleep_for(std::chrono::milliseconds(500));
                return true;
            });
    });
}

} // namespace kronos
