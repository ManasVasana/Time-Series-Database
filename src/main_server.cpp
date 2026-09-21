#include "kronosdb/db/db.hpp"
#include "kronosdb/net/server.hpp"
#include <csignal>
#include <cstdlib>
#include <string>
#include <iostream>
#include <thread>

static kronos::Server* g_server = nullptr;

static void signal_handler(int /*sig*/) {
    if (g_server) g_server->stop();
}

static void usage(const char* prog) {
    std::cerr
        << "Usage: " << prog << " [options]\n"
        << "  --db   <path>     data directory   (default: ./data)\n"
        << "  --port <port>     listen port      (default: 7420)\n"
        << "  --host <host>     listen host      (default: 127.0.0.1)\n"
        << "  --io-backend <b>  pread|mmap        (default: pread)\n"
        << "  --no-wal          disable WAL\n"
        << "  --help\n";
}

int main(int argc, char* argv[]) {
    std::string db_path    = "./data";
    std::string host       = "127.0.0.1";
    uint16_t    port       = 7420;
    std::string io_backend = "pread";
    bool        wal        = true;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--help") { usage(argv[0]); return 0; }
        else if (arg == "--db"         && i+1 < argc) { db_path    = argv[++i]; }
        else if (arg == "--port"       && i+1 < argc) { port       = static_cast<uint16_t>(std::stoi(argv[++i])); }
        else if (arg == "--host"       && i+1 < argc) { host       = argv[++i]; }
        else if (arg == "--io-backend" && i+1 < argc) { io_backend = argv[++i]; }
        else if (arg == "--no-wal")                   { wal        = false; }
        else { std::cerr << "Unknown option: " << arg << "\n"; usage(argv[0]); return 1; }
    }

    kronos::DBConfig cfg;
    cfg.path        = db_path;
    cfg.io_backend  = io_backend;
    cfg.wal_enabled = wal;

    std::cout << "KronosDB v0.1.0\n"
              << "  db path : " << db_path    << "\n"
              << "  host    : " << host << ":" << port << "\n"
              << "  backend : " << io_backend << "\n"
              << "  WAL     : " << (wal ? "enabled" : "disabled") << "\n";

    kronos::DB db(cfg);

    kronos::ServerConfig scfg;
    scfg.host = host;
    scfg.port = port;
    kronos::Server server(db, scfg);
    g_server = &server;

    std::signal(SIGINT,  signal_handler);
    std::signal(SIGTERM, signal_handler);

    std::cout << "Listening on http://" << host << ":" << port << "\n";
    server.start();  // blocks

    std::cout << "Shutdown complete.\n";
    return 0;
}
