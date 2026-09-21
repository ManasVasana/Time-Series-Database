#pragma once
#include "kronosdb/db/db.hpp"
#include "kronosdb/net/synthetic.hpp"
#include <string>
#include <cstdint>
#include <atomic>
#include <thread>
#include <memory>

// Forward-declare httplib types to keep this header lightweight
namespace httplib { class Server; }

namespace kronos {

struct ServerConfig {
    std::string host{"127.0.0.1"};
    uint16_t    port{7420};
};

class Server {
public:
    Server(DB& db, ServerConfig cfg = {});
    ~Server();

    void start();   // blocks until stop() called from another thread
    void stop();
    bool is_running() const { return running_.load(); }

private:
    DB&                                  db_;
    ServerConfig                         cfg_;
    std::unique_ptr<httplib::Server>     svr_;
    std::unique_ptr<SyntheticGenerator>  synth_;
    std::atomic<bool>                    running_{false};

    void setup_routes();
};

} // namespace kronos
