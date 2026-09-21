#pragma once
#include "kronosdb/core/key.hpp"
#include "kronosdb/core/event.hpp"
#include <string>
#include <vector>
#include <optional>
#include <cstdint>

namespace kronos {

struct QueryRequest {
    std::string            symbol;
    uint64_t               start_ns{0};
    uint64_t               end_ns{UINT64_MAX};
    std::optional<EventType> event_type;
    uint32_t               limit{1000};
};

struct QueryStats {
    uint64_t total_latency_us{0};
    uint64_t planning_us{0};
    uint64_t index_lookup_us{0};
    uint64_t block_read_us{0};
    uint64_t decompression_us{0};
    uint64_t filtering_us{0};
    uint64_t serialization_us{0};
    uint64_t files_considered{0};
    uint64_t files_skipped{0};
    uint64_t blocks_read{0};
    uint64_t rows_scanned{0};
    uint64_t rows_returned{0};
};

struct QueryResult {
    std::vector<Event> rows;
    QueryStats         stats;
    bool               ok{true};
    std::string        error;
};

} // namespace kronos
