#include "kronosdb/db/query.hpp"
#include "kronosdb/db/db.hpp"
#include "kronosdb/util/metrics.hpp"
#include <chrono>
#include <stdexcept>

// query.cpp: standalone query stats type; the actual execution lives in DB::query()
// to avoid circular deps between query.cpp and db.cpp.
// Nothing to define here beyond what's in the header.

namespace kronos {
// QueryRequest, QueryStats, QueryResult are POD/header-only.
// Intentionally empty translation unit - execution is in db.cpp.
} // namespace kronos
