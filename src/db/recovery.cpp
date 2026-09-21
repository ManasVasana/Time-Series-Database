#include "kronosdb/db/recovery.hpp"
#include <chrono>

namespace kronos {

RecoveryResult Recovery::recover(
    const std::filesystem::path& wal_dir,
    MemTable&                    target) {

    if (!std::filesystem::exists(wal_dir))
        return {0, 0, false, 0.0, 0};

    auto t0 = std::chrono::steady_clock::now();

    size_t   torn_bytes       = 0;
    bool     torn_detected    = false;
    uint64_t clean_end_offset = 0;

    WALReader reader(wal_dir);
    auto records = reader.read_all(&torn_bytes, &torn_detected, &clean_end_offset);

    for (auto& rec : records)
        target.insert(rec.event);

    double ms = std::chrono::duration<double, std::milli>(
        std::chrono::steady_clock::now() - t0).count();

    return {
        static_cast<uint64_t>(records.size()),
        torn_bytes,
        torn_detected,
        ms,
        clean_end_offset
    };
}

} // namespace kronos
