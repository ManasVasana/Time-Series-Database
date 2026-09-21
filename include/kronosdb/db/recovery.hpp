#pragma once
#include "kronosdb/storage/wal.hpp"
#include "kronosdb/storage/memtable.hpp"
#include <filesystem>
#include <cstdint>

namespace kronos {

struct RecoveryResult {
    uint64_t records_recovered;
    size_t   torn_bytes;
    bool     torn_detected;
    double   duration_ms;
    uint64_t clean_end_offset;  // byte offset just past the last valid record
};

class Recovery {
public:
    // Replay all WAL segments in wal_dir into target memtable.
    // If torn_detected is set on return, call wal_writer.truncate_and_seal(clean_end_offset)
    // before any further writes to prevent appending past the corrupt tail.
    static RecoveryResult recover(
        const std::filesystem::path& wal_dir,
        MemTable&                    target);
};

} // namespace kronos
