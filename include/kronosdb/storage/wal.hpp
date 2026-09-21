#pragma once
#include "kronosdb/core/event.hpp"
#include <filesystem>
#include <vector>
#include <cstdint>
#include <mutex>

namespace kronos {

// WAL record wire format:
//   [magic      : 4]  = 0x4B524F4E ('KRON')
//   [version    : 1]  = 0x01
//   [payload_len: 4]
//   [crc32c     : 4]  CRC32C of (version || payload_len || payload) as one stream
//   [payload    : N]  = encoded Event (65 bytes)
//
// Total per record: 13 + 65 = 78 bytes
//
// CRC chaining: crc32c(buf2, n, prev_result) correctly continues from prev_result
// because the implementation internally XORs seed with 0xFFFFFFFF, which
// undoes the final finalization of the previous call.

static constexpr uint32_t kWalMagic    = 0x4B524F4Eu;
static constexpr uint8_t  kWalVersion  = 0x01;
static constexpr size_t   kWalHeaderSize = 13;  // magic(4)+version(1)+len(4)+crc(4)

struct WALRecord {
    Event    event;
    uint64_t file_offset;  // byte offset of this record in its segment
};

struct WALAppendResult {
    uint64_t file_offset;   // offset of this record within its segment
    uint64_t segment_id;    // segment this record was written to
};

class WALWriter {
public:
    WALWriter(std::filesystem::path dir,
              size_t max_segment_bytes = 128 * 1024 * 1024);
    ~WALWriter();

    WALAppendResult append(const Event& e);

    // Force a new segment on next write; prevents appending after a torn tail.
    void seal();

    // Truncate the current segment at clean_end_offset, then seal.
    // Use after crash recovery when a torn tail was detected.
    void truncate_and_seal(uint64_t clean_end_offset);

    void sync();
    void close();

    uint64_t current_segment_id() const { return segment_id_; }
    uint64_t bytes_written()      const { return bytes_written_; }

    static std::filesystem::path segment_path(const std::filesystem::path& dir, uint64_t id);

private:
    std::filesystem::path dir_;
    size_t                max_segment_bytes_;
    int                   fd_{-1};
    uint64_t              segment_id_{0};
    uint64_t              current_offset_{0};
    uint64_t              bytes_written_{0};
    mutable std::mutex    mu_;

    void open_segment(uint64_t id);
    void rotate();
};

class WALReader {
public:
    explicit WALReader(std::filesystem::path dir);

    // Read all valid records across all segments.
    // Stops at the first corrupt/truncated record in each segment.
    // out_clean_end_offset: byte offset just past the last valid record in
    // the last segment (can be used to truncate torn tails).
    std::vector<WALRecord> read_all(
        size_t*   out_torn_bytes       = nullptr,
        bool*     out_torn_detected    = nullptr,
        uint64_t* out_clean_end_offset = nullptr);

    uint64_t max_segment_id() const { return max_segment_id_; }

private:
    std::filesystem::path dir_;
    uint64_t              max_segment_id_{0};

    std::vector<WALRecord> read_segment(const std::filesystem::path& path,
                                        size_t*   out_torn_bytes,
                                        bool*     out_torn_detected,
                                        uint64_t* out_clean_end);
};

} // namespace kronos
