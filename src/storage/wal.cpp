#include "kronosdb/storage/wal.hpp"
#include "kronosdb/util/crc32c.hpp"
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <stdexcept>
#include <cstring>
#include <algorithm>
#include <filesystem>

namespace kronos {

// ---- little-endian helpers ----
static void write_u8 (uint8_t* p, uint8_t  v)              { *p = v; }
static void write_u32(uint8_t* p, uint32_t v) {
    p[0]=(v)&0xFF; p[1]=(v>>8)&0xFF; p[2]=(v>>16)&0xFF; p[3]=(v>>24)&0xFF;
}
static uint32_t read_u32(const uint8_t* p) {
    return static_cast<uint32_t>(p[0])        |
          (static_cast<uint32_t>(p[1]) <<  8) |
          (static_cast<uint32_t>(p[2]) << 16) |
          (static_cast<uint32_t>(p[3]) << 24);
}

// ---- WAL CRC helper ----
//
// CRC32C is computed over (version || payload_len || payload) as a single stream.
// crc32c(data, len, seed) continues from a previous finalized CRC when seed != 0.
// Correct chaining: result2 = crc32c(buf2, n, result1)
// The implementation XORs seed with 0xFFFFFFFF internally, which correctly
// "un-finalizes" result1 back to the running internal state.
static uint32_t wal_crc(const uint8_t* version_and_len, const uint8_t* payload,
                         uint32_t payload_len) {
    uint32_t crc = crc32c(version_and_len, 5);
    // Pass the finalized result directly as seed — crc32c undoes the final XOR
    return kronos::crc32c(payload, payload_len, crc);
}

// ---- path helpers ----

std::filesystem::path WALWriter::segment_path(const std::filesystem::path& dir, uint64_t id) {
    char buf[32];
    snprintf(buf, sizeof(buf), "wal_%06llu.log", static_cast<unsigned long long>(id));
    return dir / buf;
}

// ---- WALWriter ----

WALWriter::WALWriter(std::filesystem::path dir, size_t max_segment_bytes)
    : dir_(std::move(dir)), max_segment_bytes_(max_segment_bytes) {
    std::filesystem::create_directories(dir_);

    uint64_t max_id = 0;
    for (auto& entry : std::filesystem::directory_iterator(dir_)) {
        if (!entry.is_regular_file()) continue;
        auto   name = entry.path().filename().string();
        uint64_t id = 0;
        if (sscanf(name.c_str(), "wal_%llu.log", &id) == 1)
            max_id = std::max(max_id, id);
    }
    segment_id_ = max_id > 0 ? max_id : 1;
    open_segment(segment_id_);
}

WALWriter::~WALWriter() { close(); }

void WALWriter::open_segment(uint64_t id) {
    if (fd_ >= 0) { ::close(fd_); fd_ = -1; }
    auto p = segment_path(dir_, id);
    fd_ = ::open(p.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (fd_ < 0) throw std::runtime_error("WAL open failed: " + p.string());
    struct stat st{};
    if (::fstat(fd_, &st) == 0) current_offset_ = static_cast<uint64_t>(st.st_size);
    segment_id_ = id;
}

void WALWriter::rotate() {
    ++segment_id_;
    current_offset_ = 0;
    open_segment(segment_id_);
}

void WALWriter::seal() {
    // Force a new segment on the next write.
    // Called after crash recovery to prevent appending past a torn tail.
    std::lock_guard lock(mu_);
    rotate();
}

void WALWriter::truncate_and_seal(uint64_t clean_end_offset) {
    // Truncate the current segment at the last known-good offset, then
    // rotate to a new segment. Called when recovery detected a torn tail.
    std::lock_guard lock(mu_);
    if (fd_ >= 0 && clean_end_offset < current_offset_) {
        ::ftruncate(fd_, static_cast<off_t>(clean_end_offset));
    }
    rotate();
}

WALAppendResult WALWriter::append(const Event& e) {
    std::lock_guard lock(mu_);

    auto payload = e.encode();

    uint8_t hdr_for_crc[5];
    hdr_for_crc[0] = kWalVersion;
    write_u32(hdr_for_crc + 1, static_cast<uint32_t>(payload.size()));

    uint32_t crc = wal_crc(hdr_for_crc, payload.data(),
                            static_cast<uint32_t>(payload.size()));

    static_assert(kWalHeaderSize == 13, "WAL header size changed");
    uint8_t rec[kWalHeaderSize + Event::kEncodedSize];
    write_u32(rec,     kWalMagic);
    write_u8 (rec + 4, kWalVersion);
    write_u32(rec + 5, static_cast<uint32_t>(payload.size()));
    write_u32(rec + 9, crc);
    std::memcpy(rec + kWalHeaderSize, payload.data(), payload.size());

    // Save the segment this record lands in (rotation happens AFTER the write)
    uint64_t record_segment = segment_id_;
    uint64_t record_offset  = current_offset_;

    ssize_t written = ::write(fd_, rec, sizeof(rec));
    if (written != static_cast<ssize_t>(sizeof(rec)))
        throw std::runtime_error("WAL write failed");

    current_offset_ += sizeof(rec);
    bytes_written_  += sizeof(rec);

    // Rotate after the write so the returned offset/segment are for THIS record
    if (current_offset_ >= max_segment_bytes_) rotate();

    return {record_offset, record_segment};
}

void WALWriter::sync() {
    std::lock_guard lock(mu_);
    if (fd_ >= 0) ::fsync(fd_);
}

void WALWriter::close() {
    std::lock_guard lock(mu_);
    if (fd_ >= 0) { ::fsync(fd_); ::close(fd_); fd_ = -1; }
}

// ---- WALReader ----

WALReader::WALReader(std::filesystem::path dir) : dir_(std::move(dir)) {
    if (!std::filesystem::exists(dir_)) return;
    for (auto& entry : std::filesystem::directory_iterator(dir_)) {
        if (!entry.is_regular_file()) continue;
        auto   name = entry.path().filename().string();
        uint64_t id = 0;
        if (sscanf(name.c_str(), "wal_%llu.log", &id) == 1)
            max_segment_id_ = std::max(max_segment_id_, id);
    }
}

std::vector<WALRecord> WALReader::read_all(size_t* out_torn_bytes, bool* out_torn,
                                            uint64_t* out_clean_end_offset) {
    if (out_torn_bytes)     *out_torn_bytes     = 0;
    if (out_torn)           *out_torn           = false;
    if (out_clean_end_offset) *out_clean_end_offset = 0;

    std::vector<WALRecord> records;
    for (uint64_t id = 1; id <= max_segment_id_; ++id) {
        auto path = WALWriter::segment_path(dir_, id);
        if (!std::filesystem::exists(path)) continue;

        uint64_t seg_clean_end = 0;
        auto seg = read_segment(path, out_torn_bytes, out_torn, &seg_clean_end);
        records.insert(records.end(), seg.begin(), seg.end());

        if (out_clean_end_offset) *out_clean_end_offset = seg_clean_end;
    }
    return records;
}

std::vector<WALRecord> WALReader::read_segment(
        const std::filesystem::path& path,
        size_t*   out_torn_bytes,
        bool*     out_torn_det,
        uint64_t* out_clean_end) {

    std::vector<WALRecord> records;
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) return records;

    struct stat st{};
    ::fstat(fd, &st);
    size_t file_size = static_cast<size_t>(st.st_size);
    size_t offset    = 0;

    while (offset + kWalHeaderSize <= file_size) {
        uint8_t hdr[kWalHeaderSize];
        ssize_t n = ::pread(fd, hdr, kWalHeaderSize, static_cast<off_t>(offset));
        if (n != static_cast<ssize_t>(kWalHeaderSize)) break;

        uint32_t magic       = read_u32(hdr);
        uint8_t  version     = hdr[4];
        uint32_t payload_len = read_u32(hdr + 5);
        uint32_t stored_crc  = read_u32(hdr + 9);

        if (magic != kWalMagic || version != kWalVersion) {
            if (out_torn_det)   *out_torn_det   = true;
            if (out_torn_bytes) *out_torn_bytes += file_size - offset;
            break;
        }

        if (offset + kWalHeaderSize + payload_len > file_size) {
            if (out_torn_det)   *out_torn_det   = true;
            if (out_torn_bytes) *out_torn_bytes += file_size - offset;
            break;
        }

        std::vector<uint8_t> payload(payload_len);
        n = ::pread(fd, payload.data(), payload_len,
                    static_cast<off_t>(offset + kWalHeaderSize));
        if (n != static_cast<ssize_t>(payload_len)) {
            if (out_torn_det) *out_torn_det = true;
            break;
        }

        // Build the same 5-byte header that was CRC'd during write
        uint8_t hdr_for_crc[5];
        hdr_for_crc[0] = version;
        hdr_for_crc[1] = (payload_len      ) & 0xFF;
        hdr_for_crc[2] = (payload_len >>  8) & 0xFF;
        hdr_for_crc[3] = (payload_len >> 16) & 0xFF;
        hdr_for_crc[4] = (payload_len >> 24) & 0xFF;

        uint32_t computed = wal_crc(hdr_for_crc, payload.data(), payload_len);
        if (computed != stored_crc) {
            if (out_torn_det)   *out_torn_det   = true;
            if (out_torn_bytes) *out_torn_bytes += file_size - offset;
            break;
        }

        // Reject unknown payload sizes (forward-compatibility guard)
        if (payload_len == Event::kEncodedSize) {
            std::span<const uint8_t, Event::kEncodedSize> sp(
                payload.data(), Event::kEncodedSize);
            WALRecord rec;
            rec.event       = Event::decode(sp);
            rec.file_offset = offset;
            records.push_back(rec);
        }

        offset += kWalHeaderSize + payload_len;
    }

    // Any bytes after the last fully-consumed record are a torn write.
    // This catches garbage < kWalHeaderSize bytes that the loop skips entirely.
    if (offset < file_size) {
        if (out_torn_det)   *out_torn_det   = true;
        if (out_torn_bytes) *out_torn_bytes += file_size - offset;
    }

    if (out_clean_end) *out_clean_end = offset;
    ::close(fd);
    return records;
}

} // namespace kronos
