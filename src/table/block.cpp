#include "kronosdb/table/block.hpp"
#include "kronosdb/util/crc32c.hpp"
#include <stdexcept>
#include <cstring>

namespace kronos {

// ---- helpers ----
static void write_u32(uint8_t* p, uint32_t v) {
    p[0]=(v)&0xFF; p[1]=(v>>8)&0xFF; p[2]=(v>>16)&0xFF; p[3]=(v>>24)&0xFF;
}
static uint32_t read_u32(const uint8_t* p) {
    return static_cast<uint32_t>(p[0])        |
           (static_cast<uint32_t>(p[1]) <<  8) |
           (static_cast<uint32_t>(p[2]) << 16) |
           (static_cast<uint32_t>(p[3]) << 24);
}

// ---- BlockBuilder ----

BlockBuilder::BlockBuilder(size_t target_bytes)
    : target_bytes_(target_bytes) {}

bool BlockBuilder::add(const Event& e) {
    size_t current_size = 4 + events_.size() * Event::kEncodedSize
                        + Key::kEncodedSize * 2 + 4;
    if (!events_.empty() && current_size + Event::kEncodedSize > target_bytes_)
        return false;
    events_.push_back(e);
    return true;
}

std::vector<uint8_t> BlockBuilder::finish() const {
    if (events_.empty()) return {};

    Key min_k = events_.front().key();
    Key max_k = events_.back().key();
    // Ensure proper min/max in case events were added out of order
    for (auto& e : events_) {
        if (e.key() < min_k) min_k = e.key();
        if (e.key() > max_k) max_k = e.key();
    }

    size_t data_size = 4 + events_.size() * Event::kEncodedSize
                     + Key::kEncodedSize * 2;
    std::vector<uint8_t> buf(data_size + 4); // +4 for crc

    uint8_t* p = buf.data();
    write_u32(p, static_cast<uint32_t>(events_.size()));
    p += 4;

    for (auto& e : events_) {
        auto encoded = e.encode();
        std::memcpy(p, encoded.data(), Event::kEncodedSize);
        p += Event::kEncodedSize;
    }

    auto min_enc = min_k.encode();
    auto max_enc = max_k.encode();
    std::memcpy(p, min_enc.data(), Key::kEncodedSize); p += Key::kEncodedSize;
    std::memcpy(p, max_enc.data(), Key::kEncodedSize); p += Key::kEncodedSize;

    // CRC over everything up to (but not including) the checksum field
    uint32_t chk = crc32c(buf.data(), data_size);
    write_u32(p, chk);

    return buf;
}

void BlockBuilder::reset() { events_.clear(); }

// ---- Block ----

Block Block::decode(std::span<const uint8_t> data) {
    if (data.size() < 4 + Key::kEncodedSize * 2 + 4)
        throw std::runtime_error("block too small");

    const uint8_t* p = data.data();
    uint32_t count = read_u32(p); p += 4;

    size_t expected = 4 + count * Event::kEncodedSize
                    + Key::kEncodedSize * 2 + 4;
    if (data.size() < expected)
        throw std::runtime_error("block size mismatch");

    // Verify checksum
    uint32_t stored_crc  = read_u32(data.data() + expected - 4);
    uint32_t computed_crc = crc32c(data.data(), expected - 4);
    if (stored_crc != computed_crc)
        throw std::runtime_error("block checksum mismatch");

    Block b;
    b.events_.reserve(count);

    for (uint32_t i = 0; i < count; ++i) {
        std::span<const uint8_t, Event::kEncodedSize> sp(p, Event::kEncodedSize);
        b.events_.push_back(Event::decode(sp));
        p += Event::kEncodedSize;
    }

    b.meta_.min_key = Key::decode(std::span<const uint8_t, Key::kEncodedSize>(p, Key::kEncodedSize));
    p += Key::kEncodedSize;
    b.meta_.max_key = Key::decode(std::span<const uint8_t, Key::kEncodedSize>(p, Key::kEncodedSize));
    b.meta_.record_count  = count;
    b.meta_.encoded_size  = static_cast<uint32_t>(expected);
    return b;
}

void Block::scan(const Key& start, const Key& end,
                 const std::function<bool(const Event&)>& cb) const {
    for (auto& e : events_) {
        auto k = e.key();
        if (k < start) continue;
        if (k > end)   break;
        if (!cb(e)) return;
    }
}

} // namespace kronos
