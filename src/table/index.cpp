#include "kronosdb/table/index.hpp"
#include <stdexcept>
#include <cstring>
#include <algorithm>

namespace kronos {

static void write_u32(uint8_t* p, uint32_t v) {
    p[0]=(v)&0xFF; p[1]=(v>>8)&0xFF; p[2]=(v>>16)&0xFF; p[3]=(v>>24)&0xFF;
}
static void write_u64(uint8_t* p, uint64_t v) {
    for (int i = 0; i < 8; ++i) p[i] = (v >> (8*i)) & 0xFF;
}
static uint32_t read_u32(const uint8_t* p) {
    return static_cast<uint32_t>(p[0])        |
           (static_cast<uint32_t>(p[1]) <<  8) |
           (static_cast<uint32_t>(p[2]) << 16) |
           (static_cast<uint32_t>(p[3]) << 24);
}
static uint64_t read_u64(const uint8_t* p) {
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i) v |= static_cast<uint64_t>(p[i]) << (8*i);
    return v;
}

void IndexBlock::add(const Key& first_key, uint64_t offset, uint32_t size) {
    entries_.push_back({first_key, offset, size});
}

std::optional<size_t> IndexBlock::find_first_block(const Key& query_key) const {
    if (entries_.empty()) return std::nullopt;
    // Binary search: find last entry whose first_key <= query_key
    size_t lo = 0, hi = entries_.size();
    while (lo + 1 < hi) {
        size_t mid = (lo + hi) / 2;
        if (entries_[mid].first_key <= query_key) lo = mid;
        else hi = mid;
    }
    return lo;
}

std::vector<size_t> IndexBlock::blocks_in_range(const Key& start, const Key& end) const {
    std::vector<size_t> result;
    if (entries_.empty()) return result;

    // Find starting entry: last block whose first_key <= start
    size_t first = 0;
    {
        size_t lo = 0, hi = entries_.size();
        while (lo + 1 < hi) {
            size_t mid = (lo + hi) / 2;
            if (entries_[mid].first_key <= start) lo = mid;
            else hi = mid;
        }
        first = lo;
    }

    for (size_t i = first; i < entries_.size(); ++i) {
        if (entries_[i].first_key > end) break;
        result.push_back(i);
    }
    return result;
}

std::vector<uint8_t> IndexBlock::encode() const {
    // [count:4] [entry_0: 32] ... [entry_N: 32]
    std::vector<uint8_t> buf(4 + entries_.size() * IndexEntry::kEncodedSize);
    uint8_t* p = buf.data();
    write_u32(p, static_cast<uint32_t>(entries_.size())); p += 4;
    for (auto& e : entries_) {
        auto kenc = e.first_key.encode();
        std::memcpy(p, kenc.data(), Key::kEncodedSize); p += Key::kEncodedSize;
        write_u64(p, e.block_offset); p += 8;
        write_u32(p, e.block_size);   p += 4;
    }
    return buf;
}

IndexBlock IndexBlock::decode(std::span<const uint8_t> data) {
    if (data.size() < 4) throw std::runtime_error("index block too small");
    const uint8_t* p = data.data();
    uint32_t count = read_u32(p); p += 4;
    if (data.size() < 4 + count * IndexEntry::kEncodedSize)
        throw std::runtime_error("index block truncated");

    IndexBlock ib;
    ib.entries_.reserve(count);
    for (uint32_t i = 0; i < count; ++i) {
        IndexEntry e;
        e.first_key    = Key::decode(std::span<const uint8_t, Key::kEncodedSize>(p, Key::kEncodedSize));
        p += Key::kEncodedSize;
        e.block_offset = read_u64(p); p += 8;
        e.block_size   = read_u32(p); p += 4;
        ib.entries_.push_back(e);
    }
    return ib;
}

} // namespace kronos
